package io.unitycatalog.spark

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.delta.kernel.{Operation, Transaction}
import io.delta.kernel.data.ColumnVector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{BaseTable, DataFile, DataFiles, Metrics, PartitionSpec, Snapshot, StaticTableOperations, Table, TableMetadata, TableMetadataParser, TableProperties}
import org.apache.iceberg.hadoop.{HadoopFileIO, HadoopTableOperations}

import java.util.{Collections, Optional, UUID}
import java.nio.file.FileSystem
import scala.None$
import scala.Option
import scala.Some$
import scala.collection.Map
import scala.collection.Map$
import scala.collection.JavaConverters._
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.exceptions.TableNotFoundException
import io.delta.kernel.utils.{CloseableIterable, DataFileStatus}
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction
import io.delta.kernel.internal.TableImpl
import io.delta.kernel.internal.util.Utils
import org.apache.iceberg.mapping.NameMapping
import org.apache.iceberg.relocated.com.google.common.collect.Maps
import org.apache.iceberg.util.JsonUtil.FromJson
import org.apache.iceberg.util.{JsonUtil, Pair}

import java.nio.ByteBuffer

class ConvertIcebergToDeltaScalaUtils {
  private val ICEBERG_SPARK_CLASS_NAME = "org.apache.spark.sql.delta.commands.convert.IcebergTable"

  private val HADOOP_CONF = new Configuration
  private val DEFAULT_ENGINE = DefaultEngine.create(HADOOP_CONF);
  private val MAPPER = new ObjectMapper()

  def convertToDelta(icebergManifestLocation: String, deltaTableLocation: String): Long = {
    val startTimeMs = System.currentTimeMillis()
    try {
      val icebergTable: org.apache.iceberg.Table =
        createIcebergTable(
          icebergManifestLocation,
          deltaTableLocation,
          (newManifestListLocation) => {
            // do nothing
          })

      val icebergSnapshpot: Snapshot = icebergTable.currentSnapshot

      val kernelTable = io.delta.kernel.Table.forPath(DEFAULT_ENGINE, deltaTableLocation)

      var existingTable = true;
      try {
        kernelTable.getLatestSnapshot(DEFAULT_ENGINE)
      } catch {
        case e: TableNotFoundException => existingTable = false
      }

      val txnBuilder = kernelTable.createTransactionBuilder(DEFAULT_ENGINE, "UC", Operation.CREATE_TABLE)

      if (existingTable) {
        // existing table. Lets find what is the last snaphot of the Iceberg
        // the Delta table is synced to
        val (addFiles, removeFiles) = getDeltaActionsFromIcebergCommit(icebergSnapshpot, deltaTableLocation)

        val txn = txnBuilder.build(DEFAULT_ENGINE)
        val txnState = txn.getTransactionState(DEFAULT_ENGINE)
        val writeContext = Transaction.getWriteContext(DEFAULT_ENGINE, txnState, Map.empty.asJava)

        val cAddIter = Utils.toCloseableIterator(addFiles.iterator.asJava)
        val cRemoveIter = Utils.toCloseableIterator(removeFiles.iterator.asJava)

        val cAddActions = CloseableIterable.inMemoryIterable(
          Transaction.generateAppendActions(DEFAULT_ENGINE, txnState, cAddIter, writeContext).combine(
            Transaction.generateRemoveActions(DEFAULT_ENGINE, txnState, cRemoveIter, writeContext)))

        val commitStatus = txn.commit(DEFAULT_ENGINE, cAddActions)

        commitStatus.getVersion
      }
      else {
        // new table
        // update the metadata of the Delta table
        val properties = icebergTable.properties.asScala.toMap + ("delta.columnMapping.mode" -> "id")

        val kernelSchema = SchemaUtils.fromIcebergSchema(icebergTable.schema().asStruct())

        val txn = txnBuilder
          .withSchema(DEFAULT_ENGINE, kernelSchema)
          .withTableProperties(DEFAULT_ENGINE, properties.asJava)
          .build(DEFAULT_ENGINE)

        val txnState = txn.getTransactionState(DEFAULT_ENGINE)
        val writeContext = Transaction.getWriteContext(DEFAULT_ENGINE, txnState, Map.empty.asJava)

        val (addFiles, removeFiles) = getDeltaActionsFromIcebergCommit(icebergSnapshpot, deltaTableLocation)

        val cIter = Utils.toCloseableIterator(addFiles.iterator.asJava)

        val cAddActions = CloseableIterable.inMemoryIterable(
          Transaction.generateAppendActions(DEFAULT_ENGINE, txnState, cIter, writeContext))

        val commitStatus = txn.commit(DEFAULT_ENGINE, cAddActions)

        commitStatus.getVersion
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    } finally {
      val endTimeMs = System.currentTimeMillis()
      println(s"Time taken to convert Iceberg to Delta: ${endTimeMs - startTimeMs} ms")
    }
  }

  def backfillIcebergMetadata(
      deltaTableLocation: String,
      lastConvertedVersion: Long,
      lastManifestLocation: String): Pair[java.lang.Long, java.lang.String] = {

    try {
      val kernelTable = io.delta.kernel.Table.forPath(DEFAULT_ENGINE, deltaTableLocation)
      val latestDeltaVersion = try {
        kernelTable.getLatestSnapshot(DEFAULT_ENGINE).getVersion(DEFAULT_ENGINE)
      } catch {
        case e: TableNotFoundException => -1
      }

      var lastManifest = lastManifestLocation;
      var lastVersion = lastConvertedVersion;

      Seq.range(lastConvertedVersion + 1, latestDeltaVersion + 1).foreach { deltaVersion =>
        val (addFiles, removeFiles) = getIcebergActionsFromDeltaCommit(deltaTableLocation, deltaVersion)

        val icebergTable: Table = createIcebergTable(lastManifest, deltaTableLocation, (newManifestListLocation) => {
          lastManifest = newManifestListLocation
        })

        val icebergTxn = icebergTable.newTransaction()

        if (removeFiles.nonEmpty && addFiles.nonEmpty) {
          val rewriteFilesOps = icebergTxn.newRewrite()

          addFiles.foreach { file =>
            rewriteFilesOps.addFile(file)
          }
          removeFiles.foreach { file =>
            rewriteFilesOps.deleteFile(file)
          }

          rewriteFilesOps.commit()
        } else if (addFiles.nonEmpty) {
          val appendFilesOps = icebergTxn.newAppend()

          addFiles.foreach { file =>
            appendFilesOps.appendFile(file)
          }

          appendFilesOps.commit()
        } else if (removeFiles.nonEmpty) {
          val deleteFilesOps = icebergTxn.newDelete()

          removeFiles.foreach { file =>
            deleteFilesOps.deleteFile(file)
          }

          deleteFilesOps.commit()
        }

        icebergTxn.commitTransaction()
        lastVersion = deltaVersion
      }

      return Pair.of(java.lang.Long.valueOf(lastVersion), lastManifest)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }

  // Create the Iceberg table
  def createIcebergTable(manifestLocation: String, tableLocation: String, newManifestReceiver: (String) => Unit): Table = {
    val ops = new StaticTableOperations(manifestLocation, new HadoopFileIO(HADOOP_CONF)) {
      override def metadataFileLocation(fileName: String): String = {
        String.format("%s/%s/%s", tableLocation, "metadata", fileName)
      }

      override def commit(base: TableMetadata, metadata: TableMetadata): Unit = {
        val codecName = metadata.property(TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT)
        val codec = TableMetadataParser.Codec.fromName(codecName)
        val fileExtension = TableMetadataParser.getFileExtension(codec)
        val tempMetadataFile = new Path(
          String.format("%s/%s/%s", tableLocation, "metadata", UUID.randomUUID.toString + fileExtension)
        )
        TableMetadataParser.write(metadata, io.newOutputFile(tempMetadataFile.toString))

        newManifestReceiver(tempMetadataFile.toString)
      }
    }
    return new BaseTable(ops, manifestLocation)
  }

  private def getDeltaActionsFromIcebergCommit(snapshot: Snapshot, deltaTableLocation: String)
      : (Seq[DataFileStatus], Seq[DataFileStatus]) = {
    // get the list of data files
    // create the actions
    val deltaPath = new Path(deltaTableLocation)
    val fs = deltaPath.getFileSystem(HADOOP_CONF)

    val addFiles = new scala.collection.mutable.ArrayBuffer[DataFileStatus]
    val removeFiles = new scala.collection.mutable.ArrayBuffer[DataFileStatus]

    snapshot.addedDataFiles(new HadoopFileIO(HADOOP_CONF)).iterator().asScala.foreach { file =>
      // create the action
      val filePath = new Path(file.path().toString)

      val relativePath = new Path(fs.makeQualified(deltaPath).toUri.relativize(fs.makeQualified(filePath).toUri))
      addFiles += new DataFileStatus(
        relativePath.toString,
        file.fileSizeInBytes(),
        122L /* modification time */,
        Optional.empty())
    }

    snapshot.removedDataFiles(new HadoopFileIO(HADOOP_CONF)).iterator().asScala.foreach { file =>
      // create the action
      val filePath = new Path(file.path().toString)
      val relativePath = new Path(fs.makeQualified(deltaPath).toUri.relativize(fs.makeQualified(filePath).toUri))
      removeFiles += new DataFileStatus(
        relativePath.toString,
        file.fileSizeInBytes(),
        234 /* modification time */,
        Optional.empty())
    }

    return (addFiles.toSeq, removeFiles.toSeq)
  }

  private def getIcebergActionsFromDeltaCommit(deltaTableLocation: String, version: Long): (Seq[DataFile], Seq[DataFile]) = {
    val kernelTable = io.delta.kernel.Table.forPath(DEFAULT_ENGINE, deltaTableLocation).asInstanceOf[io.delta.kernel.internal.TableImpl]
    val changesIter = kernelTable.getChanges(DEFAULT_ENGINE, version, version, Seq(DeltaAction.ADD, DeltaAction.REMOVE).toSet.asJava);

    val addedFiles = new scala.collection.mutable.ArrayBuffer[DataFile]
    val removedFiles = new scala.collection.mutable.ArrayBuffer[DataFile]

    val tablePath = new Path(deltaTableLocation)

    while (changesIter.hasNext) {
      val batch = changesIter.next()
      val addsVector = batch.getColumnVector(batch.getSchema().indexOf("add"))

      val addPathVector = addsVector.getChild(0)
      val addSizeVector = addsVector.getChild(2)
      val addStatsVector = addsVector.getChild(7)
      for (i <- 0 until addsVector.getSize) {
        if (!addsVector.isNullAt(i)) {
          val path = addPathVector.getString(i)
          val absPath = absolutePath(deltaTableLocation, path)
          val size = addSizeVector.getLong(i)

          val dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(absPath.toString())
            .withFileSizeInBytes(size)
            .withMetrics(extractNumRecords(addStatsVector, i))
            .build()

          addedFiles += dataFile
        }
      }

      val removesVector = batch.getColumnVector(batch.getSchema().indexOf("remove"))
      val removePathVector = removesVector.getChild(0)
      val removeSizeVector = removesVector.getChild(5)
      val removeStatsVector = removesVector.getChild(6)
      for (i <- 0 until removesVector.getSize) {
        if (!removesVector.isNullAt(i)) {
          val removePath = removePathVector.getString(i)
          val absPath = absolutePath(deltaTableLocation, removePath)

          val size = removeSizeVector.getLong(i)

          val dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(absPath.toString())
            .withFileSizeInBytes(size)
            .withMetrics(extractNumRecords(removeStatsVector, i))
            .build()

          removedFiles += dataFile
        }
      }
    }

    (addedFiles, removedFiles)
  }

  private def extractNumRecords(statsVector: ColumnVector, i: Int): Metrics = {
    if (statsVector.isNullAt(i)) {
      new Metrics(0, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap())
    } else {
      val jsonStats = statsVector.getString(i)
      val jsonNode = MAPPER.readValue(jsonStats, classOf[JsonNode])
      metrics(jsonNode)
    }
  }

  private def metrics(node: JsonNode): Metrics = {
    val pNode = node.get("numRecords")
    val rowCount: Long = pNode.asLong()
    return new Metrics(rowCount, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap())
  }

  private def absolutePath(basePath: String, relativePath: String): Path = {
    val base = new Path(basePath)
    val relative = new Path(relativePath)
    if (relative.isAbsolute) {
      relative
    } else {
      new Path(base, relative)
    }
  }
}
