package io.unitycatalog.spark

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.iceberg.BaseTable
import org.apache.iceberg.Snapshot
import org.apache.iceberg.StaticTableOperations
import org.apache.iceberg.Table
import org.apache.iceberg.hadoop.HadoopFileIO
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaConfigs
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.actions.{Action, AddFile, FileAction, Metadata, RemoveFile}
import org.apache.spark.sql.delta.commands.CloneIcebergSource
import org.apache.spark.sql.delta.commands.CloneSource
import org.apache.spark.sql.delta.commands.CloneTableCommand
import org.apache.spark.sql.delta.commands.convert.IcebergSchemaUtils
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.delta.DeltaOperations

import java.nio.file.FileSystem
import scala.None$
import scala.Option
import scala.Some$
import scala.collection.Map
import scala.collection.Map$
import scala.collection.JavaConverters._

class ConvertIcebergToDeltaScalaUtils {
  private val ICEBERG_SPARK_CLASS_NAME = "org.apache.spark.sql.delta.commands.convert.IcebergTable"

  private val HADOOP_CONF = new Configuration

  def convertToDelta(icebergManifestLocation: String, deltaTableLocation: String): Long = {
    try {
      val spark: SparkSession = createSparkSession
      val deltaTableId: TableIdentifier = new TableIdentifier(deltaTableLocation, Some("delta"))

      val deltaLog: DeltaLog = DeltaLog.forTable(spark, deltaTableLocation)
      val txn: OptimisticTransaction = deltaLog.startTransaction

      val icebergTable: Table = createTable(icebergManifestLocation)

      val icebergSnapshpot: Snapshot = icebergTable.currentSnapshot
      if (txn.readVersion > -(1)) {
        // existing table. Lets find what is the last snaphot of the Iceberg
        // the Delta table is synced to

        // TODO: make sure there are no schema changes
        return txn.commit(
          getFileActions(icebergSnapshpot, deltaTableLocation),
          DeltaOperations.ManualUpdate,
          Map.empty)
      }
      else {
        // new table
        // update the metadata of the Delta table
        val deltaSchema: StructType = getDeltaSchema(icebergTable)
        val properties = icebergTable.properties.asScala.toMap + ("delta.columnMapping.mode" -> "id")

        val metadata: Metadata = Metadata(
          schemaString = deltaSchema.json,
          configuration = properties
        )

        txn.updateMetadata(metadata)
        return txn.commit(
          getFileActions(icebergSnapshpot, deltaTableLocation),
          DeltaOperations.ManualUpdate,
          Map.empty)
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }

  def backfillIcebergMetadata(deltaTableLocation: String, startVersion: Long, endVersion: Long): Unit = {

  }

  // Create the Iceberg table
  def createTable(manifestLocation: String): Table = {
    val ops: StaticTableOperations = new StaticTableOperations(manifestLocation, new HadoopFileIO(HADOOP_CONF))
    return new BaseTable(ops, manifestLocation)
  }

  private def getDeltaSchema(table: Table): StructType = {
    return IcebergSchemaUtils.convertIcebergSchemaToSpark(table.schema)
  }

  private def getFileActions(snapshot: Snapshot, deltaTableLocation: String): Seq[Action] = {
    // get the list of data files
    // create the actions
    val deltaPath = new Path(deltaTableLocation)
    val fs = deltaPath.getFileSystem(HADOOP_CONF)

    val arrayBufferSeq = new scala.collection.mutable.ArrayBuffer[Action]

      snapshot.addedDataFiles(new HadoopFileIO(HADOOP_CONF)).iterator().asScala.foreach { file =>
      // create the action
      val filePath = file.path().toString
      val fsStatus = fs.getFileStatus(new Path(filePath))
      val relativePath = DeltaFileOperations.tryRelativizePath(fs, deltaPath, new Path(filePath))
      arrayBufferSeq += AddFile(
        path = relativePath.toString,
        partitionValues = Map.empty,
        size = fsStatus.getLen(),
        modificationTime = fsStatus.getModificationTime,
        dataChange = true)
    }

    snapshot.removedDataFiles(new HadoopFileIO(HADOOP_CONF)).iterator().asScala.foreach { file =>
      // create the action
      val filePath = file.path().toString
      val fsStatus = fs.getFileStatus(new Path(filePath))
      val relativePath = DeltaFileOperations.tryRelativizePath(fs, deltaPath, new Path(filePath))
      arrayBufferSeq += RemoveFile(
        path = relativePath.toString,
        deletionTimestamp = Some(System.currentTimeMillis()),
        partitionValues = Map.empty,
        dataChange = true)
    }

    return arrayBufferSeq.toSeq
  }

  private def createSparkSession = SparkSession.builder.appName("IcebergSession")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .config(
      "spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/path/to/your/warehouse")
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .getOrCreate
}
