package io.unitycatalog.server.service.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.Pair;

public class IcebergMetadataConversion {
  private static final String ICEBERG_SPARK_CLASS_NAME =
      "org.apache.spark.sql.delta.commands.convert.IcebergTable";
  private static final Configuration HADOOP_CONF = new Configuration();
  private static final DefaultEngine DEFAULT_ENGINE = DefaultEngine.create(HADOOP_CONF);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static final String DELTA_TABLE_ACCESS_ENABLED_THROUGH_IRC =
      "supports_read_write_through_IRC";
  public static final String ICEBERG_METADATA_PROP = "iceberg_metadata_prop";
  public static final String ICEBERG_IS_STAGED_PROP = "iceberg_is_staged_prop";

  private static final String CONVERTER_CLASS = "io.unitycatalog.spark.ConvertIcebergToDeltaUtils";
  private static final String CONVERTER_SCALA_CLASS =
      "io.unitycatalog.spark.ConvertIcebergToDeltaScalaUtils";

  public static long convertToDelta(String icebergTable, String deltaTable) {
    return convertToDeltaHelper(icebergTable, deltaTable);
  }

  public static Pair<Long, String> backfillIcebergMetadata(
      String deltaTable, long lastDeltaVersionConverted, String lastIcebergManifestLocation) {
    return backfillIcebergMetadataHelper(
        deltaTable, lastDeltaVersionConverted, lastIcebergManifestLocation);
  }

  public static long convertToDeltaHelper(
      String icebergManifestLocation, String deltaTableLocation) {
    long startTimeMs = System.currentTimeMillis();
    try {
      Table icebergTable =
          createIcebergTable(
              icebergManifestLocation,
              deltaTableLocation,
              newManifestListLocation -> {
                // do nothing
              });
      Snapshot icebergSnapshot = icebergTable.currentSnapshot();
      io.delta.kernel.Table kernelTable =
          io.delta.kernel.Table.forPath(DEFAULT_ENGINE, deltaTableLocation);
      boolean existingTable = true;
      try {
        kernelTable.getLatestSnapshot(DEFAULT_ENGINE);
      } catch (TableNotFoundException e) {
        existingTable = false;
      }
      TransactionBuilder txnBuilder =
          kernelTable.createTransactionBuilder(DEFAULT_ENGINE, "UC", Operation.CREATE_TABLE);
      if (existingTable) {
        // existing table. Let's find what is the last snapshot of the Iceberg
        // the Delta table is synced to
        Pair<List<DataFileStatus>, List<DataFileStatus>> actions =
            getDeltaActionsFromIcebergCommit(icebergSnapshot, deltaTableLocation);
        List<DataFileStatus> addFiles = actions.first();
        List<DataFileStatus> removeFiles = actions.second();
        Transaction txn = txnBuilder.build(DEFAULT_ENGINE);
        Row txnState = txn.getTransactionState(DEFAULT_ENGINE);
        DataWriteContext writeContext =
            Transaction.getWriteContext(DEFAULT_ENGINE, txnState, new HashMap<>());
        CloseableIterator<DataFileStatus> cAddIter = Utils.toCloseableIterator(addFiles.iterator());
        CloseableIterator<DataFileStatus> cRemoveIter =
            Utils.toCloseableIterator(removeFiles.iterator());
        CloseableIterable<io.delta.kernel.data.Row> cAddActions =
            CloseableIterable.inMemoryIterable(
                Transaction.generateAppendActions(DEFAULT_ENGINE, txnState, cAddIter, writeContext)
                    .combine(
                        Transaction.generateRemoveActions(
                            DEFAULT_ENGINE, txnState, cRemoveIter, writeContext)));
        TransactionCommitResult commitStatus = txn.commit(DEFAULT_ENGINE, cAddActions);
        return commitStatus.getVersion();
      } else {
        // new table
        // update the metadata of the Delta table
        Map<String, String> properties = new HashMap<>(icebergTable.properties());
        properties.put("delta.columnMapping.mode", "id");
        io.delta.kernel.types.StructType kernelSchema =
            SchemaUtils.fromIcebergSchema(icebergTable.schema().asStruct());
        Transaction txn =
            txnBuilder
                .withSchema(DEFAULT_ENGINE, kernelSchema)
                .withTableProperties(DEFAULT_ENGINE, properties)
                .build(DEFAULT_ENGINE);
        Row txnState = txn.getTransactionState(DEFAULT_ENGINE);
        DataWriteContext writeContext =
            Transaction.getWriteContext(DEFAULT_ENGINE, txnState, new HashMap<>());
        Pair<List<DataFileStatus>, List<DataFileStatus>> actions =
            getDeltaActionsFromIcebergCommit(icebergSnapshot, deltaTableLocation);
        List<DataFileStatus> addFiles = actions.first();
        CloseableIterator<DataFileStatus> cIter = Utils.toCloseableIterator(addFiles.iterator());
        CloseableIterable<io.delta.kernel.data.Row> cAddActions =
            CloseableIterable.inMemoryIterable(
                Transaction.generateAppendActions(DEFAULT_ENGINE, txnState, cIter, writeContext));
        TransactionCommitResult commitStatus = txn.commit(DEFAULT_ENGINE, cAddActions);
        return commitStatus.getVersion();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      long endTimeMs = System.currentTimeMillis();
      System.out.println(
          "Time taken to convert Iceberg to Delta: " + (endTimeMs - startTimeMs) + " ms");
    }
  }

  public static Pair<Long, String> backfillIcebergMetadataHelper(
      String deltaTableLocation, long lastConvertedVersion, String lastManifestLocation) {
    try {
      io.delta.kernel.Table kernelTable =
          io.delta.kernel.Table.forPath(DEFAULT_ENGINE, deltaTableLocation);
      long latestDeltaVersion;
      try {
        latestDeltaVersion =
            kernelTable.getLatestSnapshot(DEFAULT_ENGINE).getVersion(DEFAULT_ENGINE);
      } catch (TableNotFoundException e) {
        latestDeltaVersion = -1;
      }

      AtomicReference<String> lastManifest = new AtomicReference<>(lastManifestLocation);
      long lastVersion = lastConvertedVersion;

      for (long deltaVersion = lastConvertedVersion + 1;
          deltaVersion <= latestDeltaVersion;
          deltaVersion++) {
        Pair<List<DataFile>, List<DataFile>> actions =
            getIcebergActionsFromDeltaCommit(deltaTableLocation, deltaVersion);
        List<DataFile> addFiles = actions.first();
        List<DataFile> removeFiles = actions.second();

        Table icebergTable =
            createIcebergTable(
                lastManifest.get(),
                deltaTableLocation,
                newManifestListLocation -> {
                  lastManifest.set(newManifestListLocation);
                });

        org.apache.iceberg.Transaction icebergTxn = icebergTable.newTransaction();

        if (!removeFiles.isEmpty() && !addFiles.isEmpty()) {
          RewriteFiles rewriteFilesOps = icebergTxn.newRewrite();

          for (DataFile file : addFiles) {
            rewriteFilesOps.addFile(file);
          }
          for (DataFile file : removeFiles) {
            rewriteFilesOps.deleteFile(file);
          }

          rewriteFilesOps.commit();
        } else if (!addFiles.isEmpty()) {
          AppendFiles appendFilesOps = icebergTxn.newAppend();

          for (DataFile file : addFiles) {
            appendFilesOps.appendFile(file);
          }

          appendFilesOps.commit();
        } else if (!removeFiles.isEmpty()) {
          DeleteFiles deleteFilesOps = icebergTxn.newDelete();

          for (DataFile file : removeFiles) {
            deleteFilesOps.deleteFile(file);
          }

          deleteFilesOps.commit();
        }

        icebergTxn.commitTransaction();
        lastVersion = deltaVersion;
      }

      return Pair.of(lastVersion, lastManifest.get());
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  // Create the Iceberg table
  public static Table createIcebergTable(
      String manifestLocation, String tableLocation, NewManifestReceiver newManifestReceiver) {
    StaticTableOperations ops =
        new StaticTableOperations(manifestLocation, new HadoopFileIO(HADOOP_CONF)) {
          @Override
          public String metadataFileLocation(String fileName) {
            return String.format("%s/%s/%s", tableLocation, "metadata", fileName);
          }

          @Override
          public void commit(TableMetadata base, TableMetadata metadata) {
            String codecName =
                metadata.property(
                    TableProperties.METADATA_COMPRESSION,
                    TableProperties.METADATA_COMPRESSION_DEFAULT);
            TableMetadataParser.Codec codec = TableMetadataParser.Codec.fromName(codecName);
            String fileExtension = TableMetadataParser.getFileExtension(codec);
            Path tempMetadataFile =
                new Path(
                    String.format(
                        "%s/%s/%s",
                        tableLocation, "metadata", UUID.randomUUID().toString() + fileExtension));
            OutputFile outputFile = io().newOutputFile(tempMetadataFile.toString());
            TableMetadataParser.write(metadata, outputFile);

            newManifestReceiver.receive(tempMetadataFile.toString());
          }
        };
    return new BaseTable(ops, manifestLocation);
  }

  private static Pair<List<DataFileStatus>, List<DataFileStatus>> getDeltaActionsFromIcebergCommit(
      Snapshot snapshot, String deltaTableLocation) throws IOException {
    Path deltaPath = new Path(deltaTableLocation);
    FileSystem fs = deltaPath.getFileSystem(HADOOP_CONF);

    List<DataFileStatus> addFiles = new ArrayList<>();
    List<DataFileStatus> removeFiles = new ArrayList<>();

    HadoopFileIO fileIO = new HadoopFileIO(HADOOP_CONF);

    for (DataFile file : snapshot.addedDataFiles(fileIO)) {
      Path filePath = new Path(file.path().toString());
      Path relativePath =
          new Path(
              fs.makeQualified(deltaPath).toUri().relativize(fs.makeQualified(filePath).toUri()));
      addFiles.add(
          new DataFileStatus(
              relativePath.toString(),
              file.fileSizeInBytes(),
              122L, // modification time
              Optional.empty()));
    }

    for (DataFile file : snapshot.removedDataFiles(fileIO)) {
      Path filePath = new Path(file.path().toString());
      Path relativePath =
          new Path(
              fs.makeQualified(deltaPath).toUri().relativize(fs.makeQualified(filePath).toUri()));
      removeFiles.add(
          new DataFileStatus(
              relativePath.toString(),
              file.fileSizeInBytes(),
              234L, // modification time
              Optional.empty()));
    }

    return Pair.of(addFiles, removeFiles);
  }

  // Interface for newManifestReceiver
  public interface NewManifestReceiver {
    void receive(String newManifestLocation);
  }

  private static Pair<List<DataFile>, List<DataFile>> getIcebergActionsFromDeltaCommit(
      String deltaTableLocation, long version) {
    TableImpl kernelTable =
        (TableImpl) io.delta.kernel.Table.forPath(DEFAULT_ENGINE, deltaTableLocation);
    CloseableIterator<ColumnarBatch> changesIter =
        kernelTable.getChanges(
            DEFAULT_ENGINE,
            version,
            version,
            new HashSet<>(Arrays.asList(DeltaAction.ADD, DeltaAction.REMOVE)));

    List<DataFile> addedFiles = new ArrayList<>();
    List<DataFile> removedFiles = new ArrayList<>();

    Path tablePath = new Path(deltaTableLocation);

    while (changesIter.hasNext()) {
      ColumnarBatch batch = changesIter.next();
      ColumnVector addsVector = batch.getColumnVector(batch.getSchema().indexOf("add"));

      ColumnVector addPathVector = addsVector.getChild(0);
      ColumnVector addSizeVector = addsVector.getChild(2);
      ColumnVector addStatsVector = addsVector.getChild(7);
      for (int i = 0; i < addsVector.getSize(); i++) {
        if (!addsVector.isNullAt(i)) {
          String path = addPathVector.getString(i);
          Path absPath = absolutePath(deltaTableLocation, path);
          long size = addSizeVector.getLong(i);

          DataFile dataFile =
              DataFiles.builder(PartitionSpec.unpartitioned())
                  .withPath(absPath.toString())
                  .withFileSizeInBytes(size)
                  .withMetrics(extractNumRecords(addStatsVector, i))
                  .build();

          addedFiles.add(dataFile);
        }
      }

      ColumnVector removesVector = batch.getColumnVector(batch.getSchema().indexOf("remove"));
      ColumnVector removePathVector = removesVector.getChild(0);
      ColumnVector removeSizeVector = removesVector.getChild(5);
      ColumnVector removeStatsVector = removesVector.getChild(6);
      for (int i = 0; i < removesVector.getSize(); i++) {
        if (!removesVector.isNullAt(i)) {
          String removePath = removePathVector.getString(i);
          Path absPath = absolutePath(deltaTableLocation, removePath);

          long size = removeSizeVector.getLong(i);

          DataFile dataFile =
              DataFiles.builder(PartitionSpec.unpartitioned())
                  .withPath(absPath.toString())
                  .withFileSizeInBytes(size)
                  .withMetrics(extractNumRecords(removeStatsVector, i))
                  .build();

          removedFiles.add(dataFile);
        }
      }
    }

    return Pair.of(addedFiles, removedFiles);
  }

  private static Metrics extractNumRecords(ColumnVector statsVector, int i) {
    if (statsVector.isNullAt(i)) {
      return new Metrics(
          0L,
          Collections.emptyMap(),
          Collections.emptyMap(),
          Collections.emptyMap(),
          Collections.emptyMap(),
          Collections.emptyMap(),
          Collections.emptyMap());
    } else {
      String jsonStats = statsVector.getString(i);
      JsonNode jsonNode;
      try {
        jsonNode = MAPPER.readValue(jsonStats, JsonNode.class);
      } catch (Exception e) {
        throw new RuntimeException("Failed to parse JSON stats", e);
      }
      return metrics(jsonNode);
    }
  }

  private static Metrics metrics(JsonNode node) {
    JsonNode pNode = node.get("numRecords");
    long rowCount = pNode.asLong();
    return new Metrics(
        rowCount,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap());
  }

  private static Path absolutePath(String basePath, String relativePath) {
    Path base = new Path(basePath);
    Path relative = new Path(relativePath);
    if (relative.isAbsolute()) {
      return relative;
    } else {
      return new Path(base, relative);
    }
  }
}
