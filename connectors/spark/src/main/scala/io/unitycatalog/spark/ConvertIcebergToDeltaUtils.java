package io.unitycatalog.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.delta.DeltaConfigs;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.OptimisticTransaction;
import org.apache.spark.sql.delta.actions.Metadata;
import org.apache.spark.sql.delta.commands.CloneIcebergSource;
import org.apache.spark.sql.delta.commands.CloneSource;
import org.apache.spark.sql.delta.commands.CloneTableCommand;

import org.apache.spark.sql.delta.commands.convert.IcebergSchemaUtils;
import org.apache.spark.sql.types.StructType;
import scala.None$;
import scala.Option;
import scala.Some$;
import scala.collection.Map;
import scala.collection.Map$;

import scala.collection.JavaConverters.*;

public class ConvertIcebergToDeltaUtils
{
    private static final String ICEBERG_SPARK_CLASS_NAME =
            "org.apache.spark.sql.delta.commands.convert.IcebergTable";

    private static final Configuration HADOOP_CONF = new Configuration();

    public static long convertToDeltaCloneCommand(String icebergTable, String deltaTable)
    {
        SparkSession spark = createSparkSession();
        TableIdentifier icebergTableId = new TableIdentifier(icebergTable, Some$.MODULE$.apply("iceberg"));

//        ConvertTargetTable icebergTargetTable =
//                ConvertUtils.getIcebergTable(spark, icebergTable, None$.apply(null), None$.apply(null));

        TableIdentifier deltaTableId = new TableIdentifier(deltaTable, Some$.MODULE$.apply("delta"));

        CloneSource cloneSource = new CloneIcebergSource(icebergTableId, None$.apply(null), None$.apply(null), spark);

        CloneTableCommand cloneTableCommand = new CloneTableCommand(
                cloneSource,
                deltaTableId,
                Map$.MODULE$.empty(),
                new Path(deltaTable));

        DeltaLog deltaLog = DeltaLog.forTable(spark, deltaTable);
        OptimisticTransaction txn = deltaLog.startTransaction();

        cloneTableCommand.handleClone(
                spark,
                txn,
                deltaLog);


        return 0L; // return the version of the Delta table
    }

    private static SparkSession createSparkSession()
    {
        return SparkSession.builder()
                .appName("IcebergSession")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hadoop")
                .config("spark.sql.catalog.local.warehouse", "/path/to/your/warehouse")
                // .config(s"spark.sql.catalog.$icebergCatalog.s3.endpoint", storageUrl)
                .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
                .getOrCreate();
    }

    public static long convertToDelta(String icebergManifestLocation, String deltaTableLocation)
    {
        SparkSession spark = createSparkSession();
        TableIdentifier deltaTableId = new TableIdentifier(deltaTableLocation, Some$.MODULE$.apply("delta"));

        DeltaLog deltaLog = DeltaLog.forTable(spark, deltaTableLocation);
        OptimisticTransaction txn = deltaLog.startTransaction();

        Table icebergTable = createTable(icebergManifestLocation);
        Snapshot icebergSnapshpot = icebergTable.currentSnapshot();

        if (txn.readVersion() > -1)
        {
            // existing table. Lets find what is the last snaphot of the Iceberg
            // the Delta table is synced to
            Option<String> lastIcebergSnapshot =
                    txn.snapshot().getProperties().get("iceberg.snapshot.id");
        }
        else
        {
            // new table

            // update the metadata of the Delta table
//            StructType deltaSchema = getDeltaSchema(icebergTable);
//
//            Map<String, String> properties = scala.collection.mutable.Map$.MODULE$.empty();
//            icebergTable.properties().forEach((k, v) -> {
//                properties = properties.updated(k, v);
//            });
//            properties = properties.updated("delta.columnMapping.mode", "id");
//
//
//            Metadata metadata = new Metadata(
//                    deltaSchema.json(),
//                    properties);
        }

        // get schema
        // get the list of data files

        // Create Delta transaction
        // update the schema on the table
        return 0;
    }

    public static void backfillIcebergMetadata(String deltaTableLocation, long startVersion, long endVersion)
    {

    }

    // Create the Iceberg table
    public static Table createTable(String manifestLocation) {
        StaticTableOperations ops = new StaticTableOperations(
                manifestLocation,
                new HadoopFileIO(HADOOP_CONF));

        return new BaseTable(ops, manifestLocation);
    }

    private static StructType getDeltaSchema(Table table)
    {
       return IcebergSchemaUtils.convertIcebergSchemaToSpark(table.schema());
    }
}
