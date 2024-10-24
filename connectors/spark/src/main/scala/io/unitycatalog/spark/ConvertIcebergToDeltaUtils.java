package io.unitycatalog.spark;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.OptimisticTransaction;
import org.apache.spark.sql.delta.commands.CloneIcebergSource;
import org.apache.spark.sql.delta.commands.CloneSource;
import org.apache.spark.sql.delta.commands.CloneTableCommand;
import org.apache.spark.sql.delta.commands.convert.ConvertTargetTable;
import org.apache.spark.sql.delta.commands.convert.ConvertUtils;

import scala.None$;
import scala.Some$;
import scala.collection.Map$;

public class ConvertIcebergToDeltaUtils
{
    private static final String ICEBERG_SPARK_CLASS_NAME =
            "org.apache.spark.sql.delta.commands.convert.IcebergTable";

    public static void convertToDelta(String icebergTable, String deltaTable)
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
}
