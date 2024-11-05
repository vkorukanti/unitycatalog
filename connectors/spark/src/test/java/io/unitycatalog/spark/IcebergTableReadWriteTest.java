package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IcebergTableReadWriteTest extends BaseSparkIntegrationTest {

  private static final String ANOTHER_PARQUET_TABLE = "test_parquet_another";
  private static final String PARQUET_TABLE_PARTITIONED = "test_parquet_partitioned";
  private static final String DELTA_TABLE = "test_delta";
  private static final String PARQUET_TABLE = "test_parquet";
  private static final String ANOTHER_DELTA_TABLE = "test_delta_another";
  private static final String DELTA_TABLE_PARTITIONED = "test_delta_partitioned";

  private static final String ICEBERG_CATALOG = "iceberg";

  private final File dataDir = new File(System.getProperty("java.io.tmpdir"), "spark_test");

  private TableOperations tableOperations;

  @Test
  public void testReadWrite() throws IOException, ApiException {
    // Test both `spark_catalog` and other catalog names.
    SparkSession session = createSparkSession();
    createIcebergNamespaces();

    session.sql(
        String.format(
            "CREATE TABLE iceberg.`main.default`.test USING iceberg LOCATION 's3://databricks-tdas/irctest/iceberg_test-%s'"
                + " AS SELECT 1 as c1, '1' as c2 ",
            System.currentTimeMillis()));

    List<Row> results1 = session.sql("SELECT * FROM iceberg.`main.default`.test").collectAsList();
    assertThat(results1.size()).isEqualTo(1);
    assertContainsRow(results1, 1, "1");

    // INSERT DATA THROUGH IRC
    {
      session.sql("INSERT INTO iceberg.`main.default`.test " + values(2, 20));

      List<Row> results2 = session.sql("SELECT * FROM iceberg.`main.default`.test").collectAsList();
      assertThat(results2.size()).isEqualTo(20);
      for (int i = 1; i <= 20; i++) {
        assertContainsRow(results2, i, String.valueOf(i));
      }

      List<Row> deltaResults2 = session.sql("SELECT * FROM main.default.test").collectAsList();
      assertThat(deltaResults2.size()).isEqualTo(20);
      for (int i = 1; i <= 20; i++) {
        assertContainsRow(deltaResults2, i, String.valueOf(i));
      }
    }

    // INSERT DATA THROUGH DELTA
    {
      session.sql("INSERT INTO main.default.test " + values(21, 40));

      List<Row> results2 = session.sql("SELECT * FROM iceberg.`main.default`.test").collectAsList();
      assertThat(results2.size()).isEqualTo(40);
      for (int i = 1; i <= 40; i++) {
        assertContainsRow(results2, i, String.valueOf(i));
      }

      List<Row> deltaResults2 = session.sql("SELECT * FROM main.default.test").collectAsList();
      assertThat(deltaResults2.size()).isEqualTo(40);
      for (int i = 1; i <= 40; i++) {
        assertContainsRow(deltaResults2, i, String.valueOf(i));
      }
    }

    // DML COMMANDS THROUGH IRC
    {
      session.sql("UPDATE iceberg.`main.default`.test SET c2 = '5-updated' WHERE c1 = 5");

      List<Row> results2 = session.sql("SELECT * FROM iceberg.`main.default`.test").collectAsList();
      assertThat(results2.size()).isEqualTo(40);
      for (int i = 1; i <= 40; i++) {
        assertContainsRow(results2, i, String.valueOf(i) + (i == 5 ? "-updated" : ""));
      }

      List<Row> deltaResults2 = session.sql("SELECT * FROM main.default.test").collectAsList();
      assertThat(deltaResults2.size()).isEqualTo(40);
      for (int i = 1; i <= 40; i++) {
        assertContainsRow(results2, i, String.valueOf(i) + (i == 5 ? "-updated" : ""));
      }
    }

    // DML COMMANDS THROUGH DELTA
    {
      session.sql("UPDATE main.default.test SET c2 = '10-updated' WHERE c1 = 10");

      List<Row> results2 = session.sql("SELECT * FROM iceberg.`main.default`.test").collectAsList();
      assertThat(results2.size()).isEqualTo(40);
      for (int i = 1; i <= 40; i++) {
        assertContainsRow(results2, i, String.valueOf(i) + ((i == 10 || i == 5) ? "-updated" : ""));
      }

      List<Row> deltaResults2 = session.sql("SELECT * FROM main.default.test").collectAsList();
      assertThat(deltaResults2.size()).isEqualTo(40);
      for (int i = 1; i <= 40; i++) {
        assertContainsRow(results2, i, String.valueOf(i) + ((i == 10 || i == 5) ? "-updated" : ""));
      }
    }

    session.stop();
  }

  private String values(int start, int end) {
    StringBuilder sb = new StringBuilder();
    sb.append("VALUES ");
    for (int i = start; i <= end; i++) {
      sb.append(String.format("(%d, '%s')", i, i));
      if (i < end) {
        sb.append(", ");
      }
    }
    return sb.toString();
  }

  @Test
  public void testReadWritePertTest() throws IOException, ApiException {
    // Test both `spark_catalog` and other catalog names.
    SparkSession session = createSparkSession();
    createIcebergNamespaces();

    session.sql(
        "CREATE TABLE iceberg.`main.default`.test USING iceberg AS SELECT 1 as c1, '1' as c2");

    List<Row> results1 = session.sql("SELECT * FROM iceberg.`main.default`.test").collectAsList();
    assertThat(results1.size()).isEqualTo(1);
    assertContainsRow(results1, 1, "1");

    // INSERT DATA
    {
      session.sql(
          "INSERT INTO iceberg.`main.default`.test SELECT /*+ REPARTITION(10000) */ * FROM "
              + values(2, 2000000));

      List<Row> results2 = session.sql("SELECT * FROM iceberg.`main.default`.test").collectAsList();
      assertThat(results2.size()).isEqualTo(2000000);
      for (int i = 1; i <= 2000000; i++) {
        assertContainsRow(results2, i, String.valueOf(i));
      }

      //      List<Row> deltaResults2 = session.sql("SELECT * FROM
      // main.default.test").collectAsList();
      //      assertThat(deltaResults2.size()).isEqualTo(20);
      //      for (int i = 1; i <= 20; i++) {
      //        assertContainsRow(deltaResults2, i, String.valueOf(i));
      //      }
    }

    session.stop();
  }

  private void assertContainsRow(List<Row> rows, int id, String data) {
    assertThat(rows.stream().anyMatch(row -> row.getInt(0) == id && row.getString(1).equals(data)))
        .isTrue();
  }

  // with Iceberg Rest catalog client and UCSingleCatalog for Delta
  protected SparkSession createSparkSession() {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("IcebergSession")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.cache-enabled", "false")
            .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
            .config(
                "spark.sql.catalog.iceberg.uri",
                serverConfig.getServerUrl() + "/api/2.1/unity-catalog/iceberg")
            .config("spark.sql.catalog.iceberg.token", serverConfig.getAuthToken())
            // .config(s"spark.sql.catalog.$icebergCatalog.s3.endpoint", storageUrl)
            .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog");

    String catalogConf = "spark.sql.catalog.main";
    builder =
        builder
            .config(catalogConf, UCSingleCatalog.class.getName())
            .config(catalogConf + ".uri", serverConfig.getServerUrl())
            .config(catalogConf + ".token", serverConfig.getAuthToken())
            .config(catalogConf + ".__TEST_NO_DELTA__", "true");

    builder =
        builder
            // Specify these config as environment variables
            //            .config("spark.hadoop.fs.s3a.access.key", "xxx")
            //            .config("spark.hadoop.fs.s3a.secret.key", "xxx")
            //            .config("spark.hadoop.fs.s3a.session.token", "xxx")
            //            .config("spark.hadoop.fs.s3a.region", "us-west-2")
            .config("spark.hadoop.fs.s3.impl", S3AFileSystem.class.getName());

    // Use fake file system for cloud storage so that we can test credentials.
    //    builder.config("fs.s3.impl", S3CredentialTestFileSystem.class.getName());
    //    builder.config("fs.gs.impl", GCSCredentialTestFileSystem.class.getName());
    //    builder.config("fs.abfs.impl", AzureCredentialTestFileSystem.class.getName());
    return builder.getOrCreate();
  }

  protected void createIcebergNamespaces() throws ApiException {
    // Common setup operations such as creating a catalog and schema
    catalogOperations.createCatalog(new CreateCatalog().name("main").comment(TestUtils.COMMENT));
    schemaOperations.createSchema(new CreateSchema().name("default").catalogName("main"));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    tableOperations = new SdkTableOperations(createApiClient(serverConfig));
  }

  @Override
  public void cleanUp() {
    super.cleanUp();
    UCSingleCatalog.LOAD_DELTA_CATALOG().set(true);
    try {
      JavaUtils.deleteRecursively(dataDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
