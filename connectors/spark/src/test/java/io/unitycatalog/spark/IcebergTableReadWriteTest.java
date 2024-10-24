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
    SparkSession session = createSparkSessionWithIcebergRestCatalog();
    createIcebergNamespaces();

    session.sql("CREATE TABLE iceberg.`main.default`.test USING iceberg AS SELECT 1, 'a'");

    Row row = session.sql("SELECT * FROM iceberg.`main.default`.test").collectAsList().get(0);
    assertThat(row.getInt(0)).isEqualTo(1);
    assertThat(row.getString(1)).isEqualTo("a");

    session.stop();
  }

  protected SparkSession createSparkSessionWithIcebergRestCatalog() {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("IcebergSession")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.cache-enabled", "false")
            .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
            .config(
                "spark.sql.catalog.iceberg.uri",
                serverConfig.getServerUrl() + "/api/2.1/unity-catalog/iceberg")
            .config("spark.sql.catalog.iceberg.token", serverConfig.getAuthToken())
            // .config(s"spark.sql.catalog.$icebergCatalog.s3.endpoint", storageUrl)
            .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO");

    // Use fake file system for cloud storage so that we can test credentials.
    builder.config("fs.s3.impl", S3CredentialTestFileSystem.class.getName());
    builder.config("fs.gs.impl", GCSCredentialTestFileSystem.class.getName());
    builder.config("fs.abfs.impl", AzureCredentialTestFileSystem.class.getName());
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
