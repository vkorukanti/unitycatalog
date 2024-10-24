package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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

  @Test
  public void testReadWrite() throws IOException, ApiException {
    // Test both `spark_catalog` and other catalog names.
    SparkSession session = createSparkSessionWithIcebergRestCatalog();
    createIcebergNamespaces();

    session.sql("CREATE TABLE iceberg.`main.default`.test USING iceberg AS SELECT 1, 'a'");

    //    setupExternalDeltaTable(SPARK_CATALOG, DELTA_TABLE, new ArrayList<>(0), session);
    //    testTableReadWrite(SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE, session);
    //
    //    setupExternalDeltaTable(SPARK_CATALOG, DELTA_TABLE_PARTITIONED, Arrays.asList("s"),
    // session);
    //    testTableReadWrite(SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE_PARTITIONED,
    // session);
    //
    //    setupExternalDeltaTable(CATALOG_NAME, DELTA_TABLE, new ArrayList<>(0), session);
    //    testTableReadWrite(CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE, session);
    //
    //    setupExternalDeltaTable(CATALOG_NAME, DELTA_TABLE_PARTITIONED, Arrays.asList("s"),
    // session);
    //    testTableReadWrite(CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE_PARTITIONED,
    // session);

    session.stop();
  }

  private void setupExternalParquetTable(String tableName, List<String> partitionColumns)
      throws IOException, ApiException {
    String location = generateTableLocation(SPARK_CATALOG, tableName);
    setupExternalParquetTable(tableName, location, partitionColumns);
  }

  private void setupExternalParquetTable(
      String tableName, String location, List<String> partitionColumns)
      throws IOException, ApiException {
    setupTables(
        SPARK_CATALOG, tableName, DataSourceFormat.PARQUET, location, partitionColumns, false);
  }

  private void setupExternalDeltaTable(
      String catalogName, String tableName, List<String> partitionColumns, SparkSession session)
      throws IOException, ApiException {
    String location = generateTableLocation(catalogName, tableName);
    setupExternalDeltaTable(catalogName, tableName, location, partitionColumns, session);
  }

  private String generateTableLocation(String catalogName, String tableName) throws IOException {
    return new File(new File(dataDir, catalogName), tableName).getCanonicalPath();
  }

  private void setupDeltaTableLocation(
      SparkSession session, String location, List<String> partitionColumns) {
    // The Delta path can't be empty, need to initialize before read.
    String partitionClause;
    if (partitionColumns.isEmpty()) {
      partitionClause = "";
    } else {
      partitionClause = String.format(" PARTITIONED BY (%s)", String.join(", ", partitionColumns));
    }
    // Temporarily disable the credential check when setting up the external Delta location which
    // does not involve Unity Catalog at all.
    CredentialTestFileSystem.credentialCheckEnabled = false;
    session.sql(
        String.format(
            "CREATE TABLE delta.`%s`(i INT, s STRING) USING delta %s", location, partitionClause));
    CredentialTestFileSystem.credentialCheckEnabled = true;
  }

  private void setupExternalDeltaTable(
      String catalogName,
      String tableName,
      String location,
      List<String> partitionColumns,
      SparkSession session)
      throws IOException, ApiException {
    setupDeltaTableLocation(session, location, partitionColumns);
    setupTables(catalogName, tableName, DataSourceFormat.DELTA, location, partitionColumns, false);
  }

  private void testTableReadWrite(String tableFullName, SparkSession session) {
    assertThat(session.sql("SELECT * FROM " + tableFullName).collectAsList()).isEmpty();
    session.sql("INSERT INTO " + tableFullName + " SELECT 1, 'a'");
    Row row = session.sql("SELECT * FROM " + tableFullName).collectAsList().get(0);
    assertThat(row.getInt(0)).isEqualTo(1);
    assertThat(row.getString(1)).isEqualTo("a");
  }

  private void setupTables(
      String catalogName,
      String tableName,
      DataSourceFormat format,
      String location,
      List<String> partitionColumns,
      boolean isManaged)
      throws IOException, ApiException {
    Integer partitionIndex1 = partitionColumns.indexOf("i");
    if (partitionIndex1 == -1) partitionIndex1 = null;
    Integer partitionIndex2 = partitionColumns.indexOf("s");
    if (partitionIndex2 == -1) partitionIndex2 = null;

    ColumnInfo c1 =
        new ColumnInfo()
            .name("i")
            .typeText("INTEGER")
            .typeJson("{\"type\": \"integer\"}")
            .typeName(ColumnTypeName.INT)
            .typePrecision(10)
            .typeScale(0)
            .position(0)
            .partitionIndex(partitionIndex1)
            .comment("Integer column")
            .nullable(true);

    ColumnInfo c2 =
        new ColumnInfo()
            .name("s")
            .typeText("STRING")
            .typeJson("{\"type\": \"string\"}")
            .typeName(ColumnTypeName.STRING)
            .position(1)
            .partitionIndex(partitionIndex2)
            .comment("String column")
            .nullable(true);
    TableType tableType;
    if (isManaged) {
      tableType = TableType.MANAGED;
    } else {
      tableType = TableType.EXTERNAL;
    }
    CreateTable createTableRequest =
        new CreateTable()
            .name(tableName)
            .catalogName(catalogName)
            .schemaName(SCHEMA_NAME)
            .columns(Arrays.asList(c1, c2))
            .comment(COMMENT)
            .tableType(tableType)
            .dataSourceFormat(format);
    if (!isManaged) {
      createTableRequest = createTableRequest.storageLocation(location);
    }
    tableOperations.createTable(createTableRequest);
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
