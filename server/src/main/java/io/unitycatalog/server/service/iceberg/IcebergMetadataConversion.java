package io.unitycatalog.server.service.iceberg;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URI;
import org.apache.iceberg.util.Pair;

public class IcebergMetadataConversion {
  public static final String DELTA_TABLE_ACCESS_ENABLED_THROUGH_IRC =
      "supports_read_write_through_IRC";
  public static final String ICEBERG_METADATA_PROP = "iceberg_metadata_prop";
  public static final String ICEBERG_IS_STAGED_PROP = "iceberg_is_staged_prop";

  private static final String CONVERTER_CLASS = "io.unitycatalog.spark.ConvertIcebergToDeltaUtils";
  private static final String CONVERTER_SCALA_CLASS =
      "io.unitycatalog.spark.ConvertIcebergToDeltaScalaUtils";

  public static long convertToDelta(String icebergTable, String deltaTable) {
    try {
      URI icebergURI = new URI(icebergTable);
      URI deltaURI = new URI(deltaTable);
      // Step 1: Obtain the Class object
      // Class<?> clazz = Class.forName(CONVERTER_CLASS);
      Class<?> clazz = Class.forName(CONVERTER_SCALA_CLASS);

      // Step 2: Get the Method object
      // The method name is "convertToDelta" and it has three parameters: String, String
      Method method = clazz.getMethod("convertToDelta", String.class, String.class);

      Constructor<?> constructor = clazz.getConstructor();

      // Step 3: Create a new instance
      Object instance = constructor.newInstance();

      // Step 3: Invoke the method
      // Since convertToDelta is a static method, pass null as the first argument
      Object deltaVersion = method.invoke(instance, icebergTable, deltaTable);
      return (Long) deltaVersion;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static Pair<Long, String> backfillIcebergMetadata(
      String deltaTable, long lastDeltaVersionConverted, String lastIcebergManifestLocation) {
    try {
      URI deltaURI = new URI(deltaTable);
      URI icebergManifestURI = new URI(lastIcebergManifestLocation);

      // Step 1: Obtain the Class object
      Class<?> clazz = Class.forName(CONVERTER_SCALA_CLASS);

      // Step 2: Get the Method object
      Method method =
          clazz.getMethod("backfillIcebergMetadata", String.class, long.class, String.class);

      // Step 3: Create a new instance
      Constructor<?> constructor = clazz.getConstructor();
      Object instance = constructor.newInstance();

      // Step 3: Invoke the method
      Object lastManifestVersion =
          method.invoke(
              instance, deltaTable, lastDeltaVersionConverted, lastIcebergManifestLocation);
      return (Pair<Long, String>) lastManifestVersion;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
