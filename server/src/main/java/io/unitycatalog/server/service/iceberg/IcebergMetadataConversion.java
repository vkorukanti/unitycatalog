package io.unitycatalog.server.service.iceberg;

import java.lang.reflect.Method;
import java.net.URI;

public class IcebergMetadataConversion {
  private static final String CONVERTER_CLASS = "io.unitycatalog.spark.ConvertIcebergToDeltaUtils";

  public static void convertToDelta(String icebergTable, String deltaTable) {
    try {
      URI icebergURI = new URI(icebergTable);
      URI deltaURI = new URI(deltaTable);
      // Step 1: Obtain the Class object
      Class<?> clazz = Class.forName(CONVERTER_CLASS);

      // Step 2: Get the Method object
      // The method name is "convertToDelta" and it has three parameters: String, String
      Method method = clazz.getMethod("convertToDelta", String.class, String.class);

      // Step 3: Invoke the method
      // Since convertToDelta is a static method, pass null as the first argument
      method.invoke(null, icebergURI.getPath(), deltaURI.getPath());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
