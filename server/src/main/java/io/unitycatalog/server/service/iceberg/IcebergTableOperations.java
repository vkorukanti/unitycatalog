package io.unitycatalog.server.service.iceberg;

import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.requests.CreateTableRequest;

public class IcebergTableOperations {
  public static TableMetadata createMetadataForNewTable(
      CreateTableRequest request, String tableId, String location) {
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            request.schema(),
            request.spec() == null ? PartitionSpec.unpartitioned() : request.spec(),
            request.writeOrder() == null ? SortOrder.unsorted() : request.writeOrder(),
            location,
            java.util.Collections.emptyMap());
    return TableMetadata.buildFrom(metadata)
        .assignUUID(tableId.toString())
        .setLocation(location)
        .discardChanges()
        .build();
  }

  public static String writeMetadataFileWithFileIO(
      FileIO io, TableMetadata newTableMetadata, long newMetadataVersion) {
    String newMetadataLocation =
        generateNewMetadataLocation(newTableMetadata.location(), newMetadataVersion);
    // Use overwrite to avoid negative caching in S3 (this is safe because the metadata location is
    // always unique because it includes a UUID).
    TableMetadataParser.overwrite(newTableMetadata, io.newOutputFile(newMetadataLocation));
    return newMetadataLocation;
  }

  private static String generateNewMetadataLocation(String tableLocation, long metadataVersion) {
    return String.format(
        "%s/metadata/%05d-%s.metadata.json",
        tableLocation, metadataVersion, UUID.randomUUID().toString());
  }
}
