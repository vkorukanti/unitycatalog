package io.unitycatalog.server.service.iceberg;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SetLocation;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.util.Pair;

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

  /**
   * Builds Iceberg TableMetadata using the list of metadata updates and applying them either from
   * scratch if baseMetadataOpt is empty (this is for CTAS) otherwise from the existing metadata.
   *
   * @param baseMetadataOpt Optional base metadata field (empty means build from empty for CTAS op.)
   * @param updates The list of MetadataUpdates to apply to the metadata to build new
   * @return TableMetadata built from the provided updates and baseMetadata (if provided)
   */
  public static TableMetadata buildMetadata(
      Optional<TableMetadata> baseMetadataOpt, List<MetadataUpdate> updates) {

    TableMetadata.Builder metadataBuilder;

    if (baseMetadataOpt.isPresent()) {
      metadataBuilder = TableMetadata.buildFrom(baseMetadataOpt.get());
    } else {
      metadataBuilder = TableMetadata.buildFromEmpty();
    }

    // We don't support changing the table location on update.
    try {
      for (MetadataUpdate update : updates) {
        if (!baseMetadataOpt.isPresent() || !(update instanceof SetLocation)) {
          update.applyTo(metadataBuilder);
        }
      }
    } catch (ValidationException e) {
      throw new CommitFailedException(e, "Unable to build metadata due to " + e.getMessage());
    }

    return metadataBuilder.build();
  }

  /**
   * Uses the FS client to write the new table metadata as a JSON string to a new Iceberg metadata
   * file to the table's storage location.
   *
   * @param ctx Handler context for the request
   * @param tableFullName Full table name that we are writing new metadata file for. This is
   *     necessary for FS client call.
   * @param tableLocation Table storage location where we are writing new metadata file. This is
   *     used for getting just the Iceberg metadata path from the metadata location
   * @param newTableMetadata The new table metadata we are writing out to file
   * @param newMetadataVersion The new metadata version we use as part of the metadata location
   *     filename. This number starts at 1 for a new table and increments for each commit.
   * @return both the new metadata location as String and new table metadata as JSON String
   */
  public static Pair<String, String> writeMetadataFile(
      FileIO fileIO, TableMetadata newTableMetadata, int newMetadataVersion) {

    String newMetadataLocation =
        generateNewMetadataLocation(newTableMetadata.location(), newMetadataVersion);
    String newMetadataJson = TableMetadataParser.toJson(newTableMetadata);

    writeMetadataFileWithFileIO(fileIO, newTableMetadata, newMetadataVersion);

    return Pair.of(newMetadataLocation, newMetadataJson);
  }

  /**
   * Parse the version from table metadata file name.
   *
   * @param metadataLocation table metadata file location
   * @return version of the table metadata file in success case and throw error if the version is
   *     not parsable
   */
  public static int parseIcebergVersion(String metadataLocation) {
    int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
    int versionEnd = metadataLocation.indexOf('-', versionStart);

    IllegalStateException ex =
        new IllegalStateException(
            "Invalid metadata file name: "
                + metadataLocation
                + ". Expected format: <version>-<uuid>.metadata.json");

    if (versionEnd < 0) {
      throw ex;
    }

    try {
      return Integer.valueOf(metadataLocation.substring(versionStart, versionEnd));
    } catch (NumberFormatException e) {
      throw ex;
    }
  }

  private static String generateNewMetadataLocation(String tableLocation, long metadataVersion) {
    return String.format(
        "%s/metadata/%05d-%s.metadata.json",
        tableLocation, metadataVersion, UUID.randomUUID().toString());
  }
}
