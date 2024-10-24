package io.unitycatalog.server.service.iceberg;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;

public class MetadataService {

  private final FileIOFactory fileIOFactory;

  public MetadataService(FileIOFactory fileIOFactory) {
    this.fileIOFactory = fileIOFactory;
  }

  public TableMetadata readTableMetadata(String metadataLocation) {
    URI metadataLocationUri = URI.create(metadataLocation);
    // TODO: cache fileIO
    FileIO fileIO = fileIOFactory.getFileIO(metadataLocationUri);

    return CompletableFuture.supplyAsync(() -> TableMetadataParser.read(fileIO, metadataLocation))
        .join();
  }

  public String writeTableMetadata(
      TableMetadata tableMetadata, String location, long newMetadataVersion) {
    URI locationUri = URI.create(location);
    return IcebergTableOperations.writeMetadataFileWithFileIO(
        fileIOFactory.getFileIO(locationUri), tableMetadata, newMetadataVersion);
  }
}
