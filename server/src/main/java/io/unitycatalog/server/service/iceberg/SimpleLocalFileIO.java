package io.unitycatalog.server.service.iceberg;

import java.net.URI;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class SimpleLocalFileIO implements FileIO {
  @Override
  public InputFile newInputFile(String path) {
    URI uri = URI.create(path);
    return Files.localInput(uri.getPath());
  }

  @Override
  public OutputFile newOutputFile(String path) {
    URI uri = URI.create(path);
    return Files.localOutput(uri.getPath());
  }

  @Override
  public void deleteFile(String path) {
    throw new UnsupportedOperationException();
  }
}
