/*
 * Copyright (c) 2025, Arcesium LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arcesium.swiftlake.io;

import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.metrics.ContentCacheRuntimeMetrics;
import com.arcesium.swiftlake.metrics.FileSystemCacheRuntimeMetrics;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;

/**
 * A cached implementation of SwiftLakeFileIO that delegates operations to a wrapped SwiftLakeFileIO
 * and uses a CacheFileIO for caching purposes.
 */
public class SwiftLakeCachedFileIO implements SwiftLakeFileIO, Configurable<Object> {
  private SwiftLakeFileIO fileIO;
  private CacheFileIO cacheFileIO;
  private Object conf;

  /** Constructs a new SwiftLakeCachedFileIO. */
  public SwiftLakeCachedFileIO() {}

  /**
   * Constructs a new SwiftLakeCachedFileIO.
   *
   * @param fileIO The underlying SwiftLakeFileIO implementation
   * @param cacheFileIO The CacheFileIO implementation for caching
   */
  public SwiftLakeCachedFileIO(SwiftLakeFileIO fileIO, CacheFileIO cacheFileIO) {
    this.fileIO = fileIO;
    this.cacheFileIO = cacheFileIO;
  }

  @Override
  public void setConf(Object conf) {
    this.conf = conf;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    SwiftLakeFileIOProperties fileIOProperties = new SwiftLakeFileIOProperties(properties);
    if (fileIO == null) {
      ValidationException.checkNotNull(
          fileIOProperties.getDelegateFileIOImplClass(),
          "Provide delegate FileIO implementation class.");
      String ioImpl = fileIOProperties.getDelegateFileIOImplClass();
      try {
        ValidationException.check(
            SwiftLakeFileIO.class.isAssignableFrom(Class.forName(ioImpl)),
            "The FileIO implementation class must implement the SwiftLakeFileIO interface.");
        this.fileIO = (SwiftLakeFileIO) CatalogUtil.loadFileIO(ioImpl, properties, conf);
      } catch (ClassNotFoundException e) {
        throw new SwiftLakeException(e, "An error occurred while creating FileIO instance.");
      }
    }
    fileIO.initialize(properties);

    if (cacheFileIO == null) {
      this.cacheFileIO = fileIOProperties.getCacheFileIO(properties);
    }
  }

  @Override
  public CompletableFuture<Void> downloadFileAsync(String source, Path destination) {
    return fileIO.downloadFileAsync(source, destination);
  }

  @Override
  public CompletableFuture<Void> uploadFileAsync(Path source, String destination) {
    return fileIO.uploadFileAsync(source, destination);
  }

  @Override
  public InputFile newInputFile(String path) {
    if (cacheFileIO == null) {
      return fileIO.newInputFile(path);
    }
    return cacheFileIO.newInputFile(fileIO, path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    if (cacheFileIO == null) {
      return fileIO.newInputFile(path, length);
    }
    return cacheFileIO.newInputFile(fileIO, path, length);
  }

  @Override
  public Logger getLogger() {
    return fileIO.getLogger();
  }

  @Override
  public String getLocalDir() {
    return fileIO.getLocalDir();
  }

  @Override
  public InputFile newInputFile(String path, boolean useFileSystemCache) {
    if (cacheFileIO == null) {
      return fileIO.newInputFile(path);
    }
    return cacheFileIO.newInputFile(fileIO, path, useFileSystemCache);
  }

  @Override
  public InputFile newInputFile(String path, long length, boolean useFileSystemCache) {
    if (cacheFileIO == null) {
      return fileIO.newInputFile(path, length);
    }
    return cacheFileIO.newInputFile(fileIO, path, length, useFileSystemCache);
  }

  @Override
  public InputFiles newInputFiles(List<String> paths) {
    if (cacheFileIO == null) {
      return fileIO.newInputFiles(paths);
    }
    return cacheFileIO.newInputFiles(fileIO, paths);
  }

  @Override
  public boolean isDownloadable(String path, long length) {
    return fileIO.isDownloadable(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return fileIO.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    fileIO.deleteFile(path);
  }

  @Override
  public void deleteFile(InputFile file) {
    fileIO.deleteFile(file);
  }

  @Override
  public void deleteFile(OutputFile file) {
    fileIO.deleteFile(file);
  }

  @Override
  public Map<String, String> properties() {
    return fileIO.properties();
  }

  @Override
  public void close() {
    fileIO.close();
  }

  /**
   * Retrieves the runtime metrics of the file system cache.
   *
   * @return FileSystemCacheRuntimeMetrics if cacheFileIO is available, null otherwise
   */
  public FileSystemCacheRuntimeMetrics getFileSystemCacheRuntimeMetrics() {
    if (cacheFileIO == null) {
      return null;
    }
    return cacheFileIO.getFileSystemCacheRuntimeMetrics();
  }

  /**
   * Retrieves the runtime metrics of the content cache.
   *
   * @return ContentCacheRuntimeMetrics if cacheFileIO is available, null otherwise
   */
  public ContentCacheRuntimeMetrics getContentCacheRuntimeMetrics() {
    if (cacheFileIO == null) {
      return null;
    }
    return cacheFileIO.getContentCacheRuntimeMetrics();
  }
}
