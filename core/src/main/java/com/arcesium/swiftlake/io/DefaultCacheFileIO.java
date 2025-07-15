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
import com.arcesium.swiftlake.metrics.ContentCacheRuntimeMetrics;
import com.arcesium.swiftlake.metrics.FileSystemCacheRuntimeMetrics;
import com.arcesium.swiftlake.metrics.MetricCollector;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DefaultCacheFileIO implements the CacheFileIO interface to provide caching functionality for file
 * I/O operations in SwiftLake.
 */
public class DefaultCacheFileIO implements CacheFileIO {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCacheFileIO.class);
  private ContentCache contentCache;
  private FileSystemCache fileSystemCache;

  public DefaultCacheFileIO() {}

  /**
   * Constructs a DefaultCacheFileIO with the specified content and local file caches.
   *
   * @param contentCache The ContentCache instance for caching file contents
   * @param fileSystemCache The FileSystemCache instance for caching files
   */
  public DefaultCacheFileIO(ContentCache contentCache, FileSystemCache fileSystemCache) {
    this.contentCache = contentCache;
    this.fileSystemCache = fileSystemCache;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    SwiftLakeFileIOProperties fileIOProperties = new SwiftLakeFileIOProperties(properties);
    MetricCollector metricCollector = fileIOProperties.getMetricCollector(properties);
    if (contentCache == null && fileIOProperties.isManifestCacheEnabled()) {
      this.contentCache =
          new ContentCache(
              fileIOProperties.getManifestCacheExpirationIntervalInSeconds(),
              fileIOProperties.getManifestCacheMaxTotalBytes(),
              fileIOProperties.getManifestCacheMaxContentLength(),
              metricCollector);
    }
    if (fileSystemCache == null && fileIOProperties.isFileSystemCacheEnabled()) {
      this.fileSystemCache =
          new FileSystemCache(
              fileIOProperties.getFileSystemCacheBasePath(),
              fileIOProperties.getFileSystemCacheExpirationIntervalInSeconds(),
              fileIOProperties.getFileSystemCacheMaxTotalBytes(),
              metricCollector);
    }
  }

  @Override
  public InputFile newInputFile(SwiftLakeFileIO fileIO, String path) {
    if (fileSystemCache != null) {
      return new FileCachingInputFile(path, fileIO, fileSystemCache);
    }
    return fileIO.newInputFile(path);
  }

  @Override
  public InputFiles newInputFiles(SwiftLakeFileIO fileIO, List<String> paths) {
    if (fileSystemCache != null) {
      return fileSystemCache.get(fileIO, paths);
    }
    return fileIO.downloadFiles(paths);
  }

  @Override
  public InputFile newInputFile(SwiftLakeFileIO fileIO, String path, long length) {
    if (contentCache != null) {
      return contentCache.get(fileIO, path, length);
    }
    return newInputFile(fileIO, path);
  }

  @Override
  public InputFile newInputFile(SwiftLakeFileIO fileIO, String path, boolean useFileSystemCache) {
    if (useFileSystemCache) {
      return newInputFile(fileIO, path);
    }
    return fileIO.newInputFile(path);
  }

  @Override
  public InputFile newInputFile(
      SwiftLakeFileIO fileIO, String path, long length, boolean useFileSystemCache) {
    if (useFileSystemCache) {
      return newInputFile(fileIO, path);
    }
    return fileIO.newInputFile(path, length);
  }

  @Override
  public FileSystemCacheRuntimeMetrics getFileSystemCacheRuntimeMetrics() {
    if (fileSystemCache == null) {
      return null;
    }
    return fileSystemCache.getRuntimeMetrics();
  }

  @Override
  public ContentCacheRuntimeMetrics getContentCacheRuntimeMetrics() {
    if (contentCache == null) {
      return null;
    }
    return contentCache.getRuntimeMetrics();
  }

  private static class FileCachingInputFile implements InputFile {
    private final FileSystemCache fileSystemCache;
    private final String path;
    private InputFiles inputFiles;
    private transient volatile File file;
    private SwiftLakeFileIO fileIO;

    private FileCachingInputFile(
        String path, SwiftLakeFileIO fileIO, FileSystemCache fileSystemCache) {
      this.path = path;
      this.fileIO = fileIO;
      this.fileSystemCache = fileSystemCache;
    }

    @Override
    public long getLength() {
      downloadFile();
      return file.length();
    }

    @Override
    public SeekableInputStream newStream() {
      try {
        downloadFile();
        return new SeekableFileInputStream(new RandomAccessFile(file, "r"), inputFiles);
      } catch (FileNotFoundException e) {
        throw new NotFoundException(e, "Failed to read file: %s - %s", path, file);
      }
    }

    private void downloadFile() {
      if (file == null) {
        synchronized (this) {
          if (file == null) {
            this.inputFiles = fileSystemCache.get(fileIO, Arrays.asList(path));
            this.file = new File(inputFiles.getInputFiles().get(0).getLocalFileLocation());
          }
        }
      }
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public boolean exists() {
      downloadFile();
      return file.exists();
    }

    @Override
    public String toString() {
      return path;
    }
  }

  private static class SeekableFileInputStream extends SeekableInputStream {
    private final RandomAccessFile stream;
    private final InputFiles inputFiles;

    private SeekableFileInputStream(RandomAccessFile stream, InputFiles inputFiles) {
      this.stream = stream;
      this.inputFiles = inputFiles;
    }

    @Override
    public long getPos() throws IOException {
      return stream.getFilePointer();
    }

    @Override
    public void seek(long newPos) throws IOException {
      stream.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      return stream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
      return stream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return stream.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
      if (n > Integer.MAX_VALUE) {
        return stream.skipBytes(Integer.MAX_VALUE);
      } else {
        return stream.skipBytes((int) n);
      }
    }

    @Override
    public void close() throws IOException {
      stream.close();
      try {
        inputFiles.close();
      } catch (Throwable t) {
        LOGGER.error("An error occurred while closing resource.", t);
      }
    }
  }
}
