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
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of SwiftLakeFileIO using Hadoop FileSystem for file operations. Extends
 * HadoopFileIO and implements SwiftLakeFileIO interface.
 */
public class SwiftLakeHadoopFileIO extends HadoopFileIO implements SwiftLakeFileIO {
  private static final Logger LOGGER = LoggerFactory.getLogger(SwiftLakeHadoopFileIO.class);
  public static final String PARALLELISM = "io.hadoop.num-threads";
  public static final String STAGING_DIRECTORY = "io.hadoop.staging-dir";
  private static final String EXECUTOR_POOL_NAME = "swiftlake-hadoopfileio-pool";
  private static final int DEFAULT_CORE_MULTIPLE = 4;
  private String localDir;
  private static volatile ExecutorService executorService;

  /** Constructs a new SwiftLakeHadoopFileIO instance. */
  public SwiftLakeHadoopFileIO() {}

  @Override
  public void initialize(Map<String, String> props) {
    super.initialize(props);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    this.localDir =
        conf.get(
            STAGING_DIRECTORY,
            Path.of(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString()).toString());
    new File(localDir).mkdirs();
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  @Override
  public String getLocalDir() {
    return localDir;
  }

  @Override
  public CompletableFuture<Void> downloadFileAsync(String source, Path destination) {
    ValidationException.checkNotNull(source, "source cannot be null.");
    ValidationException.checkNotNull(destination, "destination cannot be null.");
    return CompletableFuture.runAsync(() -> downloadFile(source, destination), executorService());
  }

  @Override
  public CompletableFuture<Void> uploadFileAsync(Path source, String destination) {
    ValidationException.checkNotNull(source, "source cannot be null.");
    ValidationException.checkNotNull(destination, "destination cannot be null.");
    return CompletableFuture.runAsync(() -> uploadFile(source, destination), executorService());
  }

  @Override
  public InputFiles newInputFiles(List<String> paths) {
    return downloadFiles(paths);
  }

  @Override
  public boolean isDownloadable(String path, long length) {
    return true;
  }

  /**
   * Downloads a file from the source to the destination.
   *
   * @param source The source file path
   * @param destination The destination file path
   * @throws SwiftLakeException if an error occurs during the file copy
   */
  private void downloadFile(String source, Path destination) {
    org.apache.hadoop.fs.Path sourcePath = new org.apache.hadoop.fs.Path(source);
    FileSystem fs = Util.getFs(sourcePath, conf());
    try {
      fs.copyToLocalFile(sourcePath, new org.apache.hadoop.fs.Path(destination.toString()));
    } catch (IOException e) {
      throw new SwiftLakeException(e, "An error occurred while copying a file.");
    }
  }

  /**
   * Uploads a file from the source to the destination.
   *
   * @param source The source file path
   * @param destination The destination file path
   * @throws SwiftLakeException if an error occurs during the file copy
   */
  private void uploadFile(Path source, String destination) {
    try {
      org.apache.hadoop.fs.Path destinationPath = new org.apache.hadoop.fs.Path(destination);
      FileSystem fs = Util.getFs(destinationPath, conf());
      fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(source.toString()), destinationPath);
    } catch (IOException e) {
      throw new SwiftLakeException(e, "An error occurred while copying a file.");
    }
  }

  private ExecutorService executorService() {
    if (executorService == null) {
      synchronized (SwiftLakeHadoopFileIO.class) {
        if (executorService == null) {
          executorService = ThreadPools.newExitingWorkerPool(EXECUTOR_POOL_NAME, maxThreads());
        }
      }
    }

    return executorService;
  }

  private int maxThreads() {
    int defaultValue = Runtime.getRuntime().availableProcessors() * DEFAULT_CORE_MULTIPLE;
    return conf().getInt(PARALLELISM, defaultValue);
  }
}
