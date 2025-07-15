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

import com.arcesium.swiftlake.common.DataFile;
import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.SwiftLakeException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.io.InputFile;
import org.slf4j.Logger;

/**
 * Interface for SwiftLake file operations, extending Apache Iceberg's FileIO. Provides methods for
 * file I/O operations specific to SwiftLake.
 */
public interface SwiftLakeFileIO extends org.apache.iceberg.io.FileIO {

  /**
   * Get the logger for this SwiftLakeFileIO instance.
   *
   * @return The logger instance
   */
  Logger getLogger();

  /**
   * Get the local directory path for temporary file storage.
   *
   * @return The local directory path as a String
   */
  String getLocalDir();

  /**
   * Create a new InputFile with an option to use file cache.
   *
   * @param path The path of the input file
   * @param useFileSystemCache Whether to use file system cache or not
   * @return A new InputFile instance
   */
  default InputFile newInputFile(String path, boolean useFileSystemCache) {
    return newInputFile(path);
  }

  /**
   * Create a new InputFile with specified length and an option to use file cache.
   *
   * @param path The path of the input file
   * @param length The length of the input file
   * @param useFileSystemCache Whether to use file system cache or not
   * @return A new InputFile instance
   */
  default InputFile newInputFile(String path, long length, boolean useFileSystemCache) {
    return newInputFile(path, length);
  }

  /**
   * Asynchronously download a file from the source to the destination.
   *
   * @param source The source file path
   * @param destination The destination file path
   * @return A CompletableFuture representing the asynchronous download operation
   */
  CompletableFuture<Void> downloadFileAsync(String source, Path destination);

  /**
   * Asynchronously upload a file from the source to the destination.
   *
   * @param source The source file path
   * @param destination The destination file path
   * @return A CompletableFuture representing the asynchronous upload operation
   */
  CompletableFuture<Void> uploadFileAsync(Path source, String destination);

  /**
   * Create new InputFiles for a list of file paths.
   *
   * @param paths List of file paths
   * @return InputFiles instance containing the created InputFile objects
   */
  InputFiles newInputFiles(List<String> paths);

  /**
   * Check if a file is downloadable based on its path and length.
   *
   * @param path The file path
   * @param length The file length
   * @return true if the file is downloadable, false otherwise
   */
  boolean isDownloadable(String path, long length);

  /**
   * Download multiple files from the given paths.
   *
   * @param paths List of file paths to download
   * @return InputFiles instance containing the downloaded files
   */
  default InputFiles downloadFiles(List<String> paths) {
    Instant start = Instant.now();
    List<com.arcesium.swiftlake.common.InputFile> localInputFiles =
        paths.stream()
            .map(
                path -> {
                  Path destination = Path.of(getLocalDir(), UUID.randomUUID().toString());
                  return Pair.of(Pair.of(path, destination), downloadFileAsync(path, destination));
                })
            .map(
                p -> {
                  String source = p.getLeft().getLeft();
                  try {
                    p.getRight().get();
                    return new DefaultInputFile(
                        p.getLeft().getRight().toString(),
                        source,
                        p.getLeft().getRight().toFile().length());
                  } catch (InterruptedException | ExecutionException e) {
                    throw new SwiftLakeException(
                        e, "An error occurred while downloading the file %s.", source);
                  }
                })
            .collect(Collectors.toList());
    Instant end = Instant.now();
    getLogger()
        .debug(
            "Successfully downloaded files {} in {} ms.",
            paths,
            Duration.between(start, end).toMillis());
    return new DefaultInputFiles(localInputFiles);
  }

  /**
   * Upload multiple files to their respective destinations.
   *
   * @param dataFiles List of DataFile objects containing source and destination information
   */
  default void uploadFiles(List<DataFile> dataFiles) {
    Instant start = Instant.now();
    if (dataFiles.isEmpty()) return;

    List<Pair<DataFile, CompletableFuture<Void>>> uploads =
        dataFiles.stream()
            .map(
                dataFile ->
                    Pair.of(
                        dataFile,
                        uploadFileAsync(
                            Path.of(dataFile.getSourceDataFilePath()),
                            dataFile.getDestinationDataFilePath())))
            .collect(Collectors.toList());

    uploads.forEach(
        f -> {
          try {
            f.getRight().join();
          } catch (Exception e) {
            throw new SwiftLakeException(
                e,
                "An error occurred while uploading file %s to %s",
                f.getLeft().getSourceDataFilePath(),
                f.getLeft().getDestinationDataFilePath());
          }
        });
    Instant end = Instant.now();
    getLogger()
        .debug(
            "Successfully uploaded {} files in {} ms.",
            dataFiles.size(),
            Duration.between(start, end).toMillis());
  }
}
