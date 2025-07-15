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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.InputFile;

/** Interface for cache-aware file I/O operations in SwiftLake. */
public interface CacheFileIO {

  /**
   * Initializes the CacheFileIO with the provided properties. This default implementation does
   * nothing.
   *
   * @param properties A map containing configuration properties for initialization.
   */
  default void initialize(Map<String, String> properties) {}

  /**
   * Creates a new InputFile for the given path.
   *
   * @param fileIO The SwiftLakeFileIO instance to use for file operations.
   * @param path The path of the file.
   * @return A new InputFile instance.
   */
  InputFile newInputFile(SwiftLakeFileIO fileIO, String path);

  /**
   * Creates a new InputFile for the given path with a specified length.
   *
   * @param fileIO The SwiftLakeFileIO instance to use for file operations.
   * @param path The path of the file.
   * @param length The length of the file.
   * @return A new InputFile instance.
   */
  InputFile newInputFile(SwiftLakeFileIO fileIO, String path, long length);

  /**
   * Creates a new InputFile for the given path with an option to use file cache.
   *
   * @param fileIO The SwiftLakeFileIO instance to use for file operations.
   * @param path The path of the file.
   * @param useFileSystemCache Whether to use file cache or not.
   * @return A new InputFile instance.
   */
  InputFile newInputFile(SwiftLakeFileIO fileIO, String path, boolean useFileSystemCache);

  /**
   * Creates a new InputFile for the given path with a specified length and an option to use file
   * cache.
   *
   * @param fileIO The SwiftLakeFileIO instance to use for file operations.
   * @param path The path of the file.
   * @param length The length of the file.
   * @param useFileSystemCache Whether to use file cache or not.
   * @return A new InputFile instance.
   */
  InputFile newInputFile(
      SwiftLakeFileIO fileIO, String path, long length, boolean useFileSystemCache);

  /**
   * Creates new InputFiles for the given list of paths.
   *
   * @param fileIO The SwiftLakeFileIO instance to use for file operations.
   * @param paths A list of file paths.
   * @return An InputFiles instance containing multiple InputFile objects.
   */
  InputFiles newInputFiles(SwiftLakeFileIO fileIO, List<String> paths);

  /**
   * Retrieves the runtime metrics for file cache operations.
   *
   * @return FileSystemCacheRuntimeMetrics instance containing file cache metrics.
   */
  FileSystemCacheRuntimeMetrics getFileSystemCacheRuntimeMetrics();

  /**
   * Retrieves the runtime metrics for content cache operations.
   *
   * @return ContentCacheRuntimeMetrics instance containing content cache metrics.
   */
  ContentCacheRuntimeMetrics getContentCacheRuntimeMetrics();
}
