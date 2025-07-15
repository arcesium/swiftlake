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
package com.arcesium.swiftlake.metrics;

/** Represents runtime metrics for a file system cache. */
public class FileSystemCacheRuntimeMetrics {
  private final long fileCount;
  private final long storageLimitInBytes;
  private final long storageUsageInBytes;

  /**
   * Constructs a FileSystemCacheRuntimeMetrics object with the specified metrics.
   *
   * @param fileCount The number of files in the cache.
   * @param storageLimitInBytes The storage limit of the cache in bytes.
   * @param storageUsageInBytes The current storage usage of the cache in bytes.
   */
  public FileSystemCacheRuntimeMetrics(
      long fileCount, long storageLimitInBytes, long storageUsageInBytes) {
    this.fileCount = fileCount;
    this.storageLimitInBytes = storageLimitInBytes;
    this.storageUsageInBytes = storageUsageInBytes;
  }

  /**
   * Gets the number of files in the cache.
   *
   * @return The number of files in the cache.
   */
  public long getFileCount() {
    return fileCount;
  }

  /**
   * Gets the storage limit of the cache in bytes.
   *
   * @return The storage limit in bytes.
   */
  public long getStorageLimitInBytes() {
    return storageLimitInBytes;
  }

  /**
   * Gets the current storage usage of the cache in bytes.
   *
   * @return The current storage usage in bytes.
   */
  public long getStorageUsageInBytes() {
    return storageUsageInBytes;
  }

  /**
   * Returns a string representation of the FileSystemCacheRuntimeMetrics object.
   *
   * @return A string representation of the object.
   */
  @Override
  public String toString() {
    return "FileSystemCacheRuntimeMetrics{"
        + "fileCount="
        + fileCount
        + ", storageLimitInBytes="
        + storageLimitInBytes
        + ", storageUsageInBytes="
        + storageUsageInBytes
        + '}';
  }
}
