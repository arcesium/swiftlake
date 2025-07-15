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

/** Represents runtime metrics for a content cache. */
public class ContentCacheRuntimeMetrics {
  private final long fileCount;
  private final long maxTotalBytes;
  private final long currentUsageInBytes;

  /**
   * Constructs a ContentCacheRuntimeMetrics object with the specified metrics.
   *
   * @param fileCount The number of files in the cache.
   * @param maxTotalBytes The maximum total bytes allowed in the cache.
   * @param currentUsageInBytes The current usage in bytes of the cache.
   */
  public ContentCacheRuntimeMetrics(long fileCount, long maxTotalBytes, long currentUsageInBytes) {
    this.fileCount = fileCount;
    this.maxTotalBytes = maxTotalBytes;
    this.currentUsageInBytes = currentUsageInBytes;
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
   * Gets the maximum total bytes allowed in the cache.
   *
   * @return The maximum total bytes allowed in the cache.
   */
  public long getMaxTotalBytes() {
    return maxTotalBytes;
  }

  /**
   * Gets the current usage in bytes of the cache.
   *
   * @return The current usage in bytes of the cache.
   */
  public long getCurrentUsageInBytes() {
    return currentUsageInBytes;
  }

  /**
   * Returns a string representation of the ContentCacheRuntimeMetrics object.
   *
   * @return A string representation of the object.
   */
  @Override
  public String toString() {
    return "ContentCacheRuntimeMetrics{"
        + "fileCount="
        + fileCount
        + ", maxTotalBytes="
        + maxTotalBytes
        + ", currentUsageInBytes="
        + currentUsageInBytes
        + '}';
  }
}
