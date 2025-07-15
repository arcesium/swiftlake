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

/** Represents runtime metrics for DuckDB operations. */
public class DuckDBRuntimeMetrics {
  private final long memoryLimitInBytes;
  private final long memoryUsageInBytes;
  private final long tempStorageLimitInBytes;
  private final long tempStorageUsageInBytes;

  /**
   * Constructs a DuckDBRuntimeMetrics object with the specified metrics.
   *
   * @param memoryLimitInBytes The memory limit in bytes.
   * @param memoryUsageInBytes The current memory usage in bytes.
   * @param tempStorageLimitInBytes The temporary storage limit in bytes.
   * @param tempStorageUsageInBytes The current temporary storage usage in bytes.
   */
  public DuckDBRuntimeMetrics(
      long memoryLimitInBytes,
      long memoryUsageInBytes,
      long tempStorageLimitInBytes,
      long tempStorageUsageInBytes) {
    this.memoryLimitInBytes = memoryLimitInBytes;
    this.memoryUsageInBytes = memoryUsageInBytes;
    this.tempStorageLimitInBytes = tempStorageLimitInBytes;
    this.tempStorageUsageInBytes = tempStorageUsageInBytes;
  }

  /**
   * Gets the memory limit in bytes.
   *
   * @return The memory limit in bytes.
   */
  public long getMemoryLimitInBytes() {
    return memoryLimitInBytes;
  }

  /**
   * Gets the current memory usage in bytes.
   *
   * @return The current memory usage in bytes.
   */
  public long getMemoryUsageInBytes() {
    return memoryUsageInBytes;
  }

  /**
   * Gets the temporary storage limit in bytes.
   *
   * @return The temporary storage limit in bytes.
   */
  public long getTempStorageLimitInBytes() {
    return tempStorageLimitInBytes;
  }

  /**
   * Gets the current temporary storage usage in bytes.
   *
   * @return The current temporary storage usage in bytes.
   */
  public long getTempStorageUsageInBytes() {
    return tempStorageUsageInBytes;
  }

  /**
   * Returns a string representation of the DuckDBRuntimeMetrics object.
   *
   * @return A string representation of the object.
   */
  @Override
  public String toString() {
    return "DuckDBRuntimeMetrics{"
        + "memoryLimitInBytes="
        + memoryLimitInBytes
        + ", memoryUsageInBytes="
        + memoryUsageInBytes
        + ", tempStorageLimitInBytes="
        + tempStorageLimitInBytes
        + ", tempStorageUsageInBytes="
        + tempStorageUsageInBytes
        + '}';
  }
}
