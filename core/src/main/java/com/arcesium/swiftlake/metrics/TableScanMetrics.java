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

import java.time.Duration;

/** Represents metrics collected during a table scan operation. Implements the Metrics interface. */
public class TableScanMetrics implements Metrics {
  private final String tableName;
  private final Duration totalDuration;
  private final Duration scanDuration;
  private final Duration fileFetchDuration;
  private final Duration sqlPreperationDuration;
  private final long matchedFileCount;
  private final long matchedFilesSizeInBytes;

  /**
   * Constructs a new TableScanMetrics object.
   *
   * @param tableName The name of the scanned table.
   * @param totalDuration The total duration of the scan operation.
   * @param scanDuration The duration of the actual scan process.
   * @param fileFetchDuration The duration spent fetching files.
   * @param sqlPreperationDuration The duration spent preparing SQL statements.
   * @param matchedFileCount The number of files that matched the scan criteria.
   * @param matchedFilesSizeInBytes The total size of matched files in bytes.
   */
  public TableScanMetrics(
      String tableName,
      Duration totalDuration,
      Duration scanDuration,
      Duration fileFetchDuration,
      Duration sqlPreperationDuration,
      long matchedFileCount,
      long matchedFilesSizeInBytes) {
    this.tableName = tableName;
    this.totalDuration = totalDuration;
    this.scanDuration = scanDuration;
    this.fileFetchDuration = fileFetchDuration;
    this.sqlPreperationDuration = sqlPreperationDuration;
    this.matchedFileCount = matchedFileCount;
    this.matchedFilesSizeInBytes = matchedFilesSizeInBytes;
  }

  /**
   * Gets the name of the scanned table.
   *
   * @return The table name.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Gets the total duration of the scan operation.
   *
   * @return The total duration.
   */
  public Duration getTotalDuration() {
    return totalDuration;
  }

  /**
   * Gets the duration of the actual scan process.
   *
   * @return The scan duration.
   */
  public Duration getScanDuration() {
    return scanDuration;
  }

  /**
   * Gets the duration spent fetching files.
   *
   * @return The file fetch duration.
   */
  public Duration getFileFetchDuration() {
    return fileFetchDuration;
  }

  /**
   * Gets the duration spent preparing SQL statements.
   *
   * @return The SQL preparation duration.
   */
  public Duration getSqlPreperationDuration() {
    return sqlPreperationDuration;
  }

  /**
   * Gets the number of files that matched the scan criteria.
   *
   * @return The count of matched files.
   */
  public long getMatchedFileCount() {
    return matchedFileCount;
  }

  /**
   * Gets the total size of matched files in bytes.
   *
   * @return The total size of matched files in bytes.
   */
  public long getMatchedFilesSizeInBytes() {
    return matchedFilesSizeInBytes;
  }

  /**
   * Returns a string representation of the TableScanMetrics object.
   *
   * @return A string containing all the metrics information.
   */
  @Override
  public String toString() {
    return "TableScanMetrics{"
        + "tableName='"
        + tableName
        + '\''
        + ", totalDuration="
        + totalDuration
        + ", scanDuration="
        + scanDuration
        + ", fileFetchDuration="
        + fileFetchDuration
        + ", sqlPreperationDuration="
        + sqlPreperationDuration
        + ", matchedFileCount="
        + matchedFileCount
        + ", matchedFilesSizeInBytes="
        + matchedFilesSizeInBytes
        + '}';
  }
}
