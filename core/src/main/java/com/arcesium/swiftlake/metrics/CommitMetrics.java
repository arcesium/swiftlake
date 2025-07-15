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
import java.util.List;

/** Represents metrics for a commit operation on a table. */
public class CommitMetrics implements Metrics {
  private final String tableName;
  private final List<PartitionCommitMetrics> partitionCommitMetrics;
  private final Duration totalDuration;
  private final int addedFilesCount;
  private final int removedFilesCount;
  private final long addedRecordsCount;
  private final long removedRecordsCount;

  /**
   * Constructs a CommitMetrics object with only the table name.
   *
   * @param tableName The name of the table.
   */
  public CommitMetrics(String tableName) {
    this.tableName = tableName;
    this.partitionCommitMetrics = null;
    this.totalDuration = Duration.ZERO;
    this.addedFilesCount = 0;
    this.removedFilesCount = 0;
    this.addedRecordsCount = 0;
    this.removedRecordsCount = 0;
  }

  /**
   * Constructs a CommitMetrics object with all metrics.
   *
   * @param tableName The name of the table.
   * @param partitionCommitMetrics List of partition commit metrics.
   * @param totalDuration Total duration of the commit operation.
   * @param addedFilesCount Number of files added.
   * @param removedFilesCount Number of files removed.
   * @param addedRecordsCount Number of records added.
   * @param removedRecordsCount Number of records removed.
   */
  public CommitMetrics(
      String tableName,
      List<PartitionCommitMetrics> partitionCommitMetrics,
      Duration totalDuration,
      int addedFilesCount,
      int removedFilesCount,
      long addedRecordsCount,
      long removedRecordsCount) {
    this.tableName = tableName;
    this.partitionCommitMetrics = partitionCommitMetrics;
    this.totalDuration = totalDuration;
    this.addedFilesCount = addedFilesCount;
    this.removedFilesCount = removedFilesCount;
    this.addedRecordsCount = addedRecordsCount;
    this.removedRecordsCount = removedRecordsCount;
  }

  /**
   * Gets the table name.
   *
   * @return The table name.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Gets the list of partition commit metrics.
   *
   * @return The list of partition commit metrics.
   */
  public List<PartitionCommitMetrics> getPartitionCommitMetrics() {
    return partitionCommitMetrics;
  }

  /**
   * Gets the total duration of the commit operation.
   *
   * @return The total duration.
   */
  public Duration getTotalDuration() {
    return totalDuration;
  }

  /**
   * Gets the count of added files.
   *
   * @return The count of added files.
   */
  public int getAddedFilesCount() {
    return addedFilesCount;
  }

  /**
   * Gets the count of removed files.
   *
   * @return The count of removed files.
   */
  public int getRemovedFilesCount() {
    return removedFilesCount;
  }

  /**
   * Gets the count of added records.
   *
   * @return The count of added records.
   */
  public long getAddedRecordsCount() {
    return addedRecordsCount;
  }

  /**
   * Gets the count of removed records.
   *
   * @return The count of removed records.
   */
  public long getRemovedRecordsCount() {
    return removedRecordsCount;
  }

  /**
   * Returns a string representation of the CommitMetrics object.
   *
   * @return A string representation of the object.
   */
  @Override
  public String toString() {
    return "CommitMetrics{"
        + "tableName='"
        + tableName
        + '\''
        + ", partitionCommitMetrics="
        + partitionCommitMetrics
        + ", totalDuration="
        + totalDuration
        + ", addedFilesCount="
        + addedFilesCount
        + ", removedFilesCount="
        + removedFilesCount
        + ", addedRecordsCount="
        + addedRecordsCount
        + ", removedRecordsCount="
        + removedRecordsCount
        + '}';
  }
}
