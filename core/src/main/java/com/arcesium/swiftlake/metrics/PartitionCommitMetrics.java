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

/** Represents metrics for partition commit operations. */
public class PartitionCommitMetrics {
  private final PartitionData partitionData;
  private final int addedFilesCount;
  private final int removedFilesCount;
  private final long addedRecordsCount;
  private final long removedRecordsCount;

  /**
   * Constructs a PartitionCommitMetrics object with the specified metrics.
   *
   * @param partitionData The partition data associated with these metrics.
   * @param addedFilesCount The number of files added in the commit.
   * @param removedFilesCount The number of files removed in the commit.
   * @param addedRecordsCount The number of records added in the commit.
   * @param removedRecordsCount The number of records removed in the commit.
   */
  public PartitionCommitMetrics(
      PartitionData partitionData,
      int addedFilesCount,
      int removedFilesCount,
      long addedRecordsCount,
      long removedRecordsCount) {
    this.partitionData = partitionData;
    this.addedFilesCount = addedFilesCount;
    this.removedFilesCount = removedFilesCount;
    this.addedRecordsCount = addedRecordsCount;
    this.removedRecordsCount = removedRecordsCount;
  }

  /**
   * Gets the partition data associated with these metrics.
   *
   * @return The partition data.
   */
  public PartitionData getPartitionData() {
    return partitionData;
  }

  /**
   * Gets the number of files added in the commit.
   *
   * @return The count of added files.
   */
  public int getAddedFilesCount() {
    return addedFilesCount;
  }

  /**
   * Gets the number of files removed in the commit.
   *
   * @return The count of removed files.
   */
  public int getRemovedFilesCount() {
    return removedFilesCount;
  }

  /**
   * Gets the number of records added in the commit.
   *
   * @return The count of added records.
   */
  public long getAddedRecordsCount() {
    return addedRecordsCount;
  }

  /**
   * Gets the number of records removed in the commit.
   *
   * @return The count of removed records.
   */
  public long getRemovedRecordsCount() {
    return removedRecordsCount;
  }

  /**
   * Returns a string representation of the PartitionCommitMetrics object.
   *
   * @return A string representation of the object.
   */
  @Override
  public String toString() {
    return "PartitionCommitMetrics{"
        + "addedFilesCount="
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
