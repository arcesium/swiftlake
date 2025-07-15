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

import com.arcesium.swiftlake.common.ValidationException;
import java.util.concurrent.atomic.LongAdder;

/**
 * A counter class for tracking partition commit metrics. This class uses LongAdder for thread-safe
 * counting operations.
 */
public class PartitionCommitMetricsCounter {
  private PartitionData partitionData;
  private final LongAdder addedFilesCount;
  private final LongAdder removedFilesCount;
  private final LongAdder addedRecordsCount;
  private final LongAdder removedRecordsCount;

  /** Constructs a new PartitionCommitMetricsCounter with initialized counters. */
  public PartitionCommitMetricsCounter() {
    addedFilesCount = new LongAdder();
    removedFilesCount = new LongAdder();
    addedRecordsCount = new LongAdder();
    removedRecordsCount = new LongAdder();
  }

  /**
   * Increases the count of added files.
   *
   * @param count The number of files to add to the counter.
   * @throws ValidationException if the count is negative.
   */
  public void increaseAddedFilesCount(long count) {
    validatePositiveCount(count, "Added files count");
    addedFilesCount.add(count);
  }

  /**
   * Increases the count of removed files.
   *
   * @param count The number of files to add to the counter.
   * @throws ValidationException if the count is negative.
   */
  public void increaseRemovedFilesCount(long count) {
    validatePositiveCount(count, "Removed files count");
    removedFilesCount.add(count);
  }

  /**
   * Increases the count of added records.
   *
   * @param count The number of records to add to the counter.
   * @throws ValidationException if the count is negative.
   */
  public void increaseAddedRecordsCount(long count) {
    validatePositiveCount(count, "Added records count");
    addedRecordsCount.add(count);
  }

  /**
   * Increases the count of removed records.
   *
   * @param count The number of records to add to the counter.
   * @throws ValidationException if the count is negative.
   */
  public void increaseRemovedRecordsCount(long count) {
    validatePositiveCount(count, "Removed records count");
    removedRecordsCount.add(count);
  }

  /**
   * Validates that the given count is not negative.
   *
   * @param count The count to validate.
   * @param name The name of the count for the error message.
   * @throws ValidationException if the count is negative.
   */
  private void validatePositiveCount(long count, String name) {
    if (count < 0) {
      throw new ValidationException("%s must be non-negative, but was: %d", name, count);
    }
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
   * Sets the partition data associated with these metrics.
   *
   * @param partitionData The partition data to set.
   */
  public void setPartitionData(PartitionData partitionData) {
    this.partitionData = partitionData;
  }

  /**
   * Creates and returns a PartitionCommitMetrics object based on the current counter values.
   *
   * @return A new PartitionCommitMetrics object.
   */
  public PartitionCommitMetrics getPartitionCommitMetrics() {
    return new PartitionCommitMetrics(
        partitionData,
        addedFilesCount.intValue(),
        removedFilesCount.intValue(),
        addedRecordsCount.longValue(),
        removedRecordsCount.longValue());
  }
}
