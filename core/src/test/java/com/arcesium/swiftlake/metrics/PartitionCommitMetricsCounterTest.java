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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.arcesium.swiftlake.common.ValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PartitionCommitMetricsCounterTest {

  private PartitionCommitMetricsCounter counter;
  private PartitionData mockPartitionData;

  @BeforeEach
  void setUp() {
    counter = new PartitionCommitMetricsCounter();
    mockPartitionData = mock(PartitionData.class);
  }

  @Test
  void testInitialState() {
    PartitionCommitMetrics metrics = counter.getPartitionCommitMetrics();
    assertThat(metrics.getAddedFilesCount()).isZero();
    assertThat(metrics.getRemovedFilesCount()).isZero();
    assertThat(metrics.getAddedRecordsCount()).isZero();
    assertThat(metrics.getRemovedRecordsCount()).isZero();
    assertThat(metrics.getPartitionData()).isNull();
  }

  @ParameterizedTest
  @ValueSource(longs = {1, 10, 100, Integer.MAX_VALUE})
  void testIncreaseAddedFilesCount(long count) {
    counter.increaseAddedFilesCount(count);
    PartitionCommitMetrics metrics = counter.getPartitionCommitMetrics();
    assertThat(metrics.getAddedFilesCount()).isEqualTo((int) count);
  }

  @ParameterizedTest
  @ValueSource(longs = {1, 10, 100, Integer.MAX_VALUE})
  void testIncreaseRemovedFilesCount(long count) {
    counter.increaseRemovedFilesCount(count);
    PartitionCommitMetrics metrics = counter.getPartitionCommitMetrics();
    assertThat(metrics.getRemovedFilesCount()).isEqualTo((int) count);
  }

  @ParameterizedTest
  @ValueSource(longs = {1, 10, 100, Long.MAX_VALUE})
  void testIncreaseAddedRecordsCount(long count) {
    counter.increaseAddedRecordsCount(count);
    PartitionCommitMetrics metrics = counter.getPartitionCommitMetrics();
    assertThat(metrics.getAddedRecordsCount()).isEqualTo(count);
  }

  @ParameterizedTest
  @ValueSource(longs = {1, 10, 100, Long.MAX_VALUE})
  void testIncreaseRemovedRecordsCount(long count) {
    counter.increaseRemovedRecordsCount(count);
    PartitionCommitMetrics metrics = counter.getPartitionCommitMetrics();
    assertThat(metrics.getRemovedRecordsCount()).isEqualTo(count);
  }

  @Test
  void testMultipleIncrements() {
    counter.increaseAddedFilesCount(5);
    counter.increaseAddedFilesCount(3);
    counter.increaseRemovedFilesCount(2);
    counter.increaseAddedRecordsCount(1000);
    counter.increaseAddedRecordsCount(500);
    counter.increaseRemovedRecordsCount(200);

    PartitionCommitMetrics metrics = counter.getPartitionCommitMetrics();
    assertThat(metrics.getAddedFilesCount()).isEqualTo(8);
    assertThat(metrics.getRemovedFilesCount()).isEqualTo(2);
    assertThat(metrics.getAddedRecordsCount()).isEqualTo(1500);
    assertThat(metrics.getRemovedRecordsCount()).isEqualTo(200);
  }

  @Test
  void testSetAndGetPartitionData() {
    counter.setPartitionData(mockPartitionData);
    assertThat(counter.getPartitionData()).isEqualTo(mockPartitionData);

    PartitionCommitMetrics metrics = counter.getPartitionCommitMetrics();
    assertThat(metrics.getPartitionData()).isEqualTo(mockPartitionData);
  }

  @Test
  void testThreadSafety() throws InterruptedException {
    int threadCount = 10;
    long incrementsPerThread = 10000;

    Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      threads[i] =
          new Thread(
              () -> {
                for (long j = 0; j < incrementsPerThread; j++) {
                  counter.increaseAddedFilesCount(1);
                  counter.increaseRemovedFilesCount(1);
                  counter.increaseAddedRecordsCount(1);
                  counter.increaseRemovedRecordsCount(1);
                }
              });
      threads[i].start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    PartitionCommitMetrics metrics = counter.getPartitionCommitMetrics();
    long expectedCount = threadCount * incrementsPerThread;
    assertThat(metrics.getAddedFilesCount())
        .isEqualTo((int) Math.min(expectedCount, Integer.MAX_VALUE));
    assertThat(metrics.getRemovedFilesCount())
        .isEqualTo((int) Math.min(expectedCount, Integer.MAX_VALUE));
    assertThat(metrics.getAddedRecordsCount()).isEqualTo(expectedCount);
    assertThat(metrics.getRemovedRecordsCount()).isEqualTo(expectedCount);
  }

  @Test
  void testNegativeIncrements() {
    assertThatThrownBy(() -> counter.increaseAddedFilesCount(-1))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Added files count must be non-negative, but was: -1");

    assertThatThrownBy(() -> counter.increaseRemovedFilesCount(-1))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Removed files count must be non-negative, but was: -1");

    assertThatThrownBy(() -> counter.increaseAddedRecordsCount(-1))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Added records count must be non-negative, but was: -1");

    assertThatThrownBy(() -> counter.increaseRemovedRecordsCount(-1))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Removed records count must be non-negative, but was: -1");

    PartitionCommitMetrics metrics = counter.getPartitionCommitMetrics();
    assertThat(metrics.getAddedFilesCount()).isZero();
    assertThat(metrics.getRemovedFilesCount()).isZero();
    assertThat(metrics.getAddedRecordsCount()).isZero();
    assertThat(metrics.getRemovedRecordsCount()).isZero();
  }
}
