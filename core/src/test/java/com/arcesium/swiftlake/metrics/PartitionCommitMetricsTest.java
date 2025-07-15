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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PartitionCommitMetricsTest {

  private PartitionData mockPartitionData;

  @BeforeEach
  void setUp() {
    mockPartitionData = mock(PartitionData.class);
  }

  @Test
  void testConstructorAndGetters() {
    PartitionCommitMetrics metrics =
        new PartitionCommitMetrics(mockPartitionData, 10, 5, 1000, 500);

    assertThat(metrics.getPartitionData()).isEqualTo(mockPartitionData);
    assertThat(metrics.getAddedFilesCount()).isEqualTo(10);
    assertThat(metrics.getRemovedFilesCount()).isEqualTo(5);
    assertThat(metrics.getAddedRecordsCount()).isEqualTo(1000);
    assertThat(metrics.getRemovedRecordsCount()).isEqualTo(500);
  }

  @Test
  void testToString() {
    PartitionCommitMetrics metrics =
        new PartitionCommitMetrics(mockPartitionData, 10, 5, 1000, 500);
    String expected =
        "PartitionCommitMetrics{addedFilesCount=10, removedFilesCount=5, addedRecordsCount=1000, removedRecordsCount=500}";
    assertThat(metrics.toString()).isEqualTo(expected);
  }
}
