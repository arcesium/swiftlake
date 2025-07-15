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
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.PartitionField;
import org.junit.jupiter.api.Test;

class CommitMetricsTest {

  @Test
  void testConstructorWithOnlyTableName() {
    CommitMetrics metrics = new CommitMetrics("testTable");

    assertThat(metrics).isInstanceOf(Metrics.class);
    assertThat(metrics.getTableName()).isEqualTo("testTable");
    assertThat(metrics.getPartitionCommitMetrics()).isNull();
    assertThat(metrics.getTotalDuration()).isEqualTo(Duration.ZERO);
    assertThat(metrics.getAddedFilesCount()).isZero();
    assertThat(metrics.getRemovedFilesCount()).isZero();
    assertThat(metrics.getAddedRecordsCount()).isZero();
    assertThat(metrics.getRemovedRecordsCount()).isZero();
  }

  @Test
  void testConstructorWithAllParameters() {
    PartitionField mockField1 = mock(PartitionField.class);
    PartitionField mockField2 = mock(PartitionField.class);

    PartitionData partitionData1 =
        new PartitionData(1, Arrays.asList(Pair.of(mockField1, (Object) "value1")));
    PartitionData partitionData2 =
        new PartitionData(2, Arrays.asList(Pair.of(mockField2, (Object) "value2")));

    List<PartitionCommitMetrics> partitionMetrics =
        Arrays.asList(
            new PartitionCommitMetrics(partitionData1, 1, 2, 100L, 200L),
            new PartitionCommitMetrics(partitionData2, 3, 4, 300L, 400L));
    Duration duration = Duration.ofSeconds(10);

    CommitMetrics metrics =
        new CommitMetrics("testTable", partitionMetrics, duration, 4, 6, 400L, 600L);

    assertThat(metrics.getTableName()).isEqualTo("testTable");
    assertThat(metrics.getPartitionCommitMetrics()).isEqualTo(partitionMetrics);
    assertThat(metrics.getTotalDuration()).isEqualTo(duration);
    assertThat(metrics.getAddedFilesCount()).isEqualTo(4);
    assertThat(metrics.getRemovedFilesCount()).isEqualTo(6);
    assertThat(metrics.getAddedRecordsCount()).isEqualTo(400L);
    assertThat(metrics.getRemovedRecordsCount()).isEqualTo(600L);
  }

  @Test
  void testToString() {
    PartitionField mockField = mock(PartitionField.class);
    PartitionData partitionData =
        new PartitionData(1, Arrays.asList(Pair.of(mockField, (Object) "value")));
    List<PartitionCommitMetrics> partitionMetrics =
        Arrays.asList(new PartitionCommitMetrics(partitionData, 1, 2, 100L, 200L));
    CommitMetrics metrics =
        new CommitMetrics(
            "testTable", partitionMetrics, Duration.ofSeconds(30), 5, 10, 500L, 1000L);

    String expected =
        "CommitMetrics{tableName='testTable', partitionCommitMetrics="
            + partitionMetrics
            + ", totalDuration=PT30S, addedFilesCount=5, removedFilesCount=10, "
            + "addedRecordsCount=500, removedRecordsCount=1000}";

    assertThat(metrics.toString()).isEqualTo(expected);
  }
}
