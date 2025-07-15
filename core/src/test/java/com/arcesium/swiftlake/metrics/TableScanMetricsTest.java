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

import java.time.Duration;
import org.junit.jupiter.api.Test;

class TableScanMetricsTest {

  @Test
  void testConstructorAndGetters() {
    String tableName = "testTable";
    Duration totalDuration = Duration.ofSeconds(10);
    Duration scanDuration = Duration.ofSeconds(5);
    Duration fileFetchDuration = Duration.ofSeconds(3);
    Duration sqlPreparationDuration = Duration.ofSeconds(2);
    long matchedFileCount = 100;
    long matchedFilesSizeInBytes = 1024 * 1024;

    TableScanMetrics metrics =
        new TableScanMetrics(
            tableName,
            totalDuration,
            scanDuration,
            fileFetchDuration,
            sqlPreparationDuration,
            matchedFileCount,
            matchedFilesSizeInBytes);

    assertThat(metrics).isInstanceOf(Metrics.class);
    assertThat(metrics.getTableName()).isEqualTo(tableName);
    assertThat(metrics.getTotalDuration()).isEqualTo(totalDuration);
    assertThat(metrics.getScanDuration()).isEqualTo(scanDuration);
    assertThat(metrics.getFileFetchDuration()).isEqualTo(fileFetchDuration);
    assertThat(metrics.getSqlPreperationDuration()).isEqualTo(sqlPreparationDuration);
    assertThat(metrics.getMatchedFileCount()).isEqualTo(matchedFileCount);
    assertThat(metrics.getMatchedFilesSizeInBytes()).isEqualTo(matchedFilesSizeInBytes);
  }

  @Test
  void testToString() {
    TableScanMetrics metrics =
        new TableScanMetrics(
            "testTable",
            Duration.ofSeconds(10),
            Duration.ofSeconds(5),
            Duration.ofSeconds(3),
            Duration.ofSeconds(2),
            100,
            1024 * 1024);

    String expectedString =
        "TableScanMetrics{tableName='testTable', totalDuration=PT10S, "
            + "scanDuration=PT5S, fileFetchDuration=PT3S, sqlPreperationDuration=PT2S, "
            + "matchedFileCount=100, matchedFilesSizeInBytes=1048576}";

    assertThat(metrics.toString()).isEqualTo(expectedString);
  }
}
