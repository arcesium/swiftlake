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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DuckDBRuntimeMetricsTest {
  @ParameterizedTest
  @CsvSource({
    "1000, 500, 2000, 1000",
    "0, 0, 0, 0",
    "9223372036854775807, 9223372036854775807, 9223372036854775807, 9223372036854775807",
    "-1000, -500, -2000, -1000"
  })
  void testConstructorWithVariousInputs(
      long memLimit, long memUsage, long tempLimit, long tempUsage) {
    DuckDBRuntimeMetrics metrics =
        new DuckDBRuntimeMetrics(memLimit, memUsage, tempLimit, tempUsage);

    assertThat(metrics.getMemoryLimitInBytes()).isEqualTo(memLimit);
    assertThat(metrics.getMemoryUsageInBytes()).isEqualTo(memUsage);
    assertThat(metrics.getTempStorageLimitInBytes()).isEqualTo(tempLimit);
    assertThat(metrics.getTempStorageUsageInBytes()).isEqualTo(tempUsage);
  }

  @Test
  void testToString() {
    DuckDBRuntimeMetrics metrics = new DuckDBRuntimeMetrics(1000, 500, 2000, 1000);

    String expected =
        "DuckDBRuntimeMetrics{memoryLimitInBytes=1000, memoryUsageInBytes=500, tempStorageLimitInBytes=2000, tempStorageUsageInBytes=1000}";
    assertThat(metrics.toString()).isEqualTo(expected);
  }
}
