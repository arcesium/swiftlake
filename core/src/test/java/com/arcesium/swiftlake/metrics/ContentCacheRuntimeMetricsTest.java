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

class ContentCacheRuntimeMetricsTest {

  @ParameterizedTest
  @CsvSource({
    "100, 1000000, 500000",
    "0, 0, 0",
    "9223372036854775807, 9223372036854775807, 9223372036854775807",
    "-100, -1000000, -500000"
  })
  void testConstructorWithVariousInputs(
      long fileCount, long maxTotalBytes, long currentUsageInBytes) {
    ContentCacheRuntimeMetrics metrics =
        new ContentCacheRuntimeMetrics(fileCount, maxTotalBytes, currentUsageInBytes);

    assertThat(metrics.getFileCount()).isEqualTo(fileCount);
    assertThat(metrics.getMaxTotalBytes()).isEqualTo(maxTotalBytes);
    assertThat(metrics.getCurrentUsageInBytes()).isEqualTo(currentUsageInBytes);
  }

  @Test
  void testToString() {
    ContentCacheRuntimeMetrics metrics = new ContentCacheRuntimeMetrics(100, 1_000_000, 500_000);

    String expected =
        "ContentCacheRuntimeMetrics{fileCount=100, maxTotalBytes=1000000, currentUsageInBytes=500000}";
    assertThat(metrics.toString()).isEqualTo(expected);
  }
}
