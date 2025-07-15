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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.PartitionField;
import org.junit.jupiter.api.Test;

class PartitionDataTest {

  @Test
  void testConstructorAndGetters() {
    PartitionField field1 = mock(PartitionField.class);
    PartitionField field2 = mock(PartitionField.class);
    List<Pair<PartitionField, Object>> partitionValues =
        Arrays.asList(Pair.of(field1, "value1"), Pair.of(field2, 42));

    PartitionData partitionData = new PartitionData(1, partitionValues);

    assertThat(partitionData.getSpecId()).isEqualTo(1);
    assertThat(partitionData.getPartitionValues()).isEqualTo(partitionValues);
  }

  @Test
  void testEmptyPartitionValues() {
    List<Pair<PartitionField, Object>> partitionValues = Collections.emptyList();
    PartitionData partitionData = new PartitionData(1, partitionValues);

    assertThat(partitionData.getPartitionValues()).isEmpty();
  }
}
