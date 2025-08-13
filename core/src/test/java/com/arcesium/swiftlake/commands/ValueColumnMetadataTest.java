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
package com.arcesium.swiftlake.commands;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ValueColumnMetadataTest {

  @Test
  void testBasicFunctionality() {
    Double maxDeltaValue = 0.001;
    String nullReplacement = "N/A";

    ValueColumnMetadata<String> metadata =
        new ValueColumnMetadata<>(maxDeltaValue, nullReplacement);

    assertThat(metadata.getMaxDeltaValue()).isEqualTo(maxDeltaValue);
    assertThat(metadata.getNullReplacement()).isEqualTo(nullReplacement);
    assertThat(metadata.toString()).contains("maxDeltaValue=0.001", "nullReplacement=N/A");
  }

  @Test
  void testWithNullValues() {
    ValueColumnMetadata<String> metadata = new ValueColumnMetadata<>(null, null);

    assertThat(metadata.getMaxDeltaValue()).isNull();
    assertThat(metadata.getNullReplacement()).isNull();
  }
}
