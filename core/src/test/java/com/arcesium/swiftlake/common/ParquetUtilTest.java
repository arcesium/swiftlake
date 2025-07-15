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
package com.arcesium.swiftlake.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import org.apache.iceberg.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

class ParquetUtilTest {

  @Test
  void testGetParquetFileReader() throws IOException {
    InputFile mockInputFile = mock(InputFile.class);

    assertThatThrownBy(() -> ParquetUtil.getParquetFileReader(mockInputFile))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void testHasNullIdsWithNullIds() {
    MessageType schema =
        MessageTypeParser.parseMessageType(
            "message test { " + "optional binary field1; " + "required int32 field2; " + "}");

    boolean result = ParquetUtil.hasNullIds(schema);

    assertThat(result).isTrue();
  }

  @Test
  void testHasNullIdsWithoutNullIds() {
    MessageType schema =
        MessageTypeParser.parseMessageType(
            "message test { "
                + "optional binary field1 = 1; "
                + "required int32 field2 = 2; "
                + "}");

    boolean result = ParquetUtil.hasNullIds(schema);

    assertThat(result).isFalse();
  }

  @Test
  void testHasNullIdsWithNestedStructures() {
    MessageType schema =
        MessageTypeParser.parseMessageType(
            "message test { "
                + "optional group struct1 = 1{ "
                + "  optional binary field1 = 2; "
                + "  required int32 field2= 3; "
                + "} "
                + "required group list1 (LIST) = 4 { "
                + "  repeated group list { "
                + "    optional int64 element; "
                + "  } "
                + "} "
                + "}");

    boolean result = ParquetUtil.hasNullIds(schema);

    assertThat(result).isTrue(); // Because the 'element' field in the list has no ID
  }
}
