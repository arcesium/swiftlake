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

import static org.assertj.core.api.Assertions.*;

import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DataColumnAccessorTest {

  @Test
  void testOf() {
    DataColumnAccessor<TestObject> accessor =
        DataColumnAccessor.of("testColumn", Types.StringType.get(), TestObject::getStringValue);

    assertThat(accessor).isNotNull();
    assertThat(accessor.getName()).isEqualTo("testColumn");
    assertThat(accessor.getType()).isEqualTo(Types.StringType.get());
    assertThat(accessor.getValueAccessor()).isInstanceOf(Function.class);
  }

  @Test
  void testGetName() {
    DataColumnAccessor<TestObject> accessor =
        DataColumnAccessor.of("testColumn", Types.StringType.get(), TestObject::getStringValue);

    assertThat(accessor.getName()).isEqualTo("testColumn");
  }

  @Test
  void testGetType() {
    DataColumnAccessor<TestObject> accessor =
        DataColumnAccessor.of("testColumn", Types.StringType.get(), TestObject::getStringValue);

    assertThat(accessor.getType()).isEqualTo(Types.StringType.get());
  }

  @Test
  void testGetValueAccessor() {
    Function<TestObject, Object> valueAccessor = TestObject::getStringValue;
    DataColumnAccessor<TestObject> accessor =
        DataColumnAccessor.of("testColumn", Types.StringType.get(), valueAccessor);

    assertThat(accessor.getValueAccessor()).isEqualTo(valueAccessor);
  }

  @ParameterizedTest
  @MethodSource("provideTestObjects")
  void testGetValue(TestObject testObject, Object expectedValue) {
    DataColumnAccessor<TestObject> stringAccessor =
        DataColumnAccessor.of("stringColumn", Types.StringType.get(), TestObject::getStringValue);

    DataColumnAccessor<TestObject> intAccessor =
        DataColumnAccessor.of("intColumn", Types.IntegerType.get(), TestObject::getIntValue);

    DataColumnAccessor<TestObject> booleanAccessor =
        DataColumnAccessor.of(
            "booleanColumn", Types.BooleanType.get(), TestObject::getBooleanValue);

    if (expectedValue instanceof String) {
      assertThat(stringAccessor.getValue(testObject)).isEqualTo(expectedValue);
    } else if (expectedValue instanceof Integer) {
      assertThat(intAccessor.getValue(testObject)).isEqualTo(expectedValue);
    } else if (expectedValue instanceof Boolean) {
      assertThat(booleanAccessor.getValue(testObject)).isEqualTo(expectedValue);
    }
  }

  private static Stream<Arguments> provideTestObjects() {
    return Stream.of(
        Arguments.of(new TestObject("test", 1, true), "test"),
        Arguments.of(new TestObject("test", 1, true), 1),
        Arguments.of(new TestObject("test", 1, true), true),
        Arguments.of(new TestObject(null, 0, false), null),
        Arguments.of(new TestObject(null, 0, false), 0),
        Arguments.of(new TestObject(null, 0, false), false));
  }

  @Test
  void testWithNullValueAccessor() {
    DataColumnAccessor<TestObject> accessor =
        DataColumnAccessor.of("testColumn", Types.StringType.get(), null);

    assertThat(accessor.getName()).isEqualTo("testColumn");
    assertThat(accessor.getType()).isEqualTo(Types.StringType.get());
    assertThat(accessor.getValueAccessor()).isNull();

    // Test that getValue throws NullPointerException when accessor is null
    TestObject testObject = new TestObject("test", 1, true);
    assertThatThrownBy(() -> accessor.getValue(testObject))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void testWithNullType() {
    DataColumnAccessor<TestObject> accessor =
        DataColumnAccessor.of("testColumn", null, TestObject::getStringValue);

    assertThat(accessor.getName()).isEqualTo("testColumn");
    assertThat(accessor.getType()).isNull();
    assertThat(accessor.getValueAccessor()).isNotNull();
  }

  @Test
  void testWithNullName() {
    DataColumnAccessor<TestObject> accessor =
        DataColumnAccessor.of(null, Types.StringType.get(), TestObject::getStringValue);

    assertThat(accessor.getName()).isNull();
    assertThat(accessor.getType()).isEqualTo(Types.StringType.get());
    assertThat(accessor.getValueAccessor()).isNotNull();
  }

  private static class TestObject {
    private final String stringValue;
    private final int intValue;
    private final boolean booleanValue;

    TestObject(String stringValue, int intValue, boolean booleanValue) {
      this.stringValue = stringValue;
      this.intValue = intValue;
      this.booleanValue = booleanValue;
    }

    public String getStringValue() {
      return stringValue;
    }

    public int getIntValue() {
      return intValue;
    }

    public boolean getBooleanValue() {
      return booleanValue;
    }
  }
}
