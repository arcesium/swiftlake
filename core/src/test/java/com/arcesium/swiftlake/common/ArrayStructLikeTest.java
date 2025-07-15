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
import static org.mockito.Mockito.*;

import java.util.stream.Stream;
import org.apache.iceberg.StructLike;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ArrayStructLikeTest {

  @Test
  void testConstructorWithArray() {
    Object[] values = {1, "test", 3.14};
    ArrayStructLike structLike = new ArrayStructLike(values);

    assertThat(structLike.size()).isEqualTo(3);
    assertThat(structLike.get(0, Integer.class)).isEqualTo(1);
    assertThat(structLike.get(1, String.class)).isEqualTo("test");
    assertThat(structLike.get(2, Double.class)).isEqualTo(3.14);
  }

  @Test
  void testConstructorWithStructLike() {
    StructLike mockStructLike = mock(StructLike.class);
    when(mockStructLike.size()).thenReturn(3);
    when(mockStructLike.get(0, Object.class)).thenReturn(1);
    when(mockStructLike.get(1, Object.class)).thenReturn("test");
    when(mockStructLike.get(2, Object.class)).thenReturn(3.14);

    ArrayStructLike structLike = new ArrayStructLike(mockStructLike);

    assertThat(structLike.size()).isEqualTo(3);
    assertThat(structLike.get(0, Integer.class)).isEqualTo(1);
    assertThat(structLike.get(1, String.class)).isEqualTo("test");
    assertThat(structLike.get(2, Double.class)).isEqualTo(3.14);
  }

  @Test
  void testSize() {
    Object[] values = {1, "test", 3.14, true};
    ArrayStructLike structLike = new ArrayStructLike(values);

    assertThat(structLike.size()).isEqualTo(4);
  }

  @ParameterizedTest
  @MethodSource("provideValuesForGet")
  <T> void testGet(int pos, Class<T> javaClass, T expectedValue) {
    Object[] values = {1, "test", 3.14, true};
    ArrayStructLike structLike = new ArrayStructLike(values);

    assertThat(structLike.get(pos, javaClass)).isEqualTo(expectedValue);
  }

  private static Stream<Arguments> provideValuesForGet() {
    return Stream.of(
        Arguments.of(0, Integer.class, 1),
        Arguments.of(1, String.class, "test"),
        Arguments.of(2, Double.class, 3.14),
        Arguments.of(3, Boolean.class, true));
  }

  @Test
  void testSet() {
    Object[] values = {1, "test", 3.14};
    ArrayStructLike structLike = new ArrayStructLike(values);

    structLike.set(1, "updated");
    assertThat(structLike.get(1, String.class)).isEqualTo("updated");
  }

  @Test
  void testEqualsWithSameObject() {
    Object[] values = {1, "test", 3.14};
    ArrayStructLike structLike = new ArrayStructLike(values);

    assertThat(structLike).isEqualTo(structLike);
  }

  @Test
  void testEqualsWithEqualObject() {
    Object[] values1 = {1, "test", 3.14};
    Object[] values2 = {1, "test", 3.14};
    ArrayStructLike structLike1 = new ArrayStructLike(values1);
    ArrayStructLike structLike2 = new ArrayStructLike(values2);

    assertThat(structLike1).isEqualTo(structLike2);
  }

  @Test
  void testEqualsWithDifferentObject() {
    Object[] values1 = {1, "test", 3.14};
    Object[] values2 = {1, "test", 3.15};
    ArrayStructLike structLike1 = new ArrayStructLike(values1);
    ArrayStructLike structLike2 = new ArrayStructLike(values2);

    assertThat(structLike1).isNotEqualTo(structLike2);
  }

  @Test
  void testEqualsWithDifferentClass() {
    Object[] values = {1, "test", 3.14};
    ArrayStructLike structLike = new ArrayStructLike(values);

    assertThat(structLike).isNotEqualTo("Not an ArrayStructLike");
  }

  @Test
  void testHashCode() {
    Object[] values1 = {1, "test", 3.14};
    Object[] values2 = {1, "test", 3.14};
    ArrayStructLike structLike1 = new ArrayStructLike(values1);
    ArrayStructLike structLike2 = new ArrayStructLike(values2);

    assertThat(structLike1.hashCode()).isEqualTo(structLike2.hashCode());
  }

  @Test
  void testHashCodeWithDifferentValues() {
    Object[] values1 = {1, "test", 3.14};
    Object[] values2 = {1, "test", 3.15};
    ArrayStructLike structLike1 = new ArrayStructLike(values1);
    ArrayStructLike structLike2 = new ArrayStructLike(values2);

    assertThat(structLike1.hashCode()).isNotEqualTo(structLike2.hashCode());
  }

  @Test
  void testGetWithInvalidIndex() {
    Object[] values = {1, "test", 3.14};
    ArrayStructLike structLike = new ArrayStructLike(values);

    assertThatThrownBy(() -> structLike.get(3, Object.class))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  void testSetWithInvalidIndex() {
    Object[] values = {1, "test", 3.14};
    ArrayStructLike structLike = new ArrayStructLike(values);

    assertThatThrownBy(() -> structLike.set(3, "invalid"))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  void testGetWithInvalidType() {
    Object[] values = {1, "test", 3.14};
    ArrayStructLike structLike = new ArrayStructLike(values);

    assertThatThrownBy(() -> structLike.get(0, String.class))
        .isInstanceOf(ClassCastException.class);
  }
}
