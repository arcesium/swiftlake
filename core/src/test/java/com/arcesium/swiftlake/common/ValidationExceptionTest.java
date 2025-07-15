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

import java.util.MissingFormatArgumentException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ValidationExceptionTest {

  @Test
  void testConstructorWithMessageAndArgs() {
    ValidationException exception = new ValidationException("Error: %s", "Test");

    assertThat(exception).hasMessage("Error: Test");
  }

  @Test
  void testConstructorWithMessageOnly() {
    ValidationException exception = new ValidationException("Simple error");

    assertThat(exception).hasMessage("Simple error");
  }

  @ParameterizedTest
  @MethodSource("provideArgumentsForFormatting")
  void testMessageFormatting(String format, Object[] args, String expectedMessage) {
    ValidationException exception = new ValidationException(format, args);

    assertThat(exception).hasMessage(expectedMessage);
  }

  private static Stream<Arguments> provideArgumentsForFormatting() {
    return Stream.of(
        Arguments.of("Error: %s", new Object[] {"Simple"}, "Error: Simple"),
        Arguments.of("Error: %d", new Object[] {42}, "Error: 42"),
        Arguments.of("Error: %s, %d", new Object[] {"Complex", 100}, "Error: Complex, 100"),
        Arguments.of("No args", new Object[] {}, "No args"));
  }

  @Test
  void testCheckWithTrueCondition() {
    assertThatCode(() -> ValidationException.check(true, "This should not throw"))
        .doesNotThrowAnyException();
  }

  @Test
  void testCheckWithFalseCondition() {
    assertThatThrownBy(() -> ValidationException.check(false, "Error occurred"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Error occurred");
  }

  @Test
  void testCheckWithFalseConditionAndFormatting() {
    assertThatThrownBy(() -> ValidationException.check(false, "Error: %s", "Test"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Error: Test");
  }

  @Test
  void testCheckNotNullWithNonNullValue() {
    assertThatCode(() -> ValidationException.checkNotNull("Not null", "This should not throw"))
        .doesNotThrowAnyException();
  }

  @Test
  void testCheckNotNullWithNullValue() {
    assertThatThrownBy(() -> ValidationException.checkNotNull(null, "Value is null"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Value is null");
  }

  @Test
  void testCheckNotNullWithNullValueAndFormatting() {
    assertThatThrownBy(() -> ValidationException.checkNotNull(null, "Error: %s", "Null found"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Error: Null found");
  }

  @Test
  void testExceptionInheritance() {
    ValidationException exception = new ValidationException("Test");
    assertThat(exception).isInstanceOf(RuntimeException.class);
  }

  @Test
  void testWithMultipleArguments() {
    ValidationException exception =
        new ValidationException("Error: %s, %d, %.2f", "String", 42, 3.14159);

    assertThat(exception).hasMessage("Error: String, 42, 3.14");
  }

  @Test
  void testWithMismatchedFormatAndArgs() {
    assertThatCode(() -> new ValidationException("Error: %s", 42)).doesNotThrowAnyException();
  }

  @Test
  void testWithExcessiveArgs() {
    ValidationException exception = new ValidationException("Error: %s", "Message", "Excess");

    assertThat(exception).hasMessage("Error: Message");
  }

  @Test
  void testWithInsufficientArgs() {
    assertThatThrownBy(() -> new ValidationException("Error: %s and %s", "Only one"))
        .isInstanceOf(MissingFormatArgumentException.class);
  }

  @Test
  void testCheckWithComplexCondition() {
    int value = 5;
    assertThatCode(() -> ValidationException.check(value > 0 && value < 10, "Value out of range"))
        .doesNotThrowAnyException();

    assertThatThrownBy(() -> ValidationException.check(value < 0 || value > 10, "Value in range"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Value in range");
  }

  @Test
  void testCheckNotNullWithCustomObject() {
    class CustomObject {}
    CustomObject obj = new CustomObject();

    assertThatCode(() -> ValidationException.checkNotNull(obj, "Object is null"))
        .doesNotThrowAnyException();

    assertThatThrownBy(() -> ValidationException.checkNotNull(null, "Object is null"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Object is null");
  }
}
