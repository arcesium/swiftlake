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

import java.io.IOException;
import java.util.MissingFormatArgumentException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SwiftLakeExceptionTest {

  @Test
  void testConstructorWithCauseMessageAndArgs() {
    Exception cause = new IOException("IO Error");
    SwiftLakeException exception = new SwiftLakeException(cause, "Error occurred: %s", "Test");

    assertThat(exception).hasMessage("Error occurred: Test").hasCause(cause);
  }

  @Test
  void testConstructorWithCauseAndMessage() {
    Exception cause = new RuntimeException("Runtime Error");
    SwiftLakeException exception = new SwiftLakeException(cause, "Error occurred");

    assertThat(exception).hasMessage("Error occurred").hasCause(cause);
  }

  @Test
  void testConstructorWithNullCause() {
    SwiftLakeException exception = new SwiftLakeException(null, "Error with no cause");

    assertThat(exception).hasMessage("Error with no cause").hasNoCause();
  }

  @ParameterizedTest
  @MethodSource("provideArgumentsForFormatting")
  void testMessageFormatting(String format, Object[] args, String expectedMessage) {
    SwiftLakeException exception = new SwiftLakeException(null, format, args);

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
  void testExceptionChaining() {
    Exception rootCause = new IOException("Root cause");
    Exception intermediateException = new IllegalArgumentException("Intermediate", rootCause);
    SwiftLakeException exception = new SwiftLakeException(intermediateException, "Top level");

    assertThat(exception)
        .hasMessage("Top level")
        .hasCause(intermediateException)
        .hasRootCause(rootCause);
  }

  @Test
  void testStackTracePreservation() {
    Exception cause = new RuntimeException("Cause");
    SwiftLakeException exception = new SwiftLakeException(cause, "Effect");

    assertThat(exception.getStackTrace()).isNotEmpty();
    assertThat(exception.getCause().getStackTrace()).isNotEmpty();
  }

  @Test
  void testWithMultipleArguments() {
    SwiftLakeException exception =
        new SwiftLakeException(null, "Error: %s, %d, %.2f", "String", 42, 3.14159);

    assertThat(exception).hasMessage("Error: String, 42, 3.14");
  }

  @Test
  void testWithMismatchedFormatAndArgs() {
    assertThatCode(() -> new SwiftLakeException(null, "Error: %s", 42)).doesNotThrowAnyException();
  }

  @Test
  void testWithExcessiveArgs() {
    SwiftLakeException exception = new SwiftLakeException(null, "Error: %s", "Message", "Excess");

    assertThat(exception).hasMessage("Error: Message");
  }

  @Test
  void testWithInsufficientArgs() {
    assertThatThrownBy(() -> new SwiftLakeException(null, "Error: %s and %s", "Only one"))
        .isInstanceOf(MissingFormatArgumentException.class);
  }
}
