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

import com.google.errorprone.annotations.FormatMethod;

/**
 * ValidationException is a custom runtime exception for validation errors. It provides methods for
 * common validation scenarios.
 */
public class ValidationException extends RuntimeException {
  /**
   * Constructs a new ValidationException with a formatted error message.
   *
   * @param message The error message format string.
   * @param args The arguments to be used for formatting the error message.
   */
  @FormatMethod
  public ValidationException(String message, Object... args) {
    super(String.format(message, args));
  }

  /**
   * Constructs a new ValidationException with the specified error message.
   *
   * @param message The error message.
   */
  public ValidationException(String message) {
    super(message);
  }

  /**
   * Checks a condition and throws a ValidationException if the condition is false.
   *
   * @param test The condition to check.
   * @param message The error message format string to use if the check fails.
   * @param args The arguments to be used for formatting the error message.
   * @throws ValidationException if the condition is false.
   */
  @FormatMethod
  public static void check(boolean test, String message, Object... args) {
    if (!test) {
      throw new ValidationException(message, args);
    }
  }

  /**
   * Checks a condition and throws a ValidationException if the condition is false.
   *
   * @param test The condition to check.
   * @param message The error message to use if the check fails.
   * @throws ValidationException if the condition is false.
   */
  public static void check(boolean test, String message) {
    if (!test) {
      throw new ValidationException(message);
    }
  }

  /**
   * Checks if a value is not null and throws a ValidationException if it is null.
   *
   * @param <T> The type of the value being checked.
   * @param value The value to check for null.
   * @param message The error message format string to use if the check fails.
   * @param args The arguments to be used for formatting the error message.
   * @throws ValidationException if the value is null.
   */
  @FormatMethod
  public static <T> void checkNotNull(T value, String message, Object... args) {
    if (value == null) {
      throw new ValidationException(message, args);
    }
  }

  /**
   * Checks if a value is not null and throws a ValidationException if it is null.
   *
   * @param <T> The type of the value being checked.
   * @param value The value to check for null.
   * @param message The error message to use if the check fails.
   * @throws ValidationException if the value is null.
   */
  public static <T> void checkNotNull(T value, String message) {
    if (value == null) {
      throw new ValidationException(message);
    }
  }
}
