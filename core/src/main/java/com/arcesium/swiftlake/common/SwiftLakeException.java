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
 * SwiftLakeException is a custom runtime exception for SwiftLake operations. It extends
 * RuntimeException to allow for unchecked exception handling.
 */
public class SwiftLakeException extends RuntimeException {
  /**
   * Constructs a new SwiftLakeException with the specified cause, error message, and message
   * arguments.
   *
   * @param cause The cause of the exception (can be null).
   * @param message The error message format string.
   * @param args The arguments to be used for formatting the error message.
   */
  @FormatMethod
  public SwiftLakeException(Throwable cause, String message, Object... args) {
    super(String.format(message, args), cause);
  }

  /**
   * Constructs a new SwiftLakeException with the specified cause and detail message.
   *
   * @param cause the cause of this exception (which is saved for later retrieval by the {@link
   *     #getCause()} method)
   * @param message the detail message (which is saved for later retrieval by the {@link
   *     #getMessage()} method)
   */
  public SwiftLakeException(Throwable cause, String message) {
    super(message, cause);
  }
}
