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

/**
 * The InputFile interface represents a file input for processing. It provides methods to retrieve
 * information about the file.
 */
public interface InputFile {
  /**
   * Gets the length of the file in bytes.
   *
   * @return The length of the file in bytes.
   */
  long getLength();

  /**
   * Gets the local file system location of the file.
   *
   * @return The path to the file on the local file system.
   */
  String getLocalFileLocation();

  /**
   * Gets the original location of the file. This could be a remote URL or path.
   *
   * @return The original location of the file.
   */
  String getLocation();
}
