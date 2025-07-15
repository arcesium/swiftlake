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
package com.arcesium.swiftlake.io;

import com.arcesium.swiftlake.common.InputFile;

/**
 * DefaultInputFile is a basic implementation of the InputFile interface. It represents a file with
 * its length, local file location, and remote location.
 */
public class DefaultInputFile implements InputFile {
  private final long length;
  private final String localFileLocation;
  private final String location;

  /**
   * Constructs a new DefaultInputFile with the specified parameters.
   *
   * @param localFileLocation The local file system path of the file
   * @param location The remote location or identifier of the file
   * @param length The length of the file in bytes
   */
  public DefaultInputFile(String localFileLocation, String location, long length) {
    this.length = length;
    this.localFileLocation = localFileLocation;
    this.location = location;
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public String getLocalFileLocation() {
    return localFileLocation;
  }

  @Override
  public String getLocation() {
    return location;
  }
}
