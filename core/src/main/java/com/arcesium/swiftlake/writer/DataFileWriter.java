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
package com.arcesium.swiftlake.writer;

import com.arcesium.swiftlake.common.DataFile;
import java.util.List;

/**
 * Interface for writing data files in SwiftLake. Implementations of this interface are responsible
 * for writing data to files and returning information about the written files.
 */
public interface DataFileWriter {
  /**
   * Writes data to files and returns information about the written files.
   *
   * @return A list of DataFile objects representing the written files. Each DataFile contains
   *     metadata about a single written file, such as its path, size, and other relevant
   *     information.
   */
  List<DataFile> write();
}
