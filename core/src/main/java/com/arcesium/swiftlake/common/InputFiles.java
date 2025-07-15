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

import java.util.List;

/**
 * The InputFiles interface represents a collection of InputFile objects. It extends AutoCloseable
 * to ensure proper resource management.
 */
public interface InputFiles extends AutoCloseable {
  /**
   * Retrieves a list of InputFile objects.
   *
   * @return A List of InputFile objects representing the input files.
   */
  List<InputFile> getInputFiles();

  /**
   * Closes any resources associated with the InputFiles. This method is inherited from the
   * AutoCloseable interface.
   *
   * @throws Exception if an error occurs while closing the resources.
   */
  @Override
  void close() throws Exception;
}
