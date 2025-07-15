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

import java.util.ArrayList;
import java.util.List;

/**
 * The MultipleInputFiles class implements the InputFiles interface to manage multiple InputFiles
 * objects. It aggregates InputFile objects from multiple InputFiles sources.
 */
public class MultipleInputFiles implements InputFiles {
  private final List<InputFiles> inputFilesList;
  private final List<InputFile> inputFiles;

  /**
   * Constructs a MultipleInputFiles object with the given list of InputFiles.
   *
   * @param inputFilesList A list of InputFiles objects to be managed.
   */
  public MultipleInputFiles(List<InputFiles> inputFilesList) {
    this.inputFilesList = inputFilesList;
    this.inputFiles = new ArrayList<>();
    inputFilesList.forEach(i -> this.inputFiles.addAll(i.getInputFiles()));
  }

  /**
   * Retrieves the aggregated list of InputFile objects.
   *
   * @return A List of InputFile objects from all managed InputFiles sources.
   */
  @Override
  public List<InputFile> getInputFiles() {
    return inputFiles;
  }

  /**
   * Closes all managed InputFiles resources. If an exception occurs while closing any resource,
   * it's wrapped in a SwiftLakeException.
   *
   * @throws Exception if an error occurs while closing the resources.
   */
  @Override
  public void close() throws Exception {
    inputFilesList.forEach(
        i -> {
          try {
            i.close();
          } catch (Exception e) {
            throw new SwiftLakeException(e, "An error occurred while closing resource.");
          }
        });
  }
}
