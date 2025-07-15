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
import com.arcesium.swiftlake.common.InputFiles;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DefaultInputFiles is an implementation of the InputFiles interface. It manages a list of
 * InputFile objects and provides functionality to close and delete local files.
 */
public class DefaultInputFiles implements InputFiles {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultInputFiles.class);
  private List<InputFile> inputFiles;

  /**
   * Constructs a new DefaultInputFiles with the specified list of InputFile objects.
   *
   * @param inputFiles A list of InputFile objects to be managed
   */
  public DefaultInputFiles(List<InputFile> inputFiles) {
    this.inputFiles = inputFiles;
  }

  @Override
  public void close() {
    if (this.inputFiles != null) {
      this.deleteLocalFiles(
          inputFiles.stream()
              .filter(i -> i.getLocalFileLocation() != null)
              .map(i -> i.getLocalFileLocation())
              .collect(Collectors.toList()));
      this.inputFiles = null;
    }
  }

  private void deleteLocalFiles(List<String> files) {
    LOGGER.debug("Deleting files {}", files);
    files.stream()
        .map(file -> Paths.get(file))
        .forEach(
            path -> {
              try {
                Files.deleteIfExists(path);
              } catch (IOException ex) {
                LOGGER.error("Error deleting file {}", path);
                throw new RuntimeException(ex);
              }
            });
  }

  @Override
  public List<InputFile> getInputFiles() {
    return inputFiles;
  }
}
