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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.common.InputFile;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DefaultInputFilesTest {

  @TempDir Path tempDir;

  private DefaultInputFiles defaultInputFiles;
  private List<InputFile> mockInputFiles;

  @BeforeEach
  void setUp() {
    mockInputFiles =
        Arrays.asList(mock(InputFile.class), mock(InputFile.class), mock(InputFile.class));
    defaultInputFiles = new DefaultInputFiles(mockInputFiles);
  }

  @Test
  void testGetInputFiles() {
    List<InputFile> result = defaultInputFiles.getInputFiles();

    assertThat(result).isEqualTo(mockInputFiles);
  }

  @Test
  void testCloseWithLocalFiles() throws IOException {
    Path file1 = Files.createFile(tempDir.resolve("file1.txt"));
    Path file2 = Files.createFile(tempDir.resolve("file2.txt"));

    when(mockInputFiles.get(0).getLocalFileLocation()).thenReturn(file1.toString());
    when(mockInputFiles.get(1).getLocalFileLocation()).thenReturn(file2.toString());
    when(mockInputFiles.get(2).getLocalFileLocation()).thenReturn(null);

    defaultInputFiles.close();

    assertThat(file1).doesNotExist();
    assertThat(file2).doesNotExist();
    assertThat(defaultInputFiles.getInputFiles()).isNull();
  }

  @Test
  void testCloseWithNoLocalFiles() {
    when(mockInputFiles.get(0).getLocalFileLocation()).thenReturn(null);
    when(mockInputFiles.get(1).getLocalFileLocation()).thenReturn(null);
    when(mockInputFiles.get(2).getLocalFileLocation()).thenReturn(null);

    defaultInputFiles.close();

    assertThat(defaultInputFiles.getInputFiles()).isNull();
  }

  @Test
  void testCloseWithEmptyInputFilesList() {
    defaultInputFiles = new DefaultInputFiles(Collections.emptyList());

    assertThatCode(() -> defaultInputFiles.close()).doesNotThrowAnyException();
  }

  @Test
  void testCloseWithNonExistentFiles() {
    when(mockInputFiles.get(0).getLocalFileLocation()).thenReturn("/non/existent/file1.txt");
    when(mockInputFiles.get(1).getLocalFileLocation()).thenReturn("/non/existent/file2.txt");

    assertThatCode(() -> defaultInputFiles.close()).doesNotThrowAnyException();
  }

  @Test
  void testMultipleCloseCalls() throws IOException {
    Path file = Files.createFile(tempDir.resolve("file.txt"));
    when(mockInputFiles.get(0).getLocalFileLocation()).thenReturn(file.toString());

    defaultInputFiles.close();
    assertThat(file).doesNotExist();

    // Second call should not throw an exception
    assertThatCode(() -> defaultInputFiles.close()).doesNotThrowAnyException();
  }

  @Test
  void testConstructorWithNullList() {
    assertThatCode(() -> new DefaultInputFiles(null)).doesNotThrowAnyException();
  }

  @Test
  void testGetInputFilesAfterClose() {
    defaultInputFiles.close();

    assertThat(defaultInputFiles.getInputFiles()).isNull();
  }
}
