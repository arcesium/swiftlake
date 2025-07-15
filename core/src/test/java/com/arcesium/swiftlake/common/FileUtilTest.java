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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FileUtilTest {

  @TempDir Path tempDir;

  @Test
  void testIsEmptyFolder() throws IOException {
    Path emptyFolder = tempDir.resolve("empty");
    Files.createDirectory(emptyFolder);

    Path nonEmptyFolder = tempDir.resolve("nonEmpty");
    Files.createDirectory(nonEmptyFolder);
    Files.createFile(nonEmptyFolder.resolve("file.txt"));

    assertThat(FileUtil.isEmptyFolder(emptyFolder.toString())).isTrue();
    assertThat(FileUtil.isEmptyFolder(nonEmptyFolder.toString())).isFalse();
    assertThat(FileUtil.isEmptyFolder(tempDir.resolve("nonExistent").toString())).isFalse();
  }

  @Test
  void testGetFilePathsInFolder() throws IOException {
    Path folder = tempDir.resolve("testFolder");
    Files.createDirectory(folder);
    Files.createFile(folder.resolve("file1.txt"));
    Files.createFile(folder.resolve("file2.txt"));

    List<String> filePaths = FileUtil.getFilePathsInFolder(folder.toString());

    assertThat(filePaths)
        .hasSize(2)
        .allMatch(path -> path.endsWith("file1.txt") || path.endsWith("file2.txt"));
  }

  @Test
  void testGetFilesInFolder() throws IOException {
    Path folder = tempDir.resolve("testFolder");
    Files.createDirectory(folder);
    Files.createFile(folder.resolve("file1.txt"));
    Files.createFile(folder.resolve("file2.txt"));
    Path subFolder = folder.resolve("subFolder");
    Files.createDirectory(subFolder);
    Files.createFile(subFolder.resolve("file3.txt"));

    List<File> files = FileUtil.getFilesInFolder(folder.toString());

    assertThat(files)
        .hasSize(3)
        .extracting(File::getName)
        .containsExactlyInAnyOrder("file1.txt", "file2.txt", "file3.txt");
  }

  @ParameterizedTest
  @CsvSource({
    "5MB, 5000000",
    "6MiB, 6291456",
    "2.5GB, 2500000000",
    "2.5GiB, 2684354560",
    "10KB, 10000",
    "10KiB, 10240",
    "1TB, 1000000000000",
    "2TiB, 2199023255552",
    "512bytes, 512",
    "1byte, 1"
  })
  void testParseDataSizeString(String input, long expected) {
    assertThat(FileUtil.parseDataSizeString(input)).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {"5XB", "2.5", "invalid"})
  void testParseDataSizeStringWithInvalidInput(String input) {
    assertThatThrownBy(() -> FileUtil.parseDataSizeString(input))
        .isInstanceOf(ValidationException.class);
  }

  @ParameterizedTest
  @CsvSource({
    "/path/to/file/, /path/to/file",
    "path/to/file/, path/to/file",
    "/path/to/file, /path/to/file"
  })
  void testStripTrailingSlash(String input, String expected) {
    assertThat(FileUtil.stripTrailingSlash(input)).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "/path/to/file/, path/to/file",
    "path/to/file/, path/to/file",
    "/path/to/file, path/to/file"
  })
  void testNormalizePath(String input, String expected) {
    assertThat(FileUtil.normalizePath(input)).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "file:///path/to/file, file",
    "s3://bucket/path/to/file, s3",
    "/path/to/file, ''",
    "path/to/file, ''"
  })
  void testGetScheme(String input, String expected) {
    assertThat(FileUtil.getScheme(input)).isEqualTo("".equals(expected) ? null : expected);
  }

  @Test
  void testConcatPaths() {
    assertThat(FileUtil.concatPaths("/base/path", "sub", "folder", "file.txt"))
        .isEqualTo("/base/path/sub/folder/file.txt");
    assertThat(FileUtil.concatPaths("base/path/", "/sub/", "/folder/", "/file.txt"))
        .isEqualTo("base/path/sub/folder/file.txt");
    assertThat(FileUtil.concatPaths("/base/path", null, "", "  ", "file.txt"))
        .isEqualTo("/base/path/file.txt");
    assertThat(FileUtil.concatPaths("/base/path")).isEqualTo("/base/path");
  }
}
