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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.metrics.ContentCacheRuntimeMetrics;
import com.arcesium.swiftlake.metrics.FileSystemCacheRuntimeMetrics;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultCacheFileIOTest {

  @Mock private ContentCache mockContentCache;

  @Mock private FileSystemCache mockFileSystemCache;

  @Mock private SwiftLakeFileIO mockSwiftLakeFileIO;

  private DefaultCacheFileIO defaultCacheFileIO;

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    defaultCacheFileIO = new DefaultCacheFileIO(mockContentCache, mockFileSystemCache);
  }

  @Test
  void testInitialize() {
    Map<String, String> properties = new HashMap<>();
    properties.put("swiftlake.fileio.manifest-cache.enabled", "true");
    properties.put("swiftlake.fileio.manifest-cache.expiration-interval-seconds", "3600");
    properties.put("swiftlake.fileio.manifest-cache.max-total-bytes", "1073741824");
    properties.put("swiftlake.fileio.manifest-cache.max-content-length", "1048576");
    properties.put("swiftlake.fileio.filesystem-cache.enabled", "true");
    properties.put("swiftlake.fileio.filesystem-cache.base-path", tempDir.toString());
    properties.put("swiftlake.fileio.filesystem-cache.expiration-interval-seconds", "3600");
    properties.put("swiftlake.fileio.filesystem-cache.max-total-bytes", "1073741824");

    DefaultCacheFileIO cacheFileIO = new DefaultCacheFileIO();
    cacheFileIO.initialize(properties);

    assertThat(cacheFileIO).extracting("contentCache").isNotNull();
    assertThat(cacheFileIO).extracting("fileSystemCache").isNotNull();
  }

  @Test
  void testNewInputFileWithFileSystemCache() throws Exception {
    String path = "test/file.txt";
    Path localFile = tempDir.resolve("cached_file.txt");
    Files.write(localFile, "test content".getBytes());

    InputFiles inputFiles = mock(InputFiles.class);
    com.arcesium.swiftlake.common.InputFile mockInputFile =
        mock(com.arcesium.swiftlake.common.InputFile.class);
    List<com.arcesium.swiftlake.common.InputFile> inputFileList = List.of(mockInputFile);
    when(inputFiles.getInputFiles()).thenReturn(inputFileList);
    when(mockInputFile.getLocalFileLocation()).thenReturn(localFile.toString());
    when(mockFileSystemCache.get(mockSwiftLakeFileIO, Arrays.asList(path))).thenReturn(inputFiles);

    InputFile result = defaultCacheFileIO.newInputFile(mockSwiftLakeFileIO, path);
    result.newStream();
    assertThat(result).extracting("inputFiles").isSameAs(inputFiles);
  }

  @Test
  void testNewInputFileWithoutFileSystemCache() {
    DefaultCacheFileIO cacheFileIO = new DefaultCacheFileIO(mockContentCache, null);
    String path = "test/file.txt";
    InputFile mockInputFile = mock(InputFile.class);
    when(mockSwiftLakeFileIO.newInputFile(path)).thenReturn(mockInputFile);

    InputFile result = cacheFileIO.newInputFile(mockSwiftLakeFileIO, path);

    assertThat(result).isEqualTo(mockInputFile);
  }

  @Test
  void testNewInputFilesWithFileSystemCache() {
    List<String> paths = Arrays.asList("test/file1.txt", "test/file2.txt");
    InputFiles mockInputFiles = mock(InputFiles.class);
    when(mockFileSystemCache.get(mockSwiftLakeFileIO, paths)).thenReturn(mockInputFiles);

    InputFiles result = defaultCacheFileIO.newInputFiles(mockSwiftLakeFileIO, paths);

    assertThat(result).isEqualTo(mockInputFiles);
  }

  @Test
  void testNewInputFilesWithoutFileSystemCache() {
    DefaultCacheFileIO cacheFileIO = new DefaultCacheFileIO(mockContentCache, null);
    List<String> paths = Arrays.asList("test/file1.txt", "test/file2.txt");
    InputFiles mockInputFiles = mock(InputFiles.class);
    when(mockSwiftLakeFileIO.downloadFiles(paths)).thenReturn(mockInputFiles);

    InputFiles result = cacheFileIO.newInputFiles(mockSwiftLakeFileIO, paths);

    assertThat(result).isEqualTo(mockInputFiles);
  }

  @Test
  void testNewInputFileWithContentCache() {
    String path = "test/file.txt";
    long length = 1024L;
    InputFile mockInputFile = mock(InputFile.class);
    when(mockContentCache.get(mockSwiftLakeFileIO, path, length)).thenReturn(mockInputFile);

    InputFile result = defaultCacheFileIO.newInputFile(mockSwiftLakeFileIO, path, length);

    assertThat(result).isEqualTo(mockInputFile);
  }

  @Test
  void testNewInputFileWithoutContentCache() throws Exception {
    String path = "test/file.txt";
    DefaultCacheFileIO cacheFileIO = new DefaultCacheFileIO(null, mockFileSystemCache);
    Path localFile = tempDir.resolve("cached_file.txt");
    Files.write(localFile, "test content".getBytes());
    long length = 1024L;
    InputFiles inputFiles = mock(InputFiles.class);
    com.arcesium.swiftlake.common.InputFile mockInputFile =
        mock(com.arcesium.swiftlake.common.InputFile.class);
    List<com.arcesium.swiftlake.common.InputFile> inputFileList = List.of(mockInputFile);
    when(inputFiles.getInputFiles()).thenReturn(inputFileList);
    when(mockInputFile.getLocalFileLocation()).thenReturn(localFile.toString());
    when(mockFileSystemCache.get(mockSwiftLakeFileIO, Arrays.asList(path))).thenReturn(inputFiles);

    InputFile result = cacheFileIO.newInputFile(mockSwiftLakeFileIO, path, length);
    assertThat(result.newStream().readAllBytes()).isEqualTo("test content".getBytes());
  }

  @Test
  void testGetFileSystemCacheRuntimeMetrics() {
    FileSystemCacheRuntimeMetrics mockMetrics = mock(FileSystemCacheRuntimeMetrics.class);
    when(mockFileSystemCache.getRuntimeMetrics()).thenReturn(mockMetrics);

    FileSystemCacheRuntimeMetrics result = defaultCacheFileIO.getFileSystemCacheRuntimeMetrics();

    assertThat(result).isEqualTo(mockMetrics);
  }

  @Test
  void testGetContentCacheRuntimeMetrics() {
    ContentCacheRuntimeMetrics mockMetrics = mock(ContentCacheRuntimeMetrics.class);
    when(mockContentCache.getRuntimeMetrics()).thenReturn(mockMetrics);

    ContentCacheRuntimeMetrics result = defaultCacheFileIO.getContentCacheRuntimeMetrics();

    assertThat(result).isEqualTo(mockMetrics);
  }

  @Test
  void testFileCachingInputFile() throws IOException {
    String path = "test/file.txt";
    File tempFile = tempDir.resolve("cached_file.txt").toFile();
    Files.write(tempFile.toPath(), "test content".getBytes());

    InputFiles mockInputFiles = mock(InputFiles.class);
    com.arcesium.swiftlake.common.InputFile mockInputFile =
        mock(com.arcesium.swiftlake.common.InputFile.class);
    when(mockInputFiles.getInputFiles()).thenReturn(Arrays.asList(mockInputFile));
    when(mockInputFile.getLocalFileLocation()).thenReturn(tempFile.getAbsolutePath());
    when(mockFileSystemCache.get(mockSwiftLakeFileIO, Arrays.asList(path)))
        .thenReturn(mockInputFiles);

    InputFile fileCachingInputFile = defaultCacheFileIO.newInputFile(mockSwiftLakeFileIO, path);

    assertThat(fileCachingInputFile.exists()).isTrue();
    assertThat(fileCachingInputFile.getLength()).isEqualTo(12L); // "test content".length()
    assertThat(fileCachingInputFile.location()).isEqualTo(path);

    try (SeekableInputStream stream = fileCachingInputFile.newStream()) {
      byte[] content = new byte[12];
      stream.read(content);
      assertThat(new String(content)).isEqualTo("test content");
    }
  }
}
