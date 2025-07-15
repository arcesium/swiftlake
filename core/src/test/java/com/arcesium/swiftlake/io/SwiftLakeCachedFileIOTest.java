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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.metrics.ContentCacheRuntimeMetrics;
import com.arcesium.swiftlake.metrics.FileSystemCacheRuntimeMetrics;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class SwiftLakeCachedFileIOTest {

  @Mock private SwiftLakeFileIO mockFileIO;

  @Mock private CacheFileIO mockCacheFileIO;

  private SwiftLakeCachedFileIO cachedFileIO;

  @BeforeEach
  void setUp() {
    cachedFileIO = new SwiftLakeCachedFileIO(mockFileIO, mockCacheFileIO);
  }

  @Test
  void testInitialize() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");

    cachedFileIO.initialize(properties);

    verify(mockFileIO).initialize(properties);
  }

  @Test
  void testInitializeWithNullFileIO() {
    SwiftLakeCachedFileIO nullFileIO = new SwiftLakeCachedFileIO();
    Map<String, String> properties = new HashMap<>();
    properties.put("swiftlake.fileio.delegate-impl", "com.example.CustomFileIO");

    assertThatThrownBy(() -> nullFileIO.initialize(properties))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Provide delegate FileIO implementation class");
  }

  @Test
  void testDownloadFileAsync() {
    String source = "source";
    Path destination = Path.of("destination");
    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);

    when(mockFileIO.downloadFileAsync(source, destination)).thenReturn(future);

    CompletableFuture<Void> result = cachedFileIO.downloadFileAsync(source, destination);

    assertThat(result).isSameAs(future);
    verify(mockFileIO).downloadFileAsync(source, destination);
  }

  @Test
  void testUploadFileAsync() {
    Path source = Path.of("source");
    String destination = "destination";
    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);

    when(mockFileIO.uploadFileAsync(source, destination)).thenReturn(future);

    CompletableFuture<Void> result = cachedFileIO.uploadFileAsync(source, destination);

    assertThat(result).isSameAs(future);
    verify(mockFileIO).uploadFileAsync(source, destination);
  }

  @Test
  void testNewInputFile() {
    String path = "test/file.txt";
    InputFile mockInputFile = mock(InputFile.class);

    when(mockCacheFileIO.newInputFile(mockFileIO, path)).thenReturn(mockInputFile);

    InputFile result = cachedFileIO.newInputFile(path);

    assertThat(result).isSameAs(mockInputFile);
    verify(mockCacheFileIO).newInputFile(mockFileIO, path);
  }

  @Test
  void testNewInputFileWithLength() {
    String path = "test/file.txt";
    long length = 1024L;
    InputFile mockInputFile = mock(InputFile.class);

    when(mockCacheFileIO.newInputFile(mockFileIO, path, length)).thenReturn(mockInputFile);

    InputFile result = cachedFileIO.newInputFile(path, length);

    assertThat(result).isSameAs(mockInputFile);
    verify(mockCacheFileIO).newInputFile(mockFileIO, path, length);
  }

  @Test
  void testGetLogger() {
    Logger mockLogger = mock(Logger.class);
    when(mockFileIO.getLogger()).thenReturn(mockLogger);

    Logger result = cachedFileIO.getLogger();

    assertThat(result).isSameAs(mockLogger);
    verify(mockFileIO).getLogger();
  }

  @Test
  void testGetLocalDir() {
    String localDir = "/tmp/local";
    when(mockFileIO.getLocalDir()).thenReturn(localDir);

    String result = cachedFileIO.getLocalDir();

    assertThat(result).isEqualTo(localDir);
    verify(mockFileIO).getLocalDir();
  }

  @Test
  void testNewInputFileWithFileSystemCache() {
    String path = "test/file.txt";
    boolean useFileSystemCache = true;
    InputFile mockInputFile = mock(InputFile.class);

    when(mockCacheFileIO.newInputFile(mockFileIO, path, useFileSystemCache))
        .thenReturn(mockInputFile);

    InputFile result = cachedFileIO.newInputFile(path, useFileSystemCache);

    assertThat(result).isSameAs(mockInputFile);
    verify(mockCacheFileIO).newInputFile(mockFileIO, path, useFileSystemCache);
  }

  @Test
  void testNewInputFileWithLengthAndFileSystemCache() {
    String path = "test/file.txt";
    long length = 1024L;
    boolean useFileSystemCache = true;
    InputFile mockInputFile = mock(InputFile.class);

    when(mockCacheFileIO.newInputFile(mockFileIO, path, length, useFileSystemCache))
        .thenReturn(mockInputFile);

    InputFile result = cachedFileIO.newInputFile(path, length, useFileSystemCache);

    assertThat(result).isSameAs(mockInputFile);
    verify(mockCacheFileIO).newInputFile(mockFileIO, path, length, useFileSystemCache);
  }

  @Test
  void testNewInputFiles() {
    List<String> paths = List.of("file1.txt", "file2.txt");
    InputFiles mockInputFiles = mock(InputFiles.class);

    when(mockCacheFileIO.newInputFiles(mockFileIO, paths)).thenReturn(mockInputFiles);

    InputFiles result = cachedFileIO.newInputFiles(paths);

    assertThat(result).isSameAs(mockInputFiles);
    verify(mockCacheFileIO).newInputFiles(mockFileIO, paths);
  }

  @Test
  void testIsDownloadable() {
    String path = "test/file.txt";
    long length = 1024L;

    when(mockFileIO.isDownloadable(path, length)).thenReturn(true);

    boolean result = cachedFileIO.isDownloadable(path, length);

    assertThat(result).isTrue();
    verify(mockFileIO).isDownloadable(path, length);
  }

  @Test
  void testNewOutputFile() {
    String path = "test/file.txt";
    OutputFile mockOutputFile = mock(OutputFile.class);

    when(mockFileIO.newOutputFile(path)).thenReturn(mockOutputFile);

    OutputFile result = cachedFileIO.newOutputFile(path);

    assertThat(result).isSameAs(mockOutputFile);
    verify(mockFileIO).newOutputFile(path);
  }

  @Test
  void testDeleteFile() {
    String path = "test/file.txt";

    cachedFileIO.deleteFile(path);

    verify(mockFileIO).deleteFile(path);
  }

  @Test
  void testDeleteInputFile() {
    InputFile mockInputFile = mock(InputFile.class);

    cachedFileIO.deleteFile(mockInputFile);

    verify(mockFileIO).deleteFile(mockInputFile);
  }

  @Test
  void testDeleteOutputFile() {
    OutputFile mockOutputFile = mock(OutputFile.class);

    cachedFileIO.deleteFile(mockOutputFile);

    verify(mockFileIO).deleteFile(mockOutputFile);
  }

  @Test
  void testProperties() {
    Map<String, String> properties = Map.of("key", "value");

    when(mockFileIO.properties()).thenReturn(properties);

    Map<String, String> result = cachedFileIO.properties();

    assertThat(result).isEqualTo(properties);
    verify(mockFileIO).properties();
  }

  @Test
  void testClose() {
    cachedFileIO.close();

    verify(mockFileIO).close();
  }

  @Test
  void testGetFileSystemCacheRuntimeMetrics() {
    FileSystemCacheRuntimeMetrics mockMetrics = mock(FileSystemCacheRuntimeMetrics.class);

    when(mockCacheFileIO.getFileSystemCacheRuntimeMetrics()).thenReturn(mockMetrics);

    FileSystemCacheRuntimeMetrics result = cachedFileIO.getFileSystemCacheRuntimeMetrics();

    assertThat(result).isSameAs(mockMetrics);
    verify(mockCacheFileIO).getFileSystemCacheRuntimeMetrics();
  }

  @Test
  void testGetContentCacheRuntimeMetrics() {
    ContentCacheRuntimeMetrics mockMetrics = mock(ContentCacheRuntimeMetrics.class);

    when(mockCacheFileIO.getContentCacheRuntimeMetrics()).thenReturn(mockMetrics);

    ContentCacheRuntimeMetrics result = cachedFileIO.getContentCacheRuntimeMetrics();

    assertThat(result).isSameAs(mockMetrics);
    verify(mockCacheFileIO).getContentCacheRuntimeMetrics();
  }
}
