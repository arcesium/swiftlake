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
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mockStatic;

import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class SwiftLakeHadoopFileIOTest {

  @TempDir Path tempDir;

  private SwiftLakeHadoopFileIO fileIO;
  private Configuration configuration;
  private ExecutorService mockExecutorService;

  @BeforeEach
  void setUp() throws IOException {
    fileIO = new SwiftLakeHadoopFileIO();
    configuration = new Configuration();
    mockExecutorService = Executors.newFixedThreadPool(1);
    configuration.set(SwiftLakeHadoopFileIO.STAGING_DIRECTORY, tempDir.toString());
    configuration.set(SwiftLakeHadoopFileIO.PARALLELISM, 1 + "");
    fileIO.setConf(configuration);
  }

  @Test
  void testInitialize() {
    Map<String, String> props = new HashMap<>();
    props.put("test.key", "test.value");

    fileIO.initialize(props);

    assertThat(fileIO.properties()).containsEntry("test.key", "test.value");
  }

  @Test
  void testGetLogger() {
    assertThat(fileIO.getLogger()).isNotNull();
  }

  @Test
  void testGetLocalDir() {
    assertThat(fileIO.getLocalDir()).isEqualTo(tempDir.toString());
  }

  @Test
  void testDownloadFileAsync() throws Exception {
    Path sourcePath = tempDir.resolve("source.txt");
    Path destinationPath = tempDir.resolve("destination.txt");

    Files.write(sourcePath, "test content".getBytes());

    try (MockedStatic<ThreadPools> mockedThreadPools = mockStatic(ThreadPools.class)) {
      mockedThreadPools
          .when(() -> ThreadPools.newExitingWorkerPool(anyString(), anyInt()))
          .thenReturn(mockExecutorService);

      CompletableFuture<Void> future =
          fileIO.downloadFileAsync(sourcePath.toString(), destinationPath);
      future.get(); // Wait for the future to complete

      assertThat(destinationPath).exists().hasContent("test content");
    }
  }

  @Test
  void testUploadFileAsync() throws Exception {
    Path sourcePath = tempDir.resolve("source.txt");
    Path destinationPath = tempDir.resolve("destination.txt");

    Files.write(sourcePath, "test content".getBytes());

    try (MockedStatic<ThreadPools> mockedThreadPools = mockStatic(ThreadPools.class)) {
      mockedThreadPools
          .when(() -> ThreadPools.newExitingWorkerPool(anyString(), anyInt()))
          .thenReturn(mockExecutorService);

      CompletableFuture<Void> future =
          fileIO.uploadFileAsync(sourcePath, destinationPath.toString());
      future.get(); // Wait for the future to complete

      assertThat(destinationPath).exists().hasContent("test content");
    }
  }

  @Test
  void testNewInputFiles() throws Exception {
    Path file1 = tempDir.resolve("file1.txt");
    Path file2 = tempDir.resolve("file2.txt");
    Files.write(file1, "test content 1".getBytes());
    Files.write(file2, "test content 2".getBytes());
    List<String> paths = Arrays.asList(file1.toString(), file2.toString());

    InputFiles result = fileIO.newInputFiles(paths);

    assertThat(result).isNotNull();
  }

  @Test
  void testIsDownloadable() {
    assertThat(fileIO.isDownloadable("anypath", 1000L)).isTrue();
  }

  @Test
  void testDownloadFileAsyncWithNullSource() {
    assertThatThrownBy(() -> fileIO.downloadFileAsync(null, tempDir))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("source cannot be null");
  }

  @Test
  void testDownloadFileAsyncWithNullDestination() {
    assertThatThrownBy(() -> fileIO.downloadFileAsync("source", null))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("destination cannot be null");
  }

  @Test
  void testUploadFileAsyncWithNullSource() {
    assertThatThrownBy(() -> fileIO.uploadFileAsync(null, "destination"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("source cannot be null");
  }

  @Test
  void testUploadFileAsyncWithNullDestination() {
    assertThatThrownBy(() -> fileIO.uploadFileAsync(tempDir, null))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("destination cannot be null");
  }

  @Test
  void testDownloadFileAsyncWithNonExistentSource() {
    java.nio.file.Path nonExistentSource = tempDir.resolve("nonexistent.txt");
    java.nio.file.Path destination = tempDir.resolve("destination.txt");

    try (MockedStatic<ThreadPools> mockedThreadPools = mockStatic(ThreadPools.class)) {
      mockedThreadPools
          .when(() -> ThreadPools.newExitingWorkerPool(anyString(), anyInt()))
          .thenReturn(mockExecutorService);

      CompletableFuture<Void> future =
          fileIO.downloadFileAsync(nonExistentSource.toString(), destination);

      assertThatThrownBy(future::get)
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(SwiftLakeException.class)
          .hasRootCauseMessage("File " + nonExistentSource + " does not exist");
    }
  }

  @Test
  void testUploadFileAsyncWithNonExistentSource() {
    java.nio.file.Path nonExistentSource = tempDir.resolve("nonexistent.txt");
    java.nio.file.Path destination = tempDir.resolve("destination.txt");

    try (MockedStatic<ThreadPools> mockedThreadPools = mockStatic(ThreadPools.class)) {
      mockedThreadPools
          .when(() -> ThreadPools.newExitingWorkerPool(anyString(), anyInt()))
          .thenReturn(mockExecutorService);

      CompletableFuture<Void> future =
          fileIO.uploadFileAsync(nonExistentSource, destination.toString());

      assertThatThrownBy(future::get)
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(SwiftLakeException.class)
          .hasRootCauseMessage("File " + nonExistentSource + " does not exist");
    }
  }
}
