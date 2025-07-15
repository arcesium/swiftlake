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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.common.InputFile;
import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.metrics.FileSystemCacheRuntimeMetrics;
import com.arcesium.swiftlake.metrics.MetricCollector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class FileSystemCacheTest {

  @TempDir Path tempDir;

  @Mock private SwiftLakeFileIO mockFileIO;

  @Mock private MetricCollector mockMetricCollector;

  private FileSystemCache fileSystemCache;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    fileSystemCache = new FileSystemCache(tempDir.toString(), 60, 1024 * 1024, mockMetricCollector);
  }

  @Test
  void testConstructorWithInvalidParameters() {
    assertThatThrownBy(() -> new FileSystemCache(null, 60, 1024 * 1024, mockMetricCollector))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("basePath is null or empty");

    assertThatThrownBy(
            () -> new FileSystemCache(tempDir.toString(), 0, 1024 * 1024, mockMetricCollector))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("expireAfterAccessInSeconds is less than 0");

    assertThatThrownBy(() -> new FileSystemCache(tempDir.toString(), 60, 0, mockMetricCollector))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("maxTotalBytes is equal or less than 0");
  }

  @Test
  void testGetSingleFile() throws IOException {
    String location = "test/file.txt";
    Path localFile = tempDir.resolve("cached_file.txt");
    Files.write(localFile, "test content".getBytes());

    when(mockFileIO.downloadFileAsync(eq(location), any(Path.class)))
        .thenAnswer(
            invocation -> {
              Files.copy(localFile, (Path) invocation.getArgument(1));
              return CompletableFuture.completedFuture(null);
            });

    InputFiles result = fileSystemCache.get(mockFileIO, List.of(location));

    assertThat(result).isNotNull();
    assertThat(result.getInputFiles()).hasSize(1);
    InputFile inputFile = result.getInputFiles().get(0);
    assertThat(inputFile.getLocation()).isEqualTo(location);
    assertThat(inputFile.getLength()).isEqualTo(12);
    assertThat(inputFile.getLocalFileLocation()).startsWith(tempDir.toString());
  }

  @Test
  void testGetMultipleFiles() throws IOException {
    String location1 = "test/file1.txt";
    String location2 = "test/file2.txt";
    Path localFile1 = tempDir.resolve("cached_file1.txt");
    Path localFile2 = tempDir.resolve("cached_file2.txt");
    Files.write(localFile1, "test content 1".getBytes());
    Files.write(localFile2, "test content 2".getBytes());

    when(mockFileIO.downloadFileAsync(eq(location1), any(Path.class)))
        .thenAnswer(
            invocation -> {
              Files.copy(localFile1, (Path) invocation.getArgument(1));
              return CompletableFuture.completedFuture(null);
            });
    when(mockFileIO.downloadFileAsync(eq(location2), any(Path.class)))
        .thenAnswer(
            invocation -> {
              Files.copy(localFile2, (Path) invocation.getArgument(1));
              return CompletableFuture.completedFuture(null);
            });

    InputFiles result = fileSystemCache.get(mockFileIO, Arrays.asList(location1, location2));

    assertThat(result).isNotNull();
    assertThat(result.getInputFiles()).hasSize(2);
    assertThat(result.getInputFiles())
        .extracting(InputFile::getLocation)
        .containsExactlyInAnyOrder(location1, location2);
  }

  @Test
  void testCacheHitAndMiss() throws IOException {
    String location = "test/file.txt";
    Path localFile = tempDir.resolve("cached_file.txt");
    Files.write(localFile, "test content".getBytes());

    when(mockFileIO.downloadFileAsync(eq(location), any(Path.class)))
        .thenAnswer(
            invocation -> {
              Files.copy(localFile, (Path) invocation.getArgument(1));
              return CompletableFuture.completedFuture(null);
            });

    // First call - should be a miss
    fileSystemCache.get(mockFileIO, List.of(location));

    // Second call - should be a hit
    fileSystemCache.get(mockFileIO, List.of(location));

    verify(mockFileIO, times(1)).downloadFileAsync(eq(location), any(Path.class));
    verify(mockMetricCollector, times(2)).collectMetrics(any());
  }

  @Test
  void testGetRuntimeMetrics() {
    FileSystemCacheRuntimeMetrics metrics = fileSystemCache.getRuntimeMetrics();

    assertThat(metrics).isNotNull();
    assertThat(metrics.getFileCount()).isZero();
    assertThat(metrics.getStorageLimitInBytes()).isEqualTo(1024 * 1024);
    assertThat(metrics.getStorageUsageInBytes()).isZero();
  }

  @Test
  void testCacheEviction() throws IOException {
    // Create a cache with a very small max size to force evictions
    FileSystemCache smallCache =
        new FileSystemCache(tempDir.toString(), 1, 20, mockMetricCollector);

    String location1 = "test/file1.txt";
    String location2 = "test/file2.txt";
    Path localFile1 = tempDir.resolve("cached_file1.txt");
    Path localFile2 = tempDir.resolve("cached_file2.txt");
    Files.write(localFile1, "test content 1".getBytes());
    Files.write(localFile2, "test content 2 longer".getBytes());

    when(mockFileIO.downloadFileAsync(eq(location1), any(Path.class)))
        .thenAnswer(
            invocation -> {
              Files.copy(localFile1, (Path) invocation.getArgument(1));
              return CompletableFuture.completedFuture(null);
            });
    when(mockFileIO.downloadFileAsync(eq(location2), any(Path.class)))
        .thenAnswer(
            invocation -> {
              Files.copy(localFile2, (Path) invocation.getArgument(1));
              return CompletableFuture.completedFuture(null);
            });
    // This should cause an eviction
    smallCache.get(mockFileIO, Arrays.asList(location1, location2));

    verify(mockMetricCollector, atLeastOnce()).collectMetrics(any());
  }

  @Test
  void testCloseInputFiles() throws Exception {
    String location = "test/file.txt";
    Path localFile = tempDir.resolve("cached_file.txt");
    Files.write(localFile, "test content".getBytes());

    when(mockFileIO.downloadFileAsync(eq(location), any(Path.class)))
        .thenAnswer(
            invocation -> {
              Files.copy(localFile, (Path) invocation.getArgument(1));
              return CompletableFuture.completedFuture(null);
            });

    InputFiles result = fileSystemCache.get(mockFileIO, List.of(location));
    result.close();

    // Verify that closing doesn't throw an exception
    assertThatCode(() -> result.close()).doesNotThrowAnyException();
  }

  @Test
  void testDownloadFailureHandling() {
    String location = "test/non_existent_file.txt";

    when(mockFileIO.downloadFileAsync(eq(location), any(Path.class)))
        .thenAnswer(
            invocation ->
                CompletableFuture.runAsync(
                    () -> {
                      try {
                        Thread.sleep(2000);
                        throw new RuntimeException("Download error");
                      } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                      }
                    }));

    assertThatThrownBy(() -> fileSystemCache.get(mockFileIO, List.of(location)))
        .isInstanceOf(CompletionException.class)
        .hasMessageContaining("Failed to download file: " + location)
        .hasCauseInstanceOf(SwiftLakeException.class);

    verify(mockFileIO).downloadFileAsync(eq(location), any(Path.class));
  }

  @Test
  void testDownloadCompletesButFileDoesNotExist() {
    String location = "test/phantom_file.txt";

    when(mockFileIO.downloadFileAsync(eq(location), any(Path.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    assertThatThrownBy(() -> fileSystemCache.get(mockFileIO, List.of(location)))
        .isInstanceOf(CompletionException.class)
        .hasMessageContaining("Download completed but file does not exist")
        .hasCauseInstanceOf(ValidationException.class);

    verify(mockFileIO).downloadFileAsync(eq(location), any(Path.class));
  }

  @Test
  void testRuntimeExceptionDuringDownloadSetup() {
    String location = "test/error_file.txt";

    when(mockFileIO.downloadFileAsync(eq(location), any(Path.class)))
        .thenThrow(new RuntimeException("Error setting up download"));

    assertThatThrownBy(() -> fileSystemCache.get(mockFileIO, List.of(location)))
        .isInstanceOf(CompletionException.class)
        .hasMessageContaining("Error setting up download for: " + location)
        .hasCauseInstanceOf(SwiftLakeException.class);

    verify(mockFileIO).downloadFileAsync(eq(location), any(Path.class));
  }
}
