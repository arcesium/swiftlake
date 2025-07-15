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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.metrics.ContentCacheMetrics;
import com.arcesium.swiftlake.metrics.ContentCacheRuntimeMetrics;
import com.arcesium.swiftlake.metrics.MetricCollector;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ContentCacheTest {
  @Mock private MetricCollector mockMetricCollector;

  @Mock private SwiftLakeFileIO mockFileIO;

  private ContentCache contentCache;

  @Captor private ArgumentCaptor<ContentCacheMetrics> metricsCaptor;

  @BeforeEach
  void setUp() {
    contentCache = new ContentCache(60, 1024 * 1024, 1024, mockMetricCollector);
  }

  @Test
  void testConstructorWithValidParameters() {
    assertThat(contentCache.expireAfterAccessInSeconds()).isEqualTo(60);
    assertThat(contentCache.maxTotalBytes()).isEqualTo(1024 * 1024);
    assertThat(contentCache.maxContentLength()).isEqualTo(1024);
  }

  @Test
  void testConstructorWithInvalidParameters() {
    assertThatThrownBy(() -> new ContentCache(-1, 1024, 1024, mockMetricCollector))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("expireAfterAccessInSeconds is less than 0");

    assertThatThrownBy(() -> new ContentCache(60, 0, 1024, mockMetricCollector))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("maxTotalBytes is equal or less than 0");

    assertThatThrownBy(() -> new ContentCache(60, 1024, 0, mockMetricCollector))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("maxContentLength is equal or less than 0");
  }

  @Test
  void testGetInputFile() throws IOException {
    String location = "testLocation";
    String content = "Test content";
    long length = content.length();
    InputFile inputFile = mock(InputFile.class);
    when(mockFileIO.newInputFile(location, length, false)).thenReturn(inputFile);

    when(inputFile.getLength()).thenReturn(length);
    when(inputFile.newStream()).thenReturn(createSeekableInputStream(content));

    InputFile result = contentCache.get(mockFileIO, location, length);

    assertThat(result).isNotSameAs(inputFile);
    assertThat(result.getLength()).isEqualTo(length);
    assertThat(result.location()).isEqualTo(location);

    // Read content to trigger caching
    SeekableInputStream stream = result.newStream();
    byte[] buffer = new byte[(int) length];
    stream.read(buffer);
    assertThat(new String(buffer, StandardCharsets.UTF_8)).isEqualTo("Test content");

    assertThat(result.exists()).isTrue();

    // Verify that subsequent reads use the cache
    InputFile cachedResult = contentCache.get(mockFileIO, location, length);
    verify(mockFileIO, times(1)).newInputFile(location, length, false);
    verify(inputFile, times(1)).newStream();
  }

  @Test
  void testGetLargeInputFile() {
    String location = "testLocation";
    long length = contentCache.maxContentLength() + 1;

    InputFile inputFile = mock(InputFile.class);
    when(mockFileIO.newInputFile(location, length)).thenReturn(inputFile);

    InputFile result = contentCache.get(mockFileIO, location, length);

    assertThat(result).isSameAs(inputFile);
  }

  @Test
  void testInvalidate() {
    String location = "testLocation";
    String content = "Test content 1";
    long length = content.length();

    InputFile inputFile = mock(InputFile.class);
    when(mockFileIO.newInputFile(location, length, false)).thenReturn(inputFile);
    when(inputFile.getLength()).thenReturn(length);
    when(inputFile.newStream()).thenReturn(createSeekableInputStream(content));

    contentCache.get(mockFileIO, location, length).newStream();
    contentCache.invalidate(location);

    when(inputFile.newStream()).thenReturn(createSeekableInputStream(content));
    contentCache.get(mockFileIO, location, length).newStream();

    verify(mockFileIO, times(2)).newInputFile(location, length, false);
  }

  @Test
  void testInvalidateAll() {
    String location1 = "testLocation1";
    String location2 = "testLocation2";
    String content1 = "Test content 1";
    String content2 = "Test content 2...";
    long length1 = content1.length();
    long length2 = content2.length();
    InputFile inputFile1 = mock(InputFile.class);
    InputFile inputFile2 = mock(InputFile.class);
    when(mockFileIO.newInputFile(location1, length1, false)).thenReturn(inputFile1);
    when(mockFileIO.newInputFile(location2, length2, false)).thenReturn(inputFile2);
    when(inputFile1.getLength()).thenReturn(length1);
    when(inputFile2.getLength()).thenReturn(length2);
    when(inputFile1.newStream()).thenReturn(createSeekableInputStream(content1));
    when(inputFile2.newStream()).thenReturn(createSeekableInputStream(content2));

    contentCache.get(mockFileIO, location1, length1).newStream();
    contentCache.get(mockFileIO, location2, length2).newStream();

    contentCache.invalidateAll();

    when(inputFile1.newStream()).thenReturn(createSeekableInputStream(content1));
    when(inputFile2.newStream()).thenReturn(createSeekableInputStream(content2));

    contentCache.get(mockFileIO, location1, length1).newStream();
    contentCache.get(mockFileIO, location2, length2).newStream();

    verify(mockFileIO, times(2)).newInputFile(location1, length1, false);
    verify(mockFileIO, times(2)).newInputFile(location2, length2, false);
  }

  @Test
  void testCleanUp() {
    contentCache.cleanUp();
  }

  @Test
  void testEstimatedCacheSize() {
    String location1 = "testLocation1";
    String location2 = "testLocation2";
    String content1 = "Test content 1";
    String content2 = "Test content 2...";
    long length1 = content1.length();
    long length2 = content2.length();
    InputFile inputFile1 = mock(InputFile.class);
    InputFile inputFile2 = mock(InputFile.class);
    when(mockFileIO.newInputFile(location1, length1, false)).thenReturn(inputFile1);
    when(mockFileIO.newInputFile(location2, length2, false)).thenReturn(inputFile2);
    when(inputFile1.getLength()).thenReturn(length1);
    when(inputFile2.getLength()).thenReturn(length2);
    when(inputFile1.newStream()).thenReturn(createSeekableInputStream(content1));
    when(inputFile2.newStream()).thenReturn(createSeekableInputStream(content2));

    contentCache.get(mockFileIO, location1, length1).newStream();
    contentCache.get(mockFileIO, location2, length2).newStream();

    assertThat(contentCache.estimatedCacheSize()).isEqualTo(2);
  }

  @Test
  void testGetRuntimeMetrics() {
    ContentCacheRuntimeMetrics metrics = contentCache.getRuntimeMetrics();

    assertThat(metrics).isNotNull();
    assertThat(metrics.getFileCount()).isEqualTo(0);
    assertThat(metrics.getMaxTotalBytes()).isEqualTo(1024 * 1024);
    assertThat(metrics.getCurrentUsageInBytes()).isEqualTo(0);
  }

  @Test
  void testRuntimeMetricsCollection() throws Exception {
    String location1 = "testLocation1";
    String location2 = "testLocation2";
    String content1 = "Test content 1";
    String content2 = "Test content 2...";
    long length1 = content1.length();
    long length2 = content2.length();
    InputFile inputFile1 = mock(InputFile.class);
    InputFile inputFile2 = mock(InputFile.class);
    when(mockFileIO.newInputFile(location1, length1, false)).thenReturn(inputFile1);
    when(mockFileIO.newInputFile(location2, length2, false)).thenReturn(inputFile2);
    when(inputFile1.getLength()).thenReturn(length1);
    when(inputFile2.getLength()).thenReturn(length2);
    when(inputFile1.newStream()).thenReturn(createSeekableInputStream(content1));
    when(inputFile2.newStream()).thenReturn(createSeekableInputStream(content2));

    contentCache.get(mockFileIO, location1, length1).newStream();
    contentCache.get(mockFileIO, location2, length2).newStream();

    Thread.sleep(1000);
    ContentCacheRuntimeMetrics runtimeMetrics = contentCache.getRuntimeMetrics();

    assertThat(runtimeMetrics.getFileCount()).isEqualTo(2);
    assertThat(runtimeMetrics.getMaxTotalBytes()).isEqualTo(1024 * 1024);
    assertThat(runtimeMetrics.getCurrentUsageInBytes()).isEqualTo(length1 + length2);
  }

  @Test
  void testMetricsCollectionOnCacheHit() throws IOException {
    String location = "testLocation";
    String content = "Test content 1";
    long length = content.length();

    InputFile inputFile = mock(InputFile.class);
    when(mockFileIO.newInputFile(location, length, false)).thenReturn(inputFile);
    when(inputFile.getLength()).thenReturn(length);
    when(inputFile.newStream()).thenReturn(createSeekableInputStream(content));

    // First call - should be a miss
    contentCache.get(mockFileIO, location, length).newStream();

    // Second call - should be a hit
    contentCache.get(mockFileIO, location, length).newStream();

    // Stats reporting is not accurate due to other internal calls to the cache
    verify(mockMetricCollector, times(5)).collectMetrics(metricsCaptor.capture());
    List<ContentCacheMetrics> capturedMetrics = metricsCaptor.getAllValues();

    assertThat(capturedMetrics.get(0).getMissCount()).isEqualTo(1);
    assertThat(capturedMetrics.get(0).getHitCount()).isEqualTo(0);
    assertThat(capturedMetrics.get(1).getMissCount()).isEqualTo(1);
    assertThat(capturedMetrics.get(1).getHitCount()).isEqualTo(0);
    assertThat(capturedMetrics.get(2).getMissCount()).isEqualTo(1);
    assertThat(capturedMetrics.get(2).getHitCount()).isEqualTo(0);
    assertThat(capturedMetrics.get(3).getMissCount()).isEqualTo(0);
    assertThat(capturedMetrics.get(3).getHitCount()).isEqualTo(1);
    assertThat(capturedMetrics.get(4).getMissCount()).isEqualTo(0);
    assertThat(capturedMetrics.get(4).getHitCount()).isEqualTo(1);
  }

  @Test
  void testMetricsCollectionOnCacheEviction() throws Exception {
    // Create a cache with a very small max size to force evictions
    ContentCache smallCache = new ContentCache(60, 20, 100, mockMetricCollector);

    String location1 = "testLocation1";
    String location2 = "testLocation2";
    String content1 = "Test content 1";
    String content2 = "Test content 2...";
    long length1 = content1.length();
    long length2 = content2.length();
    InputFile inputFile1 = mock(InputFile.class);
    InputFile inputFile2 = mock(InputFile.class);
    when(mockFileIO.newInputFile(location1, length1, false)).thenReturn(inputFile1);
    when(mockFileIO.newInputFile(location2, length2, false)).thenReturn(inputFile2);
    when(inputFile1.getLength()).thenReturn(length1);
    when(inputFile2.getLength()).thenReturn(length2);
    when(inputFile1.newStream()).thenReturn(createSeekableInputStream(content1));
    when(inputFile2.newStream()).thenReturn(createSeekableInputStream(content2));

    // This should cause an eviction
    smallCache.get(mockFileIO, location1, length1).newStream();
    smallCache.get(mockFileIO, location2, length2).newStream();
    Thread.sleep(1000);
    verify(mockMetricCollector, atLeastOnce()).collectMetrics(metricsCaptor.capture());
    List<ContentCacheMetrics> capturedMetrics = metricsCaptor.getAllValues();

    // Check if there's an eviction metric
    boolean evictionMetricFound =
        capturedMetrics.stream().anyMatch(metric -> metric.getEvictionCount() > 0);
    assertThat(evictionMetricFound).isTrue();
  }

  private SeekableInputStream createSeekableInputStream(String content) {
    System.out.println("createSeekableInputStream " + content);
    return new SeekableInputStream() {
      private ByteArrayInputStream bais =
          new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

      @Override
      public long getPos() throws IOException {
        return content.length() - bais.available();
      }

      @Override
      public void seek(long newPos) throws IOException {
        bais.reset();
        bais.skip(newPos);
      }

      @Override
      public int read() throws IOException {
        return bais.read();
      }
    };
  }
}
