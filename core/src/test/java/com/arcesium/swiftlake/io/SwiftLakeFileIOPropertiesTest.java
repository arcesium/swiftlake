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

import static org.assertj.core.api.Assertions.*;

import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.metrics.ContentCacheRuntimeMetrics;
import com.arcesium.swiftlake.metrics.FileSystemCacheRuntimeMetrics;
import com.arcesium.swiftlake.metrics.MetricCollector;
import com.arcesium.swiftlake.metrics.MetricCollectorProvider;
import com.arcesium.swiftlake.metrics.Metrics;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.InputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SwiftLakeFileIOPropertiesTest {

  @TempDir Path tempDir;

  @Test
  void testDefaultConstructor() {
    SwiftLakeFileIOProperties properties = new SwiftLakeFileIOProperties();

    assertThat(properties.isManifestCacheEnabled()).isTrue();
    assertThat(properties.getManifestCacheMaxTotalBytes()).isEqualTo(256L * 1024 * 1024);
    assertThat(properties.getManifestCacheMaxContentLength()).isEqualTo(16L * 1024 * 1024);
    assertThat(properties.getManifestCacheExpirationIntervalInSeconds()).isEqualTo(7200);
    assertThat(properties.isFileSystemCacheEnabled()).isTrue();
    assertThat(properties.getFileSystemCacheBasePath())
        .startsWith(System.getProperty("java.io.tmpdir"));
    assertThat(properties.getFileSystemCacheMaxTotalBytes()).isEqualTo(512L * 1024 * 1024);
    assertThat(properties.getFileSystemCacheExpirationIntervalInSeconds()).isEqualTo(172800);
  }

  @Test
  void testConstructorWithProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(SwiftLakeFileIOProperties.DELEGATE_FILE_IO_IMPL, "com.example.CustomFileIO");
    props.put(
        SwiftLakeFileIOProperties.CACHE_FILE_IO_PROVIDER, "com.example.CustomCacheFileIOProvider");
    props.put(
        SwiftLakeFileIOProperties.METRIC_COLLECTOR_PROVIDER,
        "com.example.CustomMetricCollectorProvider");
    props.put(SwiftLakeFileIOProperties.IO_MANIFEST_CACHE_ENABLED, "false");
    props.put(SwiftLakeFileIOProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES, "1048576");
    props.put(SwiftLakeFileIOProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH, "65536");
    props.put(SwiftLakeFileIOProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_SECONDS, "3600");
    props.put(SwiftLakeFileIOProperties.IO_FILE_SYSTEM_CACHE_ENABLED, "false");
    props.put(SwiftLakeFileIOProperties.IO_FILE_SYSTEM_CACHE_BASE_PATH, tempDir.toString());
    props.put(SwiftLakeFileIOProperties.IO_FILE_SYSTEM_CACHE_MAX_TOTAL_BYTES, "2097152");
    props.put(SwiftLakeFileIOProperties.IO_FILE_SYSTEM_CACHE_EXPIRATION_INTERVAL_SECONDS, "86400");

    SwiftLakeFileIOProperties properties = new SwiftLakeFileIOProperties(props);

    assertThat(properties.getDelegateFileIOImplClass()).isEqualTo("com.example.CustomFileIO");
    assertThat(properties.getCacheFileIOProviderClass())
        .isEqualTo("com.example.CustomCacheFileIOProvider");
    assertThat(properties.getMetricCollectorProviderClass())
        .isEqualTo("com.example.CustomMetricCollectorProvider");
    assertThat(properties.isManifestCacheEnabled()).isFalse();
    assertThat(properties.getManifestCacheMaxTotalBytes()).isEqualTo(1048576);
    assertThat(properties.getManifestCacheMaxContentLength()).isEqualTo(65536);
    assertThat(properties.getManifestCacheExpirationIntervalInSeconds()).isEqualTo(3600);
    assertThat(properties.isFileSystemCacheEnabled()).isFalse();
    assertThat(properties.getFileSystemCacheBasePath()).isEqualTo(tempDir.toString());
    assertThat(properties.getFileSystemCacheMaxTotalBytes()).isEqualTo(2097152);
    assertThat(properties.getFileSystemCacheExpirationIntervalInSeconds()).isEqualTo(86400);
  }

  @Test
  void testGetMetricCollectorWithNoProvider() {
    SwiftLakeFileIOProperties properties = new SwiftLakeFileIOProperties();
    assertThat(properties.getMetricCollector(new HashMap<>())).isNull();
  }

  @Test
  void testGetMetricCollectorWithProvider() {
    Map<String, String> props = new HashMap<>();
    props.put(
        SwiftLakeFileIOProperties.METRIC_COLLECTOR_PROVIDER,
        TestMetricCollectorProvider.class.getName());
    SwiftLakeFileIOProperties properties = new SwiftLakeFileIOProperties(props);

    MetricCollector collector = properties.getMetricCollector(new HashMap<>());
    assertThat(collector).isInstanceOf(TestMetricCollector.class);
  }

  @Test
  void testGetCacheFileIOWithNoProvider() {
    Map<String, String> props = new HashMap<>();
    props.put(SwiftLakeFileIOProperties.CACHE_FILE_IO_PROVIDER, "");
    SwiftLakeFileIOProperties properties = new SwiftLakeFileIOProperties(props);
    assertThat(properties.getCacheFileIO(new HashMap<>())).isNull();
  }

  @Test
  void testGetCacheFileIOWithProvider() {
    Map<String, String> props = new HashMap<>();
    props.put(
        SwiftLakeFileIOProperties.CACHE_FILE_IO_PROVIDER, TestCacheFileIOProvider.class.getName());
    SwiftLakeFileIOProperties properties = new SwiftLakeFileIOProperties(props);

    CacheFileIO cacheFileIO = properties.getCacheFileIO(new HashMap<>());
    assertThat(cacheFileIO).isInstanceOf(TestCacheFileIO.class);
  }

  @Test
  void testNewInstanceWithInvalidClass() {
    Map<String, String> props = new HashMap<>();
    props.put(SwiftLakeFileIOProperties.METRIC_COLLECTOR_PROVIDER, "com.example.NonExistentClass");
    SwiftLakeFileIOProperties properties = new SwiftLakeFileIOProperties(props);

    assertThatThrownBy(() -> properties.getMetricCollector(new HashMap<>()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot initialize");
  }

  @Test
  void testNewInstanceWithInvalidImplementation() {
    Map<String, String> props = new HashMap<>();
    props.put(SwiftLakeFileIOProperties.METRIC_COLLECTOR_PROVIDER, String.class.getName());
    SwiftLakeFileIOProperties properties = new SwiftLakeFileIOProperties(props);

    assertThatThrownBy(() -> properties.getMetricCollector(new HashMap<>()))
        .isInstanceOf(ClassCastException.class)
        .hasMessageContaining(
            "class java.lang.String cannot be cast to class com.arcesium.swiftlake.metrics.MetricCollectorProvider");
  }

  // Test implementation classes
  public static class TestMetricCollectorProvider implements MetricCollectorProvider {
    @Override
    public MetricCollector getMetricCollector(Map<String, String> properties) {
      return new TestMetricCollector();
    }
  }

  public static class TestMetricCollector implements MetricCollector {
    @Override
    public void collectMetrics(Metrics metrics) {}
  }

  public static class TestCacheFileIOProvider implements CacheFileIOProvider {
    @Override
    public CacheFileIO getCacheFileIO(Map<String, String> properties) {
      return new TestCacheFileIO();
    }
  }

  public static class TestCacheFileIO implements CacheFileIO {
    @Override
    public void initialize(Map<String, String> properties) {}

    @Override
    public InputFile newInputFile(SwiftLakeFileIO fileIO, String path) {
      return null;
    }

    @Override
    public InputFile newInputFile(SwiftLakeFileIO fileIO, String path, long length) {
      return null;
    }

    @Override
    public InputFile newInputFile(SwiftLakeFileIO fileIO, String path, boolean useFileSystemCache) {
      return null;
    }

    @Override
    public InputFile newInputFile(
        SwiftLakeFileIO fileIO, String path, long length, boolean useFileSystemCache) {
      return null;
    }

    @Override
    public InputFiles newInputFiles(SwiftLakeFileIO fileIO, List<String> paths) {
      return null;
    }

    @Override
    public FileSystemCacheRuntimeMetrics getFileSystemCacheRuntimeMetrics() {
      return null;
    }

    @Override
    public ContentCacheRuntimeMetrics getContentCacheRuntimeMetrics() {
      return null;
    }
  }
}
