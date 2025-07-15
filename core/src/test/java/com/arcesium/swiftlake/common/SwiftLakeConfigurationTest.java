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

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.metrics.MetricCollector;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SwiftLakeConfigurationTest {

  private SwiftLakeConfiguration config;

  @Mock private Catalog mockCatalog;

  @Mock private MetricCollector mockMetricCollector;

  @BeforeEach
  void setUp() {
    config = new SwiftLakeConfiguration();
  }

  @Test
  void testDefaultValues() {
    assertThat(config.getApplicationId()).isNull();
    assertThat(config.getCatalog()).isNull();
    assertThat(config.getThreads()).isNull();
    assertThat(config.getMemoryLimitInMiB()).isNull();
    assertThat(config.getMemoryLimitFraction()).isNull();
    assertThat(config.getTempStorageLimitInMiB()).isNull();
    assertThat(config.getLocalDir()).isNull();
    assertThat(config.getCachingCatalogNamespaces()).isNull();
    assertThat(config.getCachingCatalogTableNames()).isNull();
    assertThat(config.getCachingCatalogExpirationIntervalInSeconds()).isEqualTo(0);
    assertThat(config.getMybatisConfigPath()).isNull();
    assertThat(config.getMaxPartitionWriterThreads()).isNull();
    assertThat(config.isAllowCommunityExtensions()).isFalse();
    assertThat(config.isAutoInstallKnownExtensions()).isFalse();
    assertThat(config.isLockDuckDBConfiguration()).isFalse();
    assertThat(config.getTotalFileSizePerScanLimitInMiB()).isNull();
    assertThat(config.getQueryTimeoutInSeconds()).isNull();
    assertThat(config.getMaxActiveConnections()).isNull();
    assertThat(config.getConnectionCreationTimeoutInSeconds()).isNull();
    assertThat(config.getDebugFileUploadPath()).isNull();
    assertThat(config.getMetricCollector()).isNull();
    assertThat(config.getProcessTablesDefaultValue()).isFalse();
    assertThat(config.isAllowFullTableScan()).isFalse();
    assertThat(config.getConfigureFunc()).isNull();
  }

  @Test
  void testApplicationId() {
    config.setApplicationId("testApp");
    assertThat(config.getApplicationId()).isEqualTo("testApp");
  }

  @Test
  void testApplicationIdWithNull() {
    config.setApplicationId(null);
    assertThat(config.getApplicationId()).isNull();
  }

  @Test
  void testCatalog() {
    config.setCatalog(mockCatalog);
    assertThat(config.getCatalog()).isEqualTo(mockCatalog);
  }

  @Test
  void testCatalogWithNull() {
    config.setCatalog(null);
    assertThat(config.getCatalog()).isNull();
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 1, 100})
  void testThreads(int threads) {
    config.setThreads(threads);
    assertThat(config.getThreads()).isEqualTo(threads);
  }

  @Test
  void testThreadsWithNull() {
    config.setThreads(null);
    assertThat(config.getThreads()).isNull();
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 1, 1024})
  void testMemoryLimitInMiB(int limit) {
    config.setMemoryLimitInMiB(limit);
    assertThat(config.getMemoryLimitInMiB()).isEqualTo(limit);
  }

  @ParameterizedTest
  @ValueSource(doubles = {-0.1, 0.0, 0.1, 0.5, 1.0, 1.1})
  void testMemoryLimitFraction(double fraction) {
    config.setMemoryLimitFraction(fraction);
    assertThat(config.getMemoryLimitFraction()).isEqualTo(fraction);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 1, 2048})
  void testTempStorageLimitInMiB(int limit) {
    config.setTempStorageLimitInMiB(limit);
    assertThat(config.getTempStorageLimitInMiB()).isEqualTo(limit);
  }

  @Test
  void testLocalDir() {
    config.setLocalDir("/tmp/swiftlake");
    assertThat(config.getLocalDir()).isEqualTo("/tmp/swiftlake");
  }

  @Test
  void testCachingCatalogNamespaces() {
    Set<String> namespaces = new HashSet<>(Arrays.asList("ns1", "ns2"));
    config.setCachingCatalogNamespaces(namespaces);
    assertThat(config.getCachingCatalogNamespaces())
        .containsExactlyInAnyOrderElementsOf(namespaces);
  }

  @Test
  void testCachingCatalogNamespacesWithNull() {
    config.setCachingCatalogNamespaces(null);
    assertThat(config.getCachingCatalogNamespaces()).isNull();
  }

  @Test
  void testCachingCatalogTableNames() {
    Set<String> tableNames = new HashSet<>(Arrays.asList("table1", "table2"));
    config.setCachingCatalogTableNames(tableNames);
    assertThat(config.getCachingCatalogTableNames())
        .containsExactlyInAnyOrderElementsOf(tableNames);
  }

  @Test
  void testCachingCatalogTableNamesWithNull() {
    config.setCachingCatalogTableNames(null);
    assertThat(config.getCachingCatalogTableNames()).isNull();
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 1, 3600})
  void testCachingCatalogExpirationIntervalInSeconds(int interval) {
    config.setCachingCatalogExpirationIntervalInSeconds(interval);
    assertThat(config.getCachingCatalogExpirationIntervalInSeconds()).isEqualTo(interval);
  }

  @Test
  void testMybatisConfigPath() {
    config.setMybatisConfigPath("/path/to/mybatis-config.xml");
    assertThat(config.getMybatisConfigPath()).isEqualTo("/path/to/mybatis-config.xml");
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 1, 4})
  void testMaxPartitionWriterThreads(int threads) {
    config.setMaxPartitionWriterThreads(threads);
    assertThat(config.getMaxPartitionWriterThreads()).isEqualTo(threads);
  }

  @Test
  void testAllowCommunityExtensions() {
    config.setAllowCommunityExtensions(true);
    assertThat(config.isAllowCommunityExtensions()).isTrue();
    config.setAllowCommunityExtensions(false);
    assertThat(config.isAllowCommunityExtensions()).isFalse();
  }

  @Test
  void testAutoInstallKnownExtensions() {
    config.setAutoInstallKnownExtensions(true);
    assertThat(config.isAutoInstallKnownExtensions()).isTrue();
    config.setAutoInstallKnownExtensions(false);
    assertThat(config.isAutoInstallKnownExtensions()).isFalse();
  }

  @Test
  void testLockDuckDBConfiguration() {
    config.setLockDuckDBConfiguration(true);
    assertThat(config.isLockDuckDBConfiguration()).isTrue();
    config.setLockDuckDBConfiguration(false);
    assertThat(config.isLockDuckDBConfiguration()).isFalse();
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 1, 5000})
  void testTotalFileSizePerScanLimitInMiB(int limit) {
    config.setTotalFileSizePerScanLimitInMiB(limit);
    assertThat(config.getTotalFileSizePerScanLimitInMiB()).isEqualTo(limit);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 1, 30})
  void testQueryTimeoutInSeconds(int timeout) {
    config.setQueryTimeoutInSeconds(timeout);
    assertThat(config.getQueryTimeoutInSeconds()).isEqualTo(timeout);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 1, 100})
  void testMaxActiveConnections(int connections) {
    config.setMaxActiveConnections(connections);
    assertThat(config.getMaxActiveConnections()).isEqualTo(connections);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 1, 5})
  void testConnectionCreationTimeoutInSeconds(int timeout) {
    config.setConnectionCreationTimeoutInSeconds(timeout);
    assertThat(config.getConnectionCreationTimeoutInSeconds()).isEqualTo(timeout);
  }

  @Test
  void testDebugFileUploadPath() {
    config.setDebugFileUploadPath("/path/to/debug/files");
    assertThat(config.getDebugFileUploadPath()).isEqualTo("/path/to/debug/files");
  }

  @Test
  void testMetricCollector() {
    config.setMetricCollector(mockMetricCollector);
    assertThat(config.getMetricCollector()).isEqualTo(mockMetricCollector);
  }

  @Test
  void testProcessTablesDefaultValue() {
    config.setProcessTablesDefaultValue(true);
    assertThat(config.getProcessTablesDefaultValue()).isTrue();
    config.setProcessTablesDefaultValue(false);
    assertThat(config.getProcessTablesDefaultValue()).isFalse();
  }

  @Test
  void testAllowFullTableScan() {
    config.setAllowFullTableScan(true);
    assertThat(config.isAllowFullTableScan()).isTrue();
    config.setAllowFullTableScan(false);
    assertThat(config.isAllowFullTableScan()).isFalse();
  }

  @Test
  void testConfigureFunc() {
    Function<SwiftLakeEngine, List<AutoCloseable>> configureFunc =
        engine -> Arrays.asList(() -> {});
    config.setConfigureFunc(configureFunc);
    assertThat(config.getConfigureFunc()).isEqualTo(configureFunc);
  }

  @Test
  void testConfigureFuncWithNull() {
    config.setConfigureFunc(null);
    assertThat(config.getConfigureFunc()).isNull();
  }
}
