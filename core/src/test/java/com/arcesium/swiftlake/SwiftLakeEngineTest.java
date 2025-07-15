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
package com.arcesium.swiftlake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.common.FileUtil;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.dao.CommonDao;
import com.arcesium.swiftlake.metrics.DuckDBRuntimeMetrics;
import com.arcesium.swiftlake.metrics.MetricCollector;
import com.arcesium.swiftlake.metrics.PartitionData;
import com.arcesium.swiftlake.sql.IcebergScanExecutor;
import com.arcesium.swiftlake.sql.SwiftLakeConnection;
import com.arcesium.swiftlake.sql.SwiftLakeDataSource;
import com.sun.management.OperatingSystemMXBean;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.io.Resources;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

class SwiftLakeEngineTest {

  @Mock private Catalog mockCatalog;
  @Mock private Table mockTable;
  @Mock private MetricCollector mockMetricCollector;
  @Mock private CommonDao mockCommonDao;
  @Mock private IcebergScanExecutor mockIcebergScanExecutor;
  @TempDir Path tempDir;

  private SwiftLakeEngine engine;
  private SwiftLakeEngine engineWithCachingCatalog;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    engine =
        SwiftLakeEngine.builderFor("testApp")
            .catalog(mockCatalog)
            .localDir(tempDir.toString())
            .memoryLimitInMiB(1024)
            .threads(4)
            .tempStorageLimitInMiB(2048)
            .maxPartitionWriterThreads(8)
            .totalFileSizePerScanLimitInMiB(10240)
            .maxActiveConnections(100)
            .connectionCreationTimeoutInSeconds(30)
            .queryTimeoutInSeconds(60)
            .enableDebugFileUpload("/debug/upload/path")
            .metricCollector(mockMetricCollector)
            .configureDuckDBExtensions(true, true)
            .lockDuckDBConfiguration(false)
            .processTablesDefaultValue(true)
            .allowFullTableScan(false)
            .build();

    try {
      var field = SwiftLakeEngine.class.getDeclaredField("commonDao");
      field.setAccessible(true);
      field.set(engine, mockCommonDao);
      field = SwiftLakeEngine.class.getDeclaredField("icebergScanExecutor");
      field.setAccessible(true);
      field.set(engine, mockIcebergScanExecutor);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    engineWithCachingCatalog =
        SwiftLakeEngine.builderFor("testApp")
            .catalog(mockCatalog)
            .cachingCatalog(
                Arrays.asList("db2", "ns1.ns2", "db3.schema1"),
                Arrays.asList("db1.schema1.table1", "db1.schema2.table2"),
                3)
            .build();
    when(mockCatalog.loadTable(any(TableIdentifier.class))).thenReturn(mockTable);
  }

  @Test
  void testBuilderWithFullConfiguration() {
    assertThat(engine).isNotNull();
    assertThat(engine.getCatalog()).isEqualTo(mockCatalog);
    assertThat(engine.getLocalDir()).isEqualTo(tempDir.toString());
    assertThat(engine.getMemoryLimitInMiB()).isEqualTo(1024);
    assertThat(engine.getThreads()).isEqualTo(4);
    assertThat(engine.getQueryTimeoutInSeconds()).isEqualTo(60);
    assertThat(engine.getDebugFileUploadPath()).isEqualTo("/debug/upload/path");
    assertThat(engine.getMetricCollector()).isEqualTo(mockMetricCollector);
    assertThat(engine.getProcessTablesDefaultValue()).isTrue();
  }

  @Test
  void testBuilderWithInvalidConfiguration() {
    assertThatThrownBy(
            () ->
                SwiftLakeEngine.builderFor("testApp")
                    .catalog(mockCatalog)
                    .memoryLimitInMiB(-1)
                    .build())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Memory limit value must be greater than 0");
  }

  @ParameterizedTest
  @ValueSource(strings = {"db.schema.table", "schema.table", "table"})
  void testGetTableIdentifier(String tableName) {
    TableIdentifier identifier = engine.getTableIdentifier(tableName);
    assertThat(identifier.toString()).isEqualTo(tableName);
  }

  @Test
  void testGetTable() {
    String tableName = "db.schema.table";
    when(mockCatalog.loadTable(any(TableIdentifier.class))).thenReturn(mockTable);

    Table table = engine.getTable(tableName);

    assertThat(table).isNotNull().isSameAs(mockTable);
    verify(mockCatalog).loadTable(TableIdentifier.parse(tableName));
  }

  @Test
  void testGetTableWithRefresh() {
    String tableName = "db.schema.table";
    when(mockCatalog.loadTable(any(TableIdentifier.class))).thenReturn(mockTable);

    Table table = engine.getTable(tableName, true);

    assertThat(table).isNotNull().isSameAs(mockTable);
    verify(mockCatalog, times(2)).loadTable(TableIdentifier.parse(tableName));
  }

  @Test
  void testCreateConnection() throws SQLException {
    SwiftLakeConnection connection = engine.createConnection();
    assertThat(connection).isNotNull();
  }

  @Test
  void testCreateDataSource() {
    DataSource dataSource = engine.createDataSource();
    assertThat(dataSource).isNotNull().isInstanceOf(SwiftLakeDataSource.class);
  }

  @Test
  void testCreateSqlSessionFactory() {
    try (MockedStatic<Resources> mockedResources = mockStatic(Resources.class)) {
      InputStream mockInputStream =
          new ByteArrayInputStream(
              "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<!DOCTYPE configuration PUBLIC \"-//mybatis.org//DTD Config 3.0//EN\" \"https://mybatis.org/dtd/mybatis-3-config.dtd\">\n<configuration>\n</configuration>"
                  .getBytes());
      mockedResources
          .when(() -> Resources.getResourceAsStream("test-config.xml"))
          .thenReturn(mockInputStream);

      assertThat(engine.createSqlSessionFactory("test-config.xml")).isNotNull();
    }
  }

  @Test
  void testCreateMybatisConfiguration() {
    assertThat(engine.createMybatisConfiguration()).isNotNull();
  }

  @Test
  void testGetDuckDBRuntimeMetrics() {
    DuckDBRuntimeMetrics metrics =
        new DuckDBRuntimeMetrics(
            1024L * 1000 * 1000, 512L * 1024 * 1024, 2048L * 1000 * 1000 * 1000, 1024L);
    when(mockCommonDao.getDuckDBRuntimeMetrics())
        .thenReturn(
            Collections.singletonList(
                Map.of(
                    "memory_limit",
                    "1024MB",
                    "memory_usage",
                    "512MiB",
                    "max_temp_directory_size",
                    "2048GB",
                    "temp_files_size",
                    1024L)));

    DuckDBRuntimeMetrics result = engine.getDuckDBRuntimeMetrics();

    assertThat(result).isNotNull();
    assertThat(result.getMemoryLimitInBytes()).isEqualTo(metrics.getMemoryLimitInBytes());
    assertThat(result.getMemoryUsageInBytes()).isEqualTo(metrics.getMemoryUsageInBytes());
    assertThat(result.getTempStorageLimitInBytes()).isEqualTo(metrics.getTempStorageLimitInBytes());
    assertThat(result.getTempStorageUsageInBytes()).isEqualTo(metrics.getTempStorageUsageInBytes());
  }

  @Test
  void testGetPartitionLevelRecordCounts() {
    String tableName = "testTable";
    List<PartitionData> partitionDataList = new ArrayList<>();
    when(mockCatalog.loadTable(any(TableIdentifier.class))).thenReturn(mockTable);
    when(mockIcebergScanExecutor.getPartitionLevelRecordCounts(any(Table.class), anyList()))
        .thenReturn(Collections.singletonList(Pair.of(mock(PartitionData.class), 100L)));

    List<Pair<PartitionData, Long>> result =
        engine.getPartitionLevelRecordCounts(tableName, partitionDataList);

    assertThat(result).isNotNull().hasSize(1);
    verify(mockCatalog).loadTable(TableIdentifier.parse(tableName));
    verify(mockIcebergScanExecutor).getPartitionLevelRecordCounts(mockTable, partitionDataList);
  }

  @Test
  void testInsertInto() {
    assertThat(engine.insertInto("testTable")).isNotNull();
  }

  @Test
  void testInsertOverwrite() {
    assertThat(engine.insertOverwrite("testTable")).isNotNull();
  }

  @Test
  void testUpdate() {
    assertThat(engine.update("testTable")).isNotNull();
  }

  @Test
  void testDeleteFrom() {
    assertThat(engine.deleteFrom("testTable")).isNotNull();
  }

  @Test
  void testApplyChangesAsSCD1() {
    assertThat(engine.applyChangesAsSCD1("testTable")).isNotNull();
  }

  @Test
  void testApplyChangesAsSCD2() {
    assertThat(engine.applyChangesAsSCD2("testTable")).isNotNull();
  }

  @Test
  void testApplySnapshotAsSCD2() {
    assertThat(engine.applySnapshotAsSCD2("testTable")).isNotNull();
  }

  @Test
  void testClose() throws Exception {
    engine.close();
  }

  @Test
  void testLoadDuckDBExtensionsFromClassPath() throws Exception {
    Statement mockStmt = mock(Statement.class);
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockStmt.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.getString(eq(1))).thenReturn("platform");
    when(mockStmt.execute(anyString())).thenReturn(false);

    List<String> extensions = Arrays.asList("extension1", "extension2");
    engine.loadDuckDBExtensionsFromClassPath(mockStmt, "/duckdb_extensions", extensions);

    verify(mockStmt).execute(matches("LOAD '.*/platform/extension1.duckdb_extension'"));
    verify(mockStmt).execute(matches("LOAD '.*/platform/extension2.duckdb_extension'"));
  }

  @Test
  void testLoadDuckDBExtensions() throws Exception {
    String platform = "os1";
    Path folder = tempDir.resolve("extensions");
    new File(folder.toString()).mkdirs();
    Path platformFolder = folder.resolve(platform);
    new File(platformFolder.toString()).mkdirs();
    File extFile1 = new File(platformFolder.toString(), "extension1.duckdb_extension");
    assertThat(extFile1.createNewFile()).isTrue();
    TestUtil.compressGzipFile(extFile1.toString(), extFile1 + ".gz");
    assertThat(extFile1.delete()).isTrue();
    File extFile2 = new File(platformFolder.toString(), "extension2.duckdb_extension");
    assertThat(extFile2.createNewFile()).isTrue();

    Statement mockStmt = mock(Statement.class);
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockStmt.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.getString(eq(1))).thenReturn("os1");
    when(mockStmt.execute(anyString())).thenReturn(false);
    List<String> extensions = Arrays.asList("extension1", "extension2");

    engine.loadDuckDBExtensions(mockStmt, folder.toString(), extensions);

    verify(mockStmt).execute(eq(String.format("LOAD '%s';", extFile1)));
    verify(mockStmt).execute(eq(String.format("LOAD '%s';", extFile2)));
  }

  @Test
  void testLoadInvalidDuckDBExtensionsFromClassPath() throws SQLException {
    Statement mockStmt = mock(Statement.class);
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockStmt.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.getString(eq(1))).thenReturn("os1");
    List<String> extensions = Arrays.asList("extension1", "extension2");

    assertThatThrownBy(
            () -> engine.loadDuckDBExtensionsFromClassPath(mockStmt, "/extensions", extensions))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining(
            "Unable to find the extension file /extensions/os1/extension1.duckdb_extension");
  }

  @Test
  void testLoadInvalidDuckDBExtensions() throws SQLException {
    Statement mockStmt = mock(Statement.class);
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockStmt.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.getString(eq(1))).thenReturn("os1");

    List<String> extensions = Arrays.asList("extension1", "extension2");
    assertThatThrownBy(() -> engine.loadDuckDBExtensions(mockStmt, "/extensions", extensions))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining(
            "Unable to find the extension file /extensions/os1/extension1.duckdb_extension");
  }

  @Test
  void testCachedNamespaces() {
    // Test cached namespaces
    engineWithCachingCatalog.getTable("db2.table1");
    engineWithCachingCatalog.getTable("db2.table1");
    engineWithCachingCatalog.getTable("ns1.ns2.table1");
    engineWithCachingCatalog.getTable("ns1.ns2.table1");
    engineWithCachingCatalog.getTable("ns1.ns2.table2");
    engineWithCachingCatalog.getTable("ns1.ns2.table2");
    engineWithCachingCatalog.getTable("db3.schema1.table1");
    engineWithCachingCatalog.getTable("db3.schema1.table1");

    // Verify that loadTable was called only once for each namespace
    verify(mockCatalog, times(1)).loadTable(TableIdentifier.parse("db2.table1"));
    verify(mockCatalog, times(1)).loadTable(TableIdentifier.parse("ns1.ns2.table1"));
    verify(mockCatalog, times(1)).loadTable(TableIdentifier.parse("ns1.ns2.table2"));
    verify(mockCatalog, times(1)).loadTable(TableIdentifier.parse("db3.schema1.table1"));
  }

  @Test
  void testCachedTables() {
    // Test cached tables
    engineWithCachingCatalog.getTable("db1.schema1.table1");
    engineWithCachingCatalog.getTable("db1.schema1.table1");
    engineWithCachingCatalog.getTable("db1.schema2.table2");
    engineWithCachingCatalog.getTable("db1.schema2.table2");

    // Verify that loadTable was called only once for each table
    verify(mockCatalog, times(1)).loadTable(TableIdentifier.parse("db1.schema1.table1"));
    verify(mockCatalog, times(1)).loadTable(TableIdentifier.parse("db1.schema2.table2"));
  }

  @Test
  void testNonCachedTables() {
    // Test non-cached tables
    engineWithCachingCatalog.getTable("db1.schema1.table3");
    engineWithCachingCatalog.getTable("db1.schema1.table3");
    engineWithCachingCatalog.getTable("db4.schema1.table1");
    engineWithCachingCatalog.getTable("db4.schema1.table1");

    // Verify that loadTable was called for each getTable call
    verify(mockCatalog, times(2)).loadTable(TableIdentifier.parse("db1.schema1.table3"));
    verify(mockCatalog, times(2)).loadTable(TableIdentifier.parse("db4.schema1.table1"));
  }

  @Test
  void testCacheExpiration() throws InterruptedException {
    // Test cache expiration
    engineWithCachingCatalog.getTable("db2.schema1.table1");
    engineWithCachingCatalog.getTable("db1.schema1.table1");

    // Wait for cache to expire
    TimeUnit.SECONDS.sleep(5);

    engineWithCachingCatalog.getTable("db2.schema1.table1");
    engineWithCachingCatalog.getTable("db1.schema1.table1");

    // Verify that loadTable was called twice for each table (once before and once after expiration)
    verify(mockCatalog, times(2)).loadTable(TableIdentifier.parse("db2.schema1.table1"));
    verify(mockCatalog, times(2)).loadTable(TableIdentifier.parse("db1.schema1.table1"));
  }

  @Test
  void testMixedCachingScenarios() {
    // Test a mix of cached namespaces, cached tables, and non-cached tables
    engineWithCachingCatalog.getTable("db2.table1"); // Cached namespace
    engineWithCachingCatalog.getTable("ns1.ns2.table3"); // Cached namespace
    engineWithCachingCatalog.getTable("db1.schema1.table1"); // Cached table
    engineWithCachingCatalog.getTable("db4.schema1.table1"); // Non-cached
    engineWithCachingCatalog.getTable("db3.schema1.table2"); // Cached namespace
    engineWithCachingCatalog.getTable("db1.schema2.table3"); // Non-cached

    // Repeat calls
    engineWithCachingCatalog.getTable("db2.table1"); // Cached namespace
    engineWithCachingCatalog.getTable("ns1.ns2.table3"); // Cached namespace
    engineWithCachingCatalog.getTable("db1.schema1.table1"); // Cached table
    engineWithCachingCatalog.getTable("db4.schema1.table1"); // Non-cached
    engineWithCachingCatalog.getTable("db3.schema1.table2"); // Cached namespace
    engineWithCachingCatalog.getTable("db1.schema2.table3"); // Non-cached

    // Verify correct number of calls to loadTable
    verify(mockCatalog, times(1)).loadTable(TableIdentifier.parse("db2.table1"));
    verify(mockCatalog, times(1)).loadTable(TableIdentifier.parse("ns1.ns2.table3"));
    verify(mockCatalog, times(1)).loadTable(TableIdentifier.parse("db1.schema1.table1"));
    verify(mockCatalog, times(2)).loadTable(TableIdentifier.parse("db4.schema1.table1"));
    verify(mockCatalog, times(1)).loadTable(TableIdentifier.parse("db3.schema1.table2"));
    verify(mockCatalog, times(2)).loadTable(TableIdentifier.parse("db1.schema2.table3"));
  }

  @Test
  void testMemoryLimitWithMocking() {
    try (MockedStatic<ManagementFactory> managementFactoryMock =
        mockStatic(ManagementFactory.class); ) {

      // Mock OS MXBean with 16GB physical memory
      OperatingSystemMXBean mockOSBean = mock(OperatingSystemMXBean.class);
      when(mockOSBean.getTotalMemorySize()).thenReturn(16L * 1024 * 1024 * 1024); // 16 GB
      managementFactoryMock
          .when(() -> ManagementFactory.getOperatingSystemMXBean())
          .thenReturn(mockOSBean);

      // Test default behavior (no explicit settings)
      try (SwiftLakeEngine defaultEngine =
          SwiftLakeEngine.builderFor("test-app").catalog(mockCatalog).build()) {
        assertThat(defaultEngine.getMemoryLimitInMiB()).isNotNull();
        assertThat(defaultEngine.getMemoryLimitInMiB()).isGreaterThan(0);
        OperatingSystemMXBean osMxBean =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        long physicalMemoryMiB = osMxBean.getTotalMemorySize() / FileUtil.MIB_FACTOR;
        // Memory limit should be a fraction of physical memory
        assertThat(defaultEngine.getMemoryLimitInMiB()).isLessThan((int) physicalMemoryMiB);
      }

      // Test with memory fraction
      try (SwiftLakeEngine fractionEngine =
          SwiftLakeEngine.builderFor("test-app")
              .catalog(mockCatalog)
              .memoryLimitFraction(0.5)
              .build()) {
        // Calculate expected: 16GB * 0.5 / MiB_FACTOR
        int expectedMemory =
            ((Double) (16L * 1024 * 1024 * 1024 * 0.5 / FileUtil.MIB_FACTOR)).intValue();

        assertThat(fractionEngine.getMemoryLimitInMiB()).isEqualTo(expectedMemory);
      }

      // Test with explicit memory limit
      try (SwiftLakeEngine explicitEngine =
          SwiftLakeEngine.builderFor("test-app")
              .catalog(mockCatalog)
              .memoryLimitInMiB(500)
              .build()) {
        assertThat(explicitEngine.getMemoryLimitInMiB()).isEqualTo(500);
      }
    }
  }
}
