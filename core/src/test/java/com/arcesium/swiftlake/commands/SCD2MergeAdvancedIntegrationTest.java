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
package com.arcesium.swiftlake.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.TestUtil;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import com.arcesium.swiftlake.writer.TableBatchTransaction;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SCD2MergeAdvancedIntegrationTest {
  private static SwiftLakeEngine swiftLakeEngine;
  @TempDir private static Path basePath;

  @BeforeAll
  public static void setup() {
    swiftLakeEngine = TestUtil.createSwiftLakeEngine(basePath.toString());
  }

  @AfterAll
  public static void teardown() {
    swiftLakeEngine.close();
    try {
      FileUtils.deleteDirectory(new File(basePath.toString()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testProcessSourceTables(boolean isSnapshotMode) {
    TableIdentifier sourceTableId =
        TableIdentifier.of(
            "test_db", "scd2_source_table_" + UUID.randomUUID().toString().replace('-', '_'));
    String sourceTableName = sourceTableId.toString();
    TableIdentifier targetTableId =
        TableIdentifier.of(
            "test_db", "scd2_target_table_" + UUID.randomUUID().toString().replace('-', '_'));
    String targetTableName = targetTableId.toString();

    Schema sourceSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()));
    Schema targetSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()),
            Types.NestedField.required(4, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(5, "effective_end", Types.TimestampType.withoutZone()));

    // Create source and target tables
    swiftLakeEngine
        .getCatalog()
        .createTable(sourceTableId, sourceSchema, PartitionSpec.unpartitioned());
    swiftLakeEngine
        .getCatalog()
        .createTable(targetTableId, targetSchema, PartitionSpec.unpartitioned());

    // Insert initial data into target table
    List<Map<String, Object>> targetData =
        Arrays.asList(
            Map.of("id", 1L, "name", "Initial", "value", 100.0),
            Map.of("id", 2L, "name", "Initial", "value", 200.0));

    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, targetTableName, targetSchema, targetData, "2025-01-01T00:00:00");

    // Insert data into source table
    List<Map<String, Object>> sourceData = null;
    if (isSnapshotMode) {
      sourceData =
          Arrays.asList(
              Map.of("id", 1L, "name", "Updated", "value", 150.0),
              Map.of("id", 3L, "name", "New", "value", 300.0),
              Map.of("id", 2L, "name", "Initial", "value", 200.0));
    } else {
      sourceData =
          Arrays.asList(
              Map.of("id", 1L, "name", "Updated", "value", 150.0),
              Map.of("id", 3L, "name", "New", "value", 300.0));
    }

    swiftLakeEngine
        .insertInto(sourceTableName)
        .sql(TestUtil.createSelectSql(sourceData, sourceSchema))
        .processSourceTables(false)
        .execute();

    LocalDateTime effectiveTimestamp = LocalDateTime.parse("2025-01-02T00:00:00");

    if (isSnapshotMode) {
      // Apply SCD2 snapshot with processSourceTables=true
      swiftLakeEngine
          .applySnapshotAsSCD2(targetTableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql("SELECT * FROM " + sourceTableName)
          .effectiveTimestamp(effectiveTimestamp)
          .keyColumns(Arrays.asList("id"))
          .effectivePeriodColumns("effective_start", "effective_end")
          .processSourceTables(true)
          .execute();
    } else {
      // Apply SCD2 changes with processSourceTables=true
      swiftLakeEngine
          .applyChangesAsSCD2(targetTableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(
              "SELECT *, 'U' as operation_type FROM "
                  + sourceTableName
                  + " WHERE id = 1 UNION ALL SELECT *, 'I' as operation_type FROM "
                  + sourceTableName
                  + " WHERE id = 3")
          .effectiveTimestamp(effectiveTimestamp)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .effectivePeriodColumns("effective_start", "effective_end")
          .processSourceTables(true)
          .execute();
    }

    // Verify the changes were applied correctly
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "Initial",
                "value",
                100.0,
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            new HashMap<>() {
              {
                put("id", 1L);
                put("name", "Updated");
                put("value", 150.0);
                put("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
                put("effective_end", null);
              }
            },
            new HashMap<>() {
              {
                put("id", 2L);
                put("name", "Initial");
                put("value", 200.0);
                put("effective_start", LocalDateTime.parse("2025-01-01T00:00:00"));
                put("effective_end", null);
              }
            },
            new HashMap<>() {
              {
                put("id", 3L);
                put("name", "New");
                put("value", 300.0);
                put("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
                put("effective_end", null);
              }
            });

    List<Map<String, Object>> actualData =
        TestUtil.getRecordsFromTable(swiftLakeEngine, targetTableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, sourceTableName);
    TestUtil.dropIcebergTable(swiftLakeEngine, targetTableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testExecuteSqlOnceOnly(boolean isSnapshotMode) {
    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd2_execute_sql_once_test_"
                + (isSnapshotMode ? "snapshot_" : "changes_")
                + UUID.randomUUID().toString().replace('-', '_'));
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));
    Table table =
        swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    // Insert initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "value", "initial-value-1"),
            Map.of("id", 2L, "value", "initial-value-2"),
            Map.of("id", 3L, "value", "initial-value-3"));

    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00");

    List<Map<String, Object>> changes = null;
    if (isSnapshotMode) {
      changes =
          Arrays.asList(
              Map.of("id", 1L, "value", "updated-value-1"),
              Map.of("id", 4L, "value", "new-value-4"),
              Map.of("id", 2L, "value", "initial-value-2"));
    } else {
      changes =
          Arrays.asList(
              Map.of("id", 1L, "value", "updated-value-1", "operation_type", "U"),
              Map.of("id", 3L, "value", "to-be-deleted", "operation_type", "D"),
              Map.of("id", 4L, "value", "new-value-4", "operation_type", "I"));
    }

    LocalDateTime effectiveTime = LocalDateTime.parse("2025-01-02T00:00:00");

    if (isSnapshotMode) {
      swiftLakeEngine
          .applySnapshotAsSCD2(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(TestUtil.createSnapshotSql(changes, schema))
          .effectiveTimestamp(effectiveTime)
          .keyColumns(Arrays.asList("id"))
          .effectivePeriodColumns("effective_start", "effective_end")
          .executeSourceSqlOnceOnly(true)
          .processSourceTables(false)
          .execute();
    } else {
      swiftLakeEngine
          .applyChangesAsSCD2(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(TestUtil.createChangeSql(changes, schema))
          .effectiveTimestamp(effectiveTime)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .effectivePeriodColumns("effective_start", "effective_end")
          .executeSourceSqlOnceOnly(true)
          .processSourceTables(false)
          .execute();
    }

    // Verify results
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "value",
                "initial-value-1",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            new HashMap<>() {
              {
                put("id", 1L);
                put("value", "updated-value-1");
                put("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
                put("effective_end", null);
              }
            },
            new HashMap<>() {
              {
                put("id", 2L);
                put("value", "initial-value-2");
                put("effective_start", LocalDateTime.parse("2025-01-01T00:00:00"));
                put("effective_end", null);
              }
            },
            new HashMap<>() {
              {
                put("id", 3L);
                put("value", "initial-value-3");
                put("effective_start", LocalDateTime.parse("2025-01-01T00:00:00"));
                put("effective_end", LocalDateTime.parse("2025-01-02T00:00:00"));
              }
            },
            new HashMap<>() {
              {
                put("id", 4L);
                put("value", "new-value-4");
                put("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
                put("effective_end", null);
              }
            });

    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSourceMybatisStatement(boolean isSnapshotMode) throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()),
            Types.NestedField.required(4, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(5, "effective_end", Types.TimestampType.withoutZone()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd2_mybatis_test_"
                + (isSnapshotMode ? "snapshot_" : "changes_")
                + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    // Insert initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "Initial", "value", 100.0),
            Map.of("id", 2L, "name", "Initial", "value", 200.0));

    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00");

    // Create MyBatis session factory with a test mapper
    SwiftLakeSqlSessionFactory sqlSessionFactory =
        (SwiftLakeSqlSessionFactory)
            swiftLakeEngine.createSqlSessionFactory("mybatis/mybatis-config.xml");

    // Parameters for the MyBatis query
    Map<String, Object> params = new HashMap<>();
    params.put("id1", 1L);
    params.put("name1", "Updated");
    params.put("value1", 150.0);
    params.put("id2", 3L);
    params.put("name2", "New");
    params.put("value2", 300.0);
    if (!isSnapshotMode) {
      params.put("opType1", "U");
      params.put("opType2", "I");
    } else {
      params.put("id3", 2L);
      params.put("name3", "Initial");
      params.put("value3", 200.0);
    }

    // The mapper statement should have different SQL for snapshot vs changes mode
    String mybatisStatement =
        isSnapshotMode ? "TestMapper.getSCD2SnapshotData" : "TestMapper.getSCD2ChangeData";

    LocalDateTime effectiveTime = LocalDateTime.parse("2025-01-02T00:00:00");

    // Apply SCD2 changes using a MyBatis statement
    if (isSnapshotMode) {
      swiftLakeEngine
          .applySnapshotAsSCD2(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceMybatisStatement(mybatisStatement, params)
          .effectiveTimestamp(effectiveTime)
          .keyColumns(Arrays.asList("id"))
          .effectivePeriodColumns("effective_start", "effective_end")
          .processSourceTables(false)
          .sqlSessionFactory(sqlSessionFactory)
          .execute();
    } else {
      swiftLakeEngine
          .applyChangesAsSCD2(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceMybatisStatement(mybatisStatement, params)
          .effectiveTimestamp(effectiveTime)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .effectivePeriodColumns("effective_start", "effective_end")
          .processSourceTables(false)
          .sqlSessionFactory(sqlSessionFactory)
          .execute();
    }

    // Verify the changes were applied correctly
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "Initial",
                "value",
                100.0,
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            new HashMap<>() {
              {
                put("id", 1L);
                put("name", "Updated");
                put("value", 150.0);
                put("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
                put("effective_end", null);
              }
            },
            new HashMap<>() {
              {
                put("id", 2L);
                put("name", "Initial");
                put("value", 200.0);
                put("effective_start", LocalDateTime.parse("2025-01-01T00:00:00"));
                put("effective_end", null);
              }
            },
            new HashMap<>() {
              {
                put("id", 3L);
                put("name", "New");
                put("value", 300.0);
                put("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
                put("effective_end", null);
              }
            });

    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTableFilters(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()),
            Types.NestedField.required(4, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(5, "effective_end", Types.TimestampType.withoutZone()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd2_table_filter_sql_test_"
                + (isSnapshotMode ? "snapshot_" : "changes_")
                + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    // Insert initial data for multiple regions
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "region", "US", "value", 100.0),
            Map.of("id", 2L, "region", "US", "value", 200.0),
            Map.of("id", 3L, "region", "EU", "value", 300.0),
            Map.of("id", 4L, "region", "EU", "value", 400.0),
            Map.of("id", 5L, "region", "ASIA", "value", 500.0),
            Map.of("id", 6L, "region", "ASIA", "value", 600.0));

    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00");

    // Changes data for all regions
    List<Map<String, Object>> changes = new ArrayList<>();
    if (isSnapshotMode) {
      changes.addAll(
          Arrays.asList(
              Map.of("id", 1L, "region", "US", "value", 110.0),
              Map.of("id", 3L, "region", "EU", "value", 330.0),
              Map.of("id", 5L, "region", "ASIA", "value", 550.0),
              Map.of("id", 7L, "region", "US", "value", 700.0),
              Map.of("id", 2L, "region", "US", "value", 200.0),
              Map.of("id", 4L, "region", "EU", "value", 400.0),
              Map.of("id", 6L, "region", "ASIA", "value", 600.0)));
    } else {
      changes.addAll(
          Arrays.asList(
              Map.of("id", 1L, "region", "US", "value", 110.0, "operation_type", "U"),
              Map.of("id", 3L, "region", "EU", "value", 330.0, "operation_type", "U"),
              Map.of("id", 5L, "region", "ASIA", "value", 550.0, "operation_type", "U"),
              Map.of("id", 7L, "region", "US", "value", 700.0, "operation_type", "I")));
    }

    LocalDateTime effectiveTime = LocalDateTime.parse("2025-01-02T00:00:00");

    // Test SQL filter first
    if (isSnapshotMode) {
      swiftLakeEngine
          .applySnapshotAsSCD2(tableName)
          .tableFilterSql("region = 'US'") // Only apply to US region
          .sourceSql(TestUtil.createSnapshotSql(changes, schema))
          .effectiveTimestamp(effectiveTime)
          .keyColumns(Arrays.asList("id", "region"))
          .effectivePeriodColumns("effective_start", "effective_end")
          .processSourceTables(false)
          .execute();
    } else {
      swiftLakeEngine
          .applyChangesAsSCD2(tableName)
          .tableFilterSql("region = 'US'") // Only apply to US region
          .sourceSql(TestUtil.createChangeSql(changes, schema))
          .effectiveTimestamp(effectiveTime)
          .keyColumns(Arrays.asList("id", "region"))
          .operationTypeColumn("operation_type", "D")
          .effectivePeriodColumns("effective_start", "effective_end")
          .processSourceTables(false)
          .execute();
    }

    // Verify only US region records were updated
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    // Check specific records
    assertThat(actualData)
        .anySatisfy(
            row -> {
              assertThat(row).containsEntry("id", 1L);
              assertThat(row).containsEntry("region", "US");
              assertThat(row).containsEntry("value", 110.0);
              assertThat(row)
                  .containsEntry("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
            });

    // New record should be added
    assertThat(actualData)
        .anySatisfy(
            row -> {
              assertThat(row).containsEntry("id", 7L);
              assertThat(row).containsEntry("region", "US");
            });

    // Non-US records should remain unchanged
    assertThat(actualData)
        .anySatisfy(
            row -> {
              assertThat(row).containsEntry("id", 3L);
              assertThat(row).containsEntry("region", "EU");
              assertThat(row).containsEntry("value", 300.0);
              assertThat(row)
                  .containsEntry("effective_start", LocalDateTime.parse("2025-01-01T00:00:00"));
            });

    // Reset table for expression test
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
    swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00");

    // Test expression filter
    if (isSnapshotMode) {
      swiftLakeEngine
          .applySnapshotAsSCD2(tableName)
          .tableFilter(Expressions.equal("region", "US")) // Only apply to US region
          .sourceSql(TestUtil.createSnapshotSql(changes, schema))
          .effectiveTimestamp(effectiveTime)
          .keyColumns(Arrays.asList("id", "region"))
          .effectivePeriodColumns("effective_start", "effective_end")
          .processSourceTables(false)
          .execute();
    } else {
      swiftLakeEngine
          .applyChangesAsSCD2(tableName)
          .tableFilter(Expressions.equal("region", "US")) // Only apply to US region
          .sourceSql(TestUtil.createChangeSql(changes, schema))
          .effectiveTimestamp(effectiveTime)
          .keyColumns(Arrays.asList("id", "region"))
          .operationTypeColumn("operation_type", "D")
          .effectivePeriodColumns("effective_start", "effective_end")
          .processSourceTables(false)
          .execute();
    }

    // Verify results for expression filter - should be same as SQL filter
    List<Map<String, Object>> expressionFilterData =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    // Check specific records
    assertThat(expressionFilterData)
        .anySatisfy(
            row -> {
              assertThat(row).containsEntry("id", 1L);
              assertThat(row).containsEntry("region", "US");
              assertThat(row).containsEntry("value", 110.0);
              assertThat(row)
                  .containsEntry("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
            });

    // Non-US records should remain unchanged
    assertThat(expressionFilterData)
        .anySatisfy(
            row -> {
              assertThat(row).containsEntry("id", 3L);
              assertThat(row).containsEntry("region", "EU");
              assertThat(row).containsEntry("value", 300.0);
              assertThat(row)
                  .containsEntry("effective_start", LocalDateTime.parse("2025-01-01T00:00:00"));
            });

    if (!isSnapshotMode) {
      // Reset table for filter columns test
      TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
      swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
      SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
          swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00");

      changes =
          Arrays.asList(
              Map.of("id", 1L, "region", "US", "value", 110.0, "operation_type", "U"),
              Map.of("id", 7L, "region", "US", "value", 700.0, "operation_type", "I"));

      swiftLakeEngine
          .applyChangesAsSCD2(tableName)
          .tableFilterColumns(Arrays.asList("region"))
          .sourceSql(TestUtil.createChangeSql(changes, schema))
          .effectiveTimestamp(effectiveTime)
          .keyColumns(Arrays.asList("id", "region"))
          .operationTypeColumn("operation_type", "D")
          .effectivePeriodColumns("effective_start", "effective_end")
          .processSourceTables(false)
          .execute();

      // Verify results for expression filter - should be same as SQL filter
      List<Map<String, Object>> filterColumnsData =
          TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

      // Check specific records
      assertThat(filterColumnsData)
          .anySatisfy(
              row -> {
                assertThat(row).containsEntry("id", 1L);
                assertThat(row).containsEntry("region", "US");
                assertThat(row).containsEntry("value", 110.0);
                assertThat(row)
                    .containsEntry("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
              });

      // Non-US records should remain unchanged
      assertThat(filterColumnsData)
          .anySatisfy(
              row -> {
                assertThat(row).containsEntry("id", 3L);
                assertThat(row).containsEntry("region", "EU");
                assertThat(row).containsEntry("value", 300.0);
                assertThat(row)
                    .containsEntry("effective_start", LocalDateTime.parse("2025-01-01T00:00:00"));
              });
    }
    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTableBatchTransactionWithMultiThreading(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()),
            Types.NestedField.required(4, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(5, "effective_end", Types.TimestampType.withoutZone()));

    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("id").build();

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd2_batch_transaction_test_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    try {
      // Initial data insertion - create 100 records
      List<Map<String, Object>> initialData =
          IntStream.range(0, 100)
              .mapToObj(
                  i ->
                      Map.of(
                          "id",
                          (Object) (long) i,
                          "name",
                          "initial_name_" + i,
                          "value",
                          (double) i * 1.5))
              .collect(Collectors.toList());

      SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
          swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00");

      // Define timestamp for the batch operation
      LocalDateTime batchTimestamp = LocalDateTime.parse("2025-01-02T00:00:00");

      // Execute batch transaction with multi-threading
      TableBatchTransaction transaction =
          TableBatchTransaction.builderFor(swiftLakeEngine, tableName).build();

      // Execute 5 parallel operations
      int numThreads = 5;
      int recordsPerThread = 20;

      ExecutorService executor = Executors.newFixedThreadPool(numThreads);
      CountDownLatch latch = new CountDownLatch(numThreads);

      for (int t = 0; t < numThreads; t++) {
        final int threadNum = t;
        executor.submit(
            () -> {
              try {
                // Each thread processes a subset of records
                int startIdx = threadNum * recordsPerThread;
                int endIdx = startIdx + recordsPerThread;

                List<Map<String, Object>> threadData =
                    IntStream.range(startIdx, endIdx)
                        .mapToObj(
                            i ->
                                Map.of(
                                    "id",
                                    (Object) (long) i,
                                    "name",
                                    "updated_by_thread_" + threadNum + "_record_" + i,
                                    "value",
                                    (double) i * 3.0,
                                    "operation_type",
                                    "U"))
                        .collect(Collectors.toList());

                if (isSnapshotMode) {
                  swiftLakeEngine
                      .applySnapshotAsSCD2(transaction)
                      .tableFilterSql(String.format("id >= %d AND id < %d", startIdx, endIdx))
                      .sourceSql(TestUtil.createChangeSql(threadData, schema))
                      .effectiveTimestamp(batchTimestamp)
                      .keyColumns(Arrays.asList("id"))
                      .effectivePeriodColumns("effective_start", "effective_end")
                      .processSourceTables(false)
                      .execute();
                } else {
                  swiftLakeEngine
                      .applyChangesAsSCD2(transaction)
                      .tableFilterSql(String.format("id >= %d AND id < %d", startIdx, endIdx))
                      .sourceSql(TestUtil.createChangeSql(threadData, schema))
                      .effectiveTimestamp(batchTimestamp)
                      .keyColumns(Arrays.asList("id"))
                      .operationTypeColumn("operation_type", "D")
                      .effectivePeriodColumns("effective_start", "effective_end")
                      .processSourceTables(false)
                      .execute();
                }
              } finally {
                latch.countDown();
              }
            });
      }

      try {
        // Wait for all threads to complete
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        if (!completed) {
          throw new RuntimeException("Timeout waiting for batch operations to complete");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for batch operations", e);
      } finally {
        executor.shutdown();
      }

      // Commit the batch transaction
      transaction.commit();

      // Verify the results
      List<Map<String, Object>> actualData =
          TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

      // Expected data should have 200 records (100 initial + 100 updated)
      List<Map<String, Object>> expectedData = new ArrayList<>();

      // Add initial records (now with effective_end)
      expectedData.addAll(
          IntStream.range(0, 100)
              .mapToObj(
                  i ->
                      Map.of(
                          "id",
                          (Object) (long) i,
                          "name",
                          "initial_name_" + i,
                          "value",
                          (double) i * 1.5,
                          "effective_start",
                          LocalDateTime.parse("2025-01-01T00:00:00"),
                          "effective_end",
                          LocalDateTime.parse("2025-01-02T00:00:00")))
              .collect(Collectors.toList()));

      // Add updated records (without effective_end)
      for (int t = 0; t < 5; t++) {
        int startIdx = t * 20;
        int endIdx = startIdx + 20;

        int tValue = t;
        expectedData.addAll(
            IntStream.range(startIdx, endIdx)
                .mapToObj(
                    i ->
                        new HashMap<String, Object>() {
                          {
                            put("id", (long) i);
                            put("name", "updated_by_thread_" + tValue + "_record_" + i);
                            put("value", (double) i * 3.0);
                            put("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
                            put("effective_end", null);
                          }
                        })
                .collect(Collectors.toList()));
      }

      // Add assertions to verify correctness
      assertThat(actualData).hasSize(200); // 100 initial + 100 updated records
      assertThat(actualData)
          .usingRecursiveFieldByFieldElementComparatorIgnoringFields("effective_end")
          .containsAll(expectedData);

      // Specifically check within each batch that the records were updated correctly
      for (int t = 0; t < 5; t++) {
        final int threadNum = t;
        int startIdx = threadNum * 20;
        int endIdx = startIdx + 20;

        List<Map<String, Object>> threadUpdates =
            actualData.stream()
                .filter(
                    record -> {
                      Long id = (Long) record.get("id");
                      return id >= startIdx
                          && id < endIdx
                          && record
                              .get("effective_start")
                              .equals(LocalDateTime.parse("2025-01-02T00:00:00"));
                    })
                .collect(Collectors.toList());

        assertThat(threadUpdates).hasSize(20);
        threadUpdates.forEach(
            record -> {
              Long id = (Long) record.get("id");
              assertThat(record.get("name"))
                  .isEqualTo("updated_by_thread_" + threadNum + "_record_" + id);
              assertThat(record.get("value")).isEqualTo((double) id * 3.0);
            });
      }

      // Verify that all operations were done in a single transaction by checking snapshot history
      Table table = swiftLakeEngine.getTable(tableName);

      // After initial data load and one batch transaction, we should have exactly 2 snapshots
      assertThat(table.history()).hasSize(2);
    } finally {
      // Cleanup
      TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
    }
  }

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  void testSkipDataSortingSCD2(boolean isPartitioned, boolean isSnapshotMode) throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "sort_key", Types.IntegerType.get()),
            Types.NestedField.required(3, "value", Types.StringType.get()),
            Types.NestedField.required(4, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(5, "effective_end", Types.TimestampType.withoutZone()));

    SortOrder sortOrder =
        SortOrder.builderFor(schema).asc("sort_key").asc("effective_start").build();
    PartitionSpec partitionSpec =
        isPartitioned
            ? PartitionSpec.builderFor(schema).bucket("id", 1).build()
            : PartitionSpec.unpartitioned();

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd2_skip_data_sorting_test_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine
        .getCatalog()
        .buildTable(tableId, schema)
        .withPartitionSpec(partitionSpec)
        .withSortOrder(sortOrder)
        .create();
    String tableName = tableId.toString();

    // Insert initial data in reverse order
    List<Map<String, Object>> initialRecords = new ArrayList<>();
    for (int i = 50; i > 0; i--) {
      Map<String, Object> record = new HashMap<>();
      record.put("id", i);
      record.put("sort_key", i);
      record.put("value", "initial_" + String.format("%03d", i));
      initialRecords.add(record);
    }

    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialRecords, "2025-01-01T00:00:00");

    // Prepare SCD2 changes (update some records, add new ones)
    List<Map<String, Object>> changes = new ArrayList<>();

    // Updates for some existing records
    for (int i = 10; i <= 40; i += 10) {
      Map<String, Object> record = new HashMap<>();
      record.put("id", i);
      record.put("sort_key", i);
      record.put("value", "updated_" + String.format("%03d", i));
      if (!isSnapshotMode) {
        record.put("operation_type", "U");
      }
      changes.add(record);
    }

    // New records with higher IDs in reverse order
    for (int i = 100; i > 50; i -= 10) {
      Map<String, Object> record = new HashMap<>();
      record.put("id", i);
      record.put("sort_key", i);
      record.put("value", "new_" + String.format("%03d", i));
      if (!isSnapshotMode) {
        record.put("operation_type", "I");
      }
      changes.add(record);
    }

    if (isSnapshotMode) {
      // Add original records
      for (int i = 1; i <= 50; i++) {
        if (!(i % 10 == 0 && i <= 40)) {
          Map<String, Object> record = new HashMap<>();
          record.put("id", i);
          record.put("sort_key", i);
          record.put("value", "initial_" + String.format("%03d", i));
          changes.add(record);
        }
      }
    }

    // Expected data after merge
    List<Map<String, Object>> expectedRecords = new ArrayList<>();

    // Add original records with effective_end for updated records
    for (int i = 1; i <= 50; i++) {
      Map<String, Object> record = new HashMap<>();
      record.put("id", i);
      record.put("sort_key", i);
      record.put("value", "initial_" + String.format("%03d", i));
      record.put("effective_start", LocalDateTime.parse("2025-01-01T00:00:00"));

      if (i % 10 == 0 && i <= 40) {
        // Updated records get an effective_end
        record.put("effective_end", LocalDateTime.parse("2025-01-02T00:00:00"));
      } else {
        // Non-updated records have null effective_end
        record.put("effective_end", null);
      }
      expectedRecords.add(record);
    }

    // Add updated records
    for (int i = 10; i <= 40; i += 10) {
      Map<String, Object> record = new HashMap<>();
      record.put("id", i);
      record.put("sort_key", i);
      record.put("value", "updated_" + String.format("%03d", i));
      record.put("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
      record.put("effective_end", null);
      expectedRecords.add(record);
    }

    // Add new records
    for (int i = 60; i <= 100; i += 10) {
      Map<String, Object> record = new HashMap<>();
      record.put("id", i);
      record.put("sort_key", i);
      record.put("value", "new_" + String.format("%03d", i));
      record.put("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
      record.put("effective_end", null);
      expectedRecords.add(record);
    }

    // Create a sorted copy of the expected records for comparison
    List<Map<String, Object>> expectedSortedRecords = new ArrayList<>(expectedRecords);
    expectedSortedRecords.sort(
        Comparator.comparing((Map<String, Object> m) -> (Integer) m.get("sort_key"))
            .thenComparing(
                (Map<String, Object> m) -> {
                  LocalDateTime date = (LocalDateTime) m.get("effective_start");
                  return date != null ? date : LocalDateTime.MIN;
                }));

    // Apply SCD2 changes with skipDataSorting=true
    LocalDateTime effectiveTimestamp = LocalDateTime.parse("2025-01-02T00:00:00");
    String changesSql =
        isSnapshotMode
            ? TestUtil.createSnapshotSql(changes, schema)
            : TestUtil.createChangeSql(changes, schema);

    if (isSnapshotMode) {
      swiftLakeEngine
          .applySnapshotAsSCD2(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(changesSql)
          .effectiveTimestamp(effectiveTimestamp)
          .keyColumns(Arrays.asList("id"))
          .effectivePeriodColumns("effective_start", "effective_end")
          .skipDataSorting(true)
          .processSourceTables(false)
          .execute();
    } else {
      swiftLakeEngine
          .applyChangesAsSCD2(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(changesSql)
          .effectiveTimestamp(effectiveTimestamp)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .effectivePeriodColumns("effective_start", "effective_end")
          .skipDataSorting(true)
          .processSourceTables(false)
          .execute();
    }
    // Read the data in file order
    List<Map<String, Object>> actualRecordsWithSkipSorting =
        TestUtil.getRecordsFromTableInFileOrder(swiftLakeEngine, tableName);

    // With skipDataSorting=true, the data should not be in sort_key order
    boolean isUnsortedWithSkip =
        !TestUtil.compareDataInOrder(actualRecordsWithSkipSorting, expectedSortedRecords);

    // First, check that the records match regardless of order
    assertThat(actualRecordsWithSkipSorting)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedRecords)
        .as("All records should be present in the table after merge with skipDataSorting=true");

    // Check if data is unsorted as expected
    assertThat(isUnsortedWithSkip).isTrue();

    // Drop and recreate table for the second test
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
    swiftLakeEngine
        .getCatalog()
        .buildTable(tableId, schema)
        .withPartitionSpec(partitionSpec)
        .withSortOrder(sortOrder)
        .create();

    // Insert initial data again
    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialRecords, "2025-01-01T00:00:00");

    // Apply SCD2 changes with skipDataSorting=false
    if (isSnapshotMode) {
      swiftLakeEngine
          .applySnapshotAsSCD2(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(changesSql)
          .effectiveTimestamp(effectiveTimestamp)
          .keyColumns(Arrays.asList("id"))
          .effectivePeriodColumns("effective_start", "effective_end")
          .skipDataSorting(false)
          .processSourceTables(false)
          .execute();
    } else {
      swiftLakeEngine
          .applyChangesAsSCD2(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(changesSql)
          .effectiveTimestamp(effectiveTimestamp)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .effectivePeriodColumns("effective_start", "effective_end")
          .skipDataSorting(false)
          .processSourceTables(false)
          .execute();
    }
    // Read the data in file order
    List<Map<String, Object>> actualRecordsWithoutSkipSorting =
        TestUtil.getRecordsFromTableInFileOrder(swiftLakeEngine, tableName);

    // With skipDataSorting=false, the data should match the sorted order
    boolean isSortedWithoutSkip =
        TestUtil.compareDataInOrder(actualRecordsWithoutSkipSorting, expectedSortedRecords);

    // First, check that the records match regardless of order
    assertThat(actualRecordsWithoutSkipSorting)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedRecords)
        .as("All records should be present in the table after merge with skipDataSorting=false");

    // With skipDataSorting=false, the data should match exactly the sorted order
    assertThat(isSortedWithoutSkip).isTrue();

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  public void testSCD2MergeWithDifferentIsolationLevels() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "year", Types.IntegerType.get()),
            Types.NestedField.required(3, "value", Types.StringType.get()),
            Types.NestedField.required(4, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(5, "effective_end", Types.TimestampType.withoutZone()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("year").build();

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "scd2_isolation_test_" + UUID.randomUUID().toString().replace('-', '_'));
    Table table = swiftLakeEngine.getCatalog().createTable(tableId, schema, spec);
    String tableName = tableId.toString();

    // Insert initial data: two records with different years
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "year", 2023, "value", "initial_2023_value"),
            Map.of("id", 2L, "year", 2024, "value", "initial_2024_value"));

    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00");

    // Refresh and capture the table state after initial insert
    table.refresh();
    Table tableStateAfterInitialInsert = swiftLakeEngine.getTable(tableName);
    long snapshotIdAfterInitialInsert = tableStateAfterInitialInsert.currentSnapshot().snapshotId();

    // Insert another record using applyChangesAsSCD2
    List<Map<String, Object>> secondInsertData =
        Arrays.asList(
            Map.of("id", 3L, "year", 2024, "value", "second_2024_value", "operation_type", "I"));

    LocalDateTime secondInsertTimestamp = LocalDateTime.parse("2025-01-02T00:00:00");
    String secondInsertSql = TestUtil.createChangeSql(secondInsertData, schema);

    swiftLakeEngine
        .applyChangesAsSCD2(tableName)
        .tableFilterSql("id IS NOT NULL")
        .sourceSql(secondInsertSql)
        .effectiveTimestamp(secondInsertTimestamp)
        .keyColumns(Arrays.asList("id"))
        .operationTypeColumn("operation_type", "D")
        .effectivePeriodColumns("effective_start", "effective_end")
        .processSourceTables(false)
        .execute();

    // Refresh and capture table states after second insert
    table.refresh();
    Table tableStateAfterSecondInsert = swiftLakeEngine.getTable(tableName);
    Table tableStateAfterSecondInsertCopy = swiftLakeEngine.getTable(tableName);

    // Verify second insert created a new snapshot
    assertThat(tableStateAfterSecondInsert.currentSnapshot().snapshotId())
        .isNotEqualTo(snapshotIdAfterInitialInsert);

    // Prepare SCD2 changes that will update ID=1 and insert a new record
    List<Map<String, Object>> initialChanges =
        Arrays.asList(
            Map.of("id", 1L, "year", 2023, "value", "updated_2023_value", "operation_type", "U"),
            Map.of("id", 4L, "year", 2025, "value", "new_2025_value", "operation_type", "I"));

    LocalDateTime thirdChangeTimestamp = LocalDateTime.parse("2025-01-03T00:00:00");
    String initialChangesSql = TestUtil.createChangeSql(initialChanges, schema);

    // Attempt to apply changes with default isolation level should succeed
    // since we're not targeting the same partition that has been modified
    swiftLakeEngine
        .applyChangesAsSCD2(tableName)
        .tableFilterSql("id IS NOT NULL")
        .sourceSql(initialChangesSql)
        .effectiveTimestamp(thirdChangeTimestamp)
        .keyColumns(Arrays.asList("id"))
        .operationTypeColumn("operation_type", "D")
        .effectivePeriodColumns("effective_start", "effective_end")
        .processSourceTables(false)
        .execute();

    // Prepare changes that will affect the 2024 partition (which has conflicts)
    List<Map<String, Object>> conflictingChanges =
        Arrays.asList(
            Map.of("id", 2L, "year", 2024, "value", "updated_2024_value", "operation_type", "U"),
            Map.of("id", 3L, "year", 2024, "value", "deleted_value", "operation_type", "D"));

    LocalDateTime fourthChangeTimestamp = LocalDateTime.parse("2025-01-04T00:00:00");
    String conflictingChangesSql = TestUtil.createChangeSql(conflictingChanges, schema);

    // Attempting to apply changes to the 2024 partition using the old table state with default
    // isolation
    // should fail due to conflicting files
    assertThatThrownBy(
            () -> {
              swiftLakeEngine
                  .applyChangesAsSCD2(tableStateAfterInitialInsert)
                  .tableFilterSql("year = 2024")
                  .sourceSql(conflictingChangesSql)
                  .effectiveTimestamp(fourthChangeTimestamp)
                  .keyColumns(Arrays.asList("id"))
                  .operationTypeColumn("operation_type", "D")
                  .effectivePeriodColumns("effective_start", "effective_end")
                  .processSourceTables(false)
                  .execute();
            })
        .hasMessageContaining("Found conflicting files");

    // Use SNAPSHOT isolation with the table at first snapshot state
    swiftLakeEngine
        .applyChangesAsSCD2(tableStateAfterInitialInsert)
        .tableFilterSql("year = 2024")
        .sourceSql(conflictingChangesSql)
        .effectiveTimestamp(fourthChangeTimestamp)
        .keyColumns(Arrays.asList("id"))
        .operationTypeColumn("operation_type", "D")
        .effectivePeriodColumns("effective_start", "effective_end")
        .isolationLevel(IsolationLevel.SNAPSHOT)
        .processSourceTables(false)
        .execute();

    // Refresh and verify the operation succeeded with a new snapshot
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId())
        .isNotEqualTo(tableStateAfterSecondInsert.currentSnapshot().snapshotId());

    List<Map<String, Object>> moreChanges =
        Arrays.asList(
            Map.of(
                "id",
                2L,
                "year",
                2024,
                "value",
                "updated_again_2024_value",
                "operation_type",
                "U"));

    LocalDateTime fifthChangeTimestamp = LocalDateTime.parse("2025-01-05T00:00:00");
    String moreChangesSql = TestUtil.createChangeSql(moreChanges, schema);

    // Attempting with SNAPSHOT isolation on outdated state should also fail
    assertThatThrownBy(
            () -> {
              swiftLakeEngine
                  .applyChangesAsSCD2(tableStateAfterSecondInsert)
                  .tableFilterSql("year = 2024")
                  .sourceSql(moreChangesSql)
                  .effectiveTimestamp(fifthChangeTimestamp)
                  .keyColumns(Arrays.asList("id"))
                  .operationTypeColumn("operation_type", "D")
                  .effectivePeriodColumns("effective_start", "effective_end")
                  .isolationLevel(IsolationLevel.SNAPSHOT)
                  .processSourceTables(false)
                  .execute();
            })
        .hasMessageContaining("Missing required files to delete");

    // Attempting with SERIALIZABLE isolation on outdated state should also fail
    assertThatThrownBy(
            () -> {
              swiftLakeEngine
                  .applyChangesAsSCD2(tableStateAfterSecondInsertCopy)
                  .tableFilterSql("year = 2024")
                  .sourceSql(moreChangesSql)
                  .effectiveTimestamp(fifthChangeTimestamp)
                  .keyColumns(Arrays.asList("id"))
                  .operationTypeColumn("operation_type", "D")
                  .effectivePeriodColumns("effective_start", "effective_end")
                  .isolationLevel(IsolationLevel.SERIALIZABLE)
                  .processSourceTables(false)
                  .execute();
            })
        .hasMessageContaining("Found conflicting files");

    // Use SNAPSHOT isolation with current table state
    swiftLakeEngine
        .applyChangesAsSCD2(tableName)
        .tableFilterSql("year = 2024")
        .sourceSql(moreChangesSql)
        .effectiveTimestamp(fifthChangeTimestamp)
        .keyColumns(Arrays.asList("id"))
        .operationTypeColumn("operation_type", "D")
        .effectivePeriodColumns("effective_start", "effective_end")
        .isolationLevel(IsolationLevel.SNAPSHOT)
        .processSourceTables(false)
        .execute();

    // Verify we have latest data
    table.refresh();
    long updatedSnapshotId = table.currentSnapshot().snapshotId();

    // Read and verify the data after SNAPSHOT isolation changes
    List<Map<String, Object>> dataAfterSnapshotChanges =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    // We should have:
    // - Original ID=1 (with effective_end)
    // - Updated ID=1 (current)
    // - Original ID=2 (with effective_end)
    // - Updated ID=2 (with effective_end)
    // - Updated ID=2 (current)
    // - Original ID=3 (with effective_end)
    // - ID=4 (current)
    // No ID=3 current version since it was deleted in a previous operation

    assertThat(dataAfterSnapshotChanges)
        .hasSize(7)
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("year")).isEqualTo(2023);
              assertThat(row.get("value")).isEqualTo("initial_2023_value");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("year")).isEqualTo(2023);
              assertThat(row.get("value")).isEqualTo("updated_2023_value");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("value")).isEqualTo("initial_2024_value");
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-04T00:00:00"));
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-04T00:00:00"));
              assertThat(row.get("value")).isEqualTo("updated_2024_value");
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-05T00:00:00"));
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("value")).isEqualTo("updated_again_2024_value");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-05T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(4L);
              assertThat(row.get("year")).isEqualTo(2025);
              assertThat(row.get("value")).isEqualTo("new_2025_value");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            });

    // Find all records for ID=3 to check the delete operation
    List<Map<String, Object>> id3Records =
        dataAfterSnapshotChanges.stream()
            .filter(r -> ((Long) r.get("id")).equals(3L))
            .collect(Collectors.toList());

    assertThat(id3Records)
        .hasSize(1) // The original record, now with an effective_end
        .allSatisfy(
            row -> {
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
              assertThat(row.get("effective_end")).isNotNull(); // Has an end date due to delete
            });

    // Use SERIALIZABLE isolation with current table state for final changes
    List<Map<String, Object>> serializableChanges =
        Arrays.asList(
            Map.of(
                "id", 7L, "year", 2024, "value", "serializable_2024_value", "operation_type", "I"),
            Map.of("id", 2L, "year", 2024, "value", "final_update_value", "operation_type", "U"));

    LocalDateTime finalChangeTimestamp = LocalDateTime.parse("2025-01-06T00:00:00");
    String serializableChangesSql = TestUtil.createChangeSql(serializableChanges, schema);

    swiftLakeEngine
        .applyChangesAsSCD2(tableName)
        .tableFilterSql("year = 2024")
        .sourceSql(serializableChangesSql)
        .effectiveTimestamp(finalChangeTimestamp)
        .keyColumns(Arrays.asList("id"))
        .operationTypeColumn("operation_type", "D")
        .effectivePeriodColumns("effective_start", "effective_end")
        .isolationLevel(IsolationLevel.SERIALIZABLE)
        .processSourceTables(false)
        .execute();

    // The operation should create a new snapshot
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(updatedSnapshotId);

    // Verify final data after all operations
    List<Map<String, Object>> finalData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    assertThat(finalData)
        .hasSize(9)
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("year")).isEqualTo(2023);
              assertThat(row.get("value")).isEqualTo("initial_2023_value");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("year")).isEqualTo(2023);
              assertThat(row.get("value")).isEqualTo("updated_2023_value");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("value")).isEqualTo("initial_2024_value");
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-04T00:00:00"));
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-04T00:00:00"));
              assertThat(row.get("value")).isEqualTo("updated_2024_value");
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-05T00:00:00"));
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("value")).isEqualTo("updated_again_2024_value");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-05T00:00:00"));
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-06T00:00:00"));
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("value")).isEqualTo("final_update_value");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-06T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(4L);
              assertThat(row.get("year")).isEqualTo(2025);
              assertThat(row.get("value")).isEqualTo("new_2025_value");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(7L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("value")).isEqualTo("serializable_2024_value");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-06T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            });

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSCD2MergeWithBranchOption(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()),
            Types.NestedField.required(4, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(5, "effective_end", Types.TimestampType.withoutZone()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "scd2_branch_test_" + UUID.randomUUID().toString().replace('-', '_'));
    Table table =
        swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    try {
      // Create the main branch data
      List<Map<String, Object>> mainBranchData =
          Arrays.asList(
              Map.of("id", 1L, "name", "Main Record 1", "value", 100.0),
              Map.of("id", 2L, "name", "Main Record 2", "value", 200.0),
              Map.of("id", 3L, "name", "Main Record 3", "value", 300.0));

      SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
          swiftLakeEngine, tableName, schema, mainBranchData, "2025-01-01T00:00:00");

      // Create a development branch
      String devBranch = "dev";
      table.manageSnapshots().createBranch(devBranch).commit();

      // Apply changes to main branch
      List<Map<String, Object>> mainBranchChanges = null;
      if (isSnapshotMode) {
        mainBranchChanges =
            Arrays.asList(
                Map.of("id", 1L, "name", "Updated Main 1", "value", 110.0),
                Map.of("id", 4L, "name", "New Main Record", "value", 400.0),
                Map.of("id", 2L, "name", "Main Record 2", "value", 200.0),
                Map.of("id", 3L, "name", "Main Record 3", "value", 300.0));
      } else {
        mainBranchChanges =
            Arrays.asList(
                Map.of("id", 1L, "name", "Updated Main 1", "value", 110.0, "operation_type", "U"),
                Map.of("id", 4L, "name", "New Main Record", "value", 400.0, "operation_type", "I"));
      }

      if (isSnapshotMode) {
        swiftLakeEngine
            .applySnapshotAsSCD2(tableName)
            .tableFilterSql("id IS NOT NULL")
            .sourceSql(TestUtil.createSnapshotSql(mainBranchChanges, schema))
            .effectiveTimestamp(LocalDateTime.parse("2025-01-02T00:00:00"))
            .keyColumns(Arrays.asList("id"))
            .effectivePeriodColumns("effective_start", "effective_end")
            .processSourceTables(false)
            .execute();
      } else {
        swiftLakeEngine
            .applyChangesAsSCD2(tableName)
            .tableFilterSql("id IS NOT NULL")
            .sourceSql(TestUtil.createChangeSql(mainBranchChanges, schema))
            .effectiveTimestamp(LocalDateTime.parse("2025-01-02T00:00:00"))
            .keyColumns(Arrays.asList("id"))
            .operationTypeColumn("operation_type", "D")
            .effectivePeriodColumns("effective_start", "effective_end")
            .processSourceTables(false)
            .execute();
      }

      // Apply different changes to dev branch
      List<Map<String, Object>> devBranchChanges = null;

      if (isSnapshotMode) {
        devBranchChanges =
            Arrays.asList(
                Map.of("id", 1L, "name", "Dev Record 1", "value", 150.0),
                Map.of("id", 2L, "name", "Dev Record 2", "value", 250.0),
                Map.of("id", 5L, "name", "New Dev Record", "value", 500.0),
                Map.of("id", 3L, "name", "Main Record 3", "value", 300.0));
      } else {
        devBranchChanges =
            Arrays.asList(
                Map.of("id", 1L, "name", "Dev Record 1", "value", 150.0, "operation_type", "U"),
                Map.of("id", 2L, "name", "Dev Record 2", "value", 250.0, "operation_type", "U"),
                Map.of("id", 5L, "name", "New Dev Record", "value", 500.0, "operation_type", "I"));
      }

      if (isSnapshotMode) {
        swiftLakeEngine
            .applySnapshotAsSCD2(tableName)
            .tableFilterSql("id IS NOT NULL")
            .sourceSql(TestUtil.createSnapshotSql(devBranchChanges, schema))
            .effectiveTimestamp(LocalDateTime.parse("2025-01-03T00:00:00"))
            .keyColumns(Arrays.asList("id"))
            .effectivePeriodColumns("effective_start", "effective_end")
            .branch(devBranch) // Apply to dev branch
            .processSourceTables(false)
            .execute();
      } else {
        swiftLakeEngine
            .applyChangesAsSCD2(tableName)
            .tableFilterSql("id IS NOT NULL")
            .sourceSql(TestUtil.createChangeSql(devBranchChanges, schema))
            .effectiveTimestamp(LocalDateTime.parse("2025-01-03T00:00:00"))
            .keyColumns(Arrays.asList("id"))
            .operationTypeColumn("operation_type", "D")
            .effectivePeriodColumns("effective_start", "effective_end")
            .branch(devBranch) // Apply to dev branch
            .processSourceTables(false)
            .execute();
      }
      // Verify main branch data
      List<Map<String, Object>> mainBranchRecords =
          TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

      assertThat(mainBranchRecords)
          .hasSize(5) // 3 initial records + 1 updated version of ID 1 + 1 new record
          .anySatisfy(
              row -> {
                assertThat(row.get("id")).isEqualTo(1L);
                assertThat(row.get("name")).isEqualTo("Main Record 1");
                assertThat(row.get("effective_start"))
                    .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
                assertThat(row.get("effective_end"))
                    .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
              })
          .anySatisfy(
              row -> {
                assertThat(row.get("id")).isEqualTo(1L);
                assertThat(row.get("name")).isEqualTo("Updated Main 1");
                assertThat(row.get("effective_start"))
                    .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
                assertThat(row.get("effective_end")).isNull();
              })
          .anySatisfy(
              row -> {
                assertThat(row.get("id")).isEqualTo(4L);
                assertThat(row.get("name")).isEqualTo("New Main Record");
                assertThat(row.get("effective_start"))
                    .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
                assertThat(row.get("effective_end")).isNull();
              });

      // Verify dev branch data
      List<Map<String, Object>> devBranchRecords =
          TestUtil.getRecordsFromTable(
              swiftLakeEngine, TestUtil.getTableNameForBranch(tableId, devBranch));

      assertThat(devBranchRecords)
          .hasSize(6) // 3 initial records + 2 updated records + 1 new record
          .anySatisfy(
              row -> {
                assertThat(row.get("id")).isEqualTo(1L);
                assertThat(row.get("name")).isEqualTo("Main Record 1");
                assertThat(row.get("effective_start"))
                    .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
                assertThat(row.get("effective_end"))
                    .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
              })
          .anySatisfy(
              row -> {
                assertThat(row.get("id")).isEqualTo(1L);
                assertThat(row.get("name")).isEqualTo("Dev Record 1");
                assertThat(row.get("value")).isEqualTo(150.0);
                assertThat(row.get("effective_start"))
                    .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
                assertThat(row.get("effective_end")).isNull();
              })
          .anySatisfy(
              row -> {
                assertThat(row.get("id")).isEqualTo(2L);
                assertThat(row.get("name")).isEqualTo("Main Record 2");
                assertThat(row.get("effective_start"))
                    .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
                assertThat(row.get("effective_end"))
                    .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
              })
          .anySatisfy(
              row -> {
                assertThat(row.get("id")).isEqualTo(2L);
                assertThat(row.get("name")).isEqualTo("Dev Record 2");
                assertThat(row.get("effective_start"))
                    .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
                assertThat(row.get("effective_end")).isNull();
              })
          .anySatisfy(
              row -> {
                assertThat(row.get("id")).isEqualTo(5L);
                assertThat(row.get("name")).isEqualTo("New Dev Record");
                assertThat(row.get("effective_start"))
                    .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
                assertThat(row.get("effective_end")).isNull();
              });

      // Apply additional changes to both branches
      List<Map<String, Object>> mainBranchChanges2 = null;
      if (isSnapshotMode) {
        mainBranchChanges2 =
            Arrays.asList(
                Map.of("id", 1L, "name", "Updated Main 1", "value", 110.0),
                Map.of("id", 4L, "name", "New Main Record", "value", 400.0),
                Map.of("id", 2L, "name", "Main Record 2", "value", 200.0),
                Map.of("id", 3L, "name", "Updated Main 3", "value", 330.0));
      } else {
        mainBranchChanges2 =
            Arrays.asList(
                Map.of("id", 3L, "name", "Updated Main 3", "value", 330.0, "operation_type", "U"));
      }

      if (isSnapshotMode) {
        swiftLakeEngine
            .applySnapshotAsSCD2(tableName)
            .tableFilterSql("id IS NOT NULL")
            .sourceSql(TestUtil.createSnapshotSql(mainBranchChanges2, schema))
            .effectiveTimestamp(LocalDateTime.parse("2025-01-04T00:00:00"))
            .keyColumns(Arrays.asList("id"))
            .effectivePeriodColumns("effective_start", "effective_end")
            .processSourceTables(false)
            .execute();
      } else {
        swiftLakeEngine
            .applyChangesAsSCD2(tableName)
            .tableFilterSql("id IS NOT NULL")
            .sourceSql(TestUtil.createChangeSql(mainBranchChanges2, schema))
            .effectiveTimestamp(LocalDateTime.parse("2025-01-04T00:00:00"))
            .keyColumns(Arrays.asList("id"))
            .operationTypeColumn("operation_type", "D")
            .effectivePeriodColumns("effective_start", "effective_end")
            .processSourceTables(false)
            .execute();
      }

      List<Map<String, Object>> devBranchChanges2 = null;
      if (isSnapshotMode) {
        devBranchChanges2 =
            Arrays.asList(
                Map.of("id", 1L, "name", "Dev Record 1", "value", 150.0),
                Map.of("id", 2L, "name", "Dev Record 2", "value", 250.0),
                Map.of("id", 5L, "name", "New Dev Record", "value", 500.0),
                Map.of("id", 3L, "name", "Dev Record 3", "value", 350.0));
      } else {
        devBranchChanges2 =
            Arrays.asList(
                Map.of("id", 3L, "name", "Dev Record 3", "value", 350.0, "operation_type", "U"));
      }

      if (isSnapshotMode) {
        swiftLakeEngine
            .applySnapshotAsSCD2(tableName)
            .tableFilterSql("id IS NOT NULL")
            .sourceSql(TestUtil.createSnapshotSql(devBranchChanges2, schema))
            .effectiveTimestamp(LocalDateTime.parse("2025-01-05T00:00:00"))
            .keyColumns(Arrays.asList("id"))
            .effectivePeriodColumns("effective_start", "effective_end")
            .branch(devBranch) // Apply to dev branch
            .processSourceTables(false)
            .execute();
      } else {
        swiftLakeEngine
            .applyChangesAsSCD2(tableName)
            .tableFilterSql("id IS NOT NULL")
            .sourceSql(TestUtil.createChangeSql(devBranchChanges2, schema))
            .effectiveTimestamp(LocalDateTime.parse("2025-01-05T00:00:00"))
            .keyColumns(Arrays.asList("id"))
            .operationTypeColumn("operation_type", "D")
            .effectivePeriodColumns("effective_start", "effective_end")
            .branch(devBranch) // Apply to dev branch
            .processSourceTables(false)
            .execute();
      }

      // Verify main branch data after additional changes
      List<Map<String, Object>> finalMainBranchRecords =
          TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

      assertThat(finalMainBranchRecords)
          .hasSize(6) // Now includes updated version of ID 3
          .anySatisfy(
              row -> {
                assertThat(row.get("id")).isEqualTo(3L);
                assertThat(row.get("name")).isEqualTo("Main Record 3");
                assertThat(row.get("effective_start"))
                    .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
                assertThat(row.get("effective_end"))
                    .isEqualTo(LocalDateTime.parse("2025-01-04T00:00:00"));
              })
          .anySatisfy(
              row -> {
                assertThat(row.get("id")).isEqualTo(3L);
                assertThat(row.get("name")).isEqualTo("Updated Main 3");
                assertThat(row.get("effective_start"))
                    .isEqualTo(LocalDateTime.parse("2025-01-04T00:00:00"));
                assertThat(row.get("effective_end")).isNull();
              });

      // Verify dev branch data after additional changes
      List<Map<String, Object>> finalDevBranchRecords =
          TestUtil.getRecordsFromTable(
              swiftLakeEngine, TestUtil.getTableNameForBranch(tableId, devBranch));

      assertThat(finalDevBranchRecords)
          .hasSize(7) // Now includes updated version of ID 3 in dev
          .anySatisfy(
              row -> {
                assertThat(row.get("id")).isEqualTo(3L);
                assertThat(row.get("name")).isEqualTo("Main Record 3");
                assertThat(row.get("effective_start"))
                    .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
                assertThat(row.get("effective_end"))
                    .isEqualTo(LocalDateTime.parse("2025-01-05T00:00:00"));
              })
          .anySatisfy(
              row -> {
                assertThat(row.get("id")).isEqualTo(3L);
                assertThat(row.get("name")).isEqualTo("Dev Record 3");
                assertThat(row.get("value")).isEqualTo(350.0);
                assertThat(row.get("effective_start"))
                    .isEqualTo(LocalDateTime.parse("2025-01-05T00:00:00"));
                assertThat(row.get("effective_end")).isNull();
              });
    } finally {
      // Clean up
      TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
    }
  }

  @Test
  void testSCD2SnapshotMetadata() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd2_snapshot_metadata_test_" + UUID.randomUUID().toString().replace('-', '_'));
    Table table =
        swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "value", "initial-value-1"),
            Map.of("id", 2L, "value", "initial-value-2"),
            Map.of("id", 3L, "value", "initial-value-3"));

    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00");

    // Prepare SCD2 changes for update and insert with snapshot metadata
    List<Map<String, Object>> changes =
        Arrays.asList(
            Map.of("id", 1L, "value", "updated-value-1", "operation_type", "U"),
            Map.of("id", 3L, "value", "to-be-deleted", "operation_type", "D"),
            Map.of("id", 4L, "value", "new-value-4", "operation_type", "I"));

    String changesSql = TestUtil.createChangeSql(changes, schema);
    LocalDateTime effectiveTimestamp1 = LocalDateTime.parse("2025-01-02T00:00:00");

    // Apply SCD2 changes with snapshot metadata
    Map<String, String> metadata = new HashMap<>();
    metadata.put("user", "scd2-user");
    metadata.put("source", "scd2-test-system");
    metadata.put("timestamp", LocalDateTime.now().toString());

    swiftLakeEngine
        .applyChangesAsSCD2(tableName)
        .tableFilterSql("id IS NOT NULL")
        .sourceSql(changesSql)
        .effectiveTimestamp(effectiveTimestamp1)
        .keyColumns(Arrays.asList("id"))
        .operationTypeColumn("operation_type", "D")
        .effectivePeriodColumns("effective_start", "effective_end")
        .snapshotMetadata(metadata)
        .processSourceTables(false)
        .execute();

    // Verify the snapshot contains our custom metadata
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();

    Map<String, String> snapshotMetadata = snapshot.summary();
    assertThat(snapshotMetadata)
        .containsEntry("user", "scd2-user")
        .containsEntry("source", "scd2-test-system")
        .containsKey("timestamp");

    // Check the data was correctly modified
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    assertThat(actualData)
        .hasSize(5) // 2 original records + 2 updated records (with history) + 1 new record
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("value")).isEqualTo("initial-value-1");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("value")).isEqualTo("updated-value-1");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("value")).isEqualTo("initial-value-2");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(3L);
              assertThat(row.get("value")).isEqualTo("initial-value-3");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(4L);
              assertThat(row.get("value")).isEqualTo("new-value-4");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            });

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testSCD2SkipEmptySource() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd2_snapshot_metadata_test_" + UUID.randomUUID().toString().replace('-', '_'));
    Table table =
        swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "value", "initial-value-1"),
            Map.of("id", 2L, "value", "initial-value-2"),
            Map.of("id", 3L, "value", "initial-value-3"));

    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00");

    List<Map<String, Object>> changes = Arrays.asList();
    String changesSql = TestUtil.createChangeSql(changes, schema);
    LocalDateTime effectiveTimestamp1 = LocalDateTime.parse("2025-01-02T00:00:00");

    // Apply SCD2 snpashot with skipEmptySource=true
    swiftLakeEngine
        .applySnapshotAsSCD2(tableName)
        .tableFilterSql("id IS NOT NULL")
        .sourceSql(changesSql)
        .effectiveTimestamp(effectiveTimestamp1)
        .keyColumns(Arrays.asList("id"))
        .effectivePeriodColumns("effective_start", "effective_end")
        .processSourceTables(false)
        .skipEmptySource(true)
        .execute();

    table.refresh();
    assertThat(table.history()).hasSize(1);

    // Check the data was not changed
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    assertThat(actualData)
        .hasSize(3)
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("value")).isEqualTo("initial-value-1");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("value")).isEqualTo("initial-value-2");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(3L);
              assertThat(row.get("value")).isEqualTo("initial-value-3");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            });

    // Apply SCD2 snpashot with skipEmptySource=false
    swiftLakeEngine
        .applySnapshotAsSCD2(tableName)
        .tableFilterSql("id IS NOT NULL")
        .sourceSql(changesSql)
        .effectiveTimestamp(effectiveTimestamp1)
        .keyColumns(Arrays.asList("id"))
        .effectivePeriodColumns("effective_start", "effective_end")
        .processSourceTables(false)
        .skipEmptySource(false)
        .execute();

    table.refresh();
    assertThat(table.history()).hasSize(2);

    // Check the data was not changed
    actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData)
        .hasSize(3)
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("value")).isEqualTo("initial-value-1");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("value")).isEqualTo("initial-value-2");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(3L);
              assertThat(row.get("value")).isEqualTo("initial-value-3");
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
            });

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSCD2Columns(boolean isPartitioned) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get()),
            Types.NestedField.optional(4, "phone", Types.StringType.get()),
            Types.NestedField.optional(5, "value", Types.DoubleType.get()),
            Types.NestedField.required(6, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(7, "effective_end", Types.TimestampType.withoutZone()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "scd2_columns_test_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine
        .getCatalog()
        .createTable(
            tableId,
            schema,
            isPartitioned
                ? PartitionSpec.builderFor(schema).identity("id").build()
                : PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    // Initial data with all columns populated
    List<Map<String, Object>> initialData =
        Arrays.asList(
            new HashMap<String, Object>() {
              {
                put("id", 1L);
                put("name", "John");
                put("email", "john@example.com");
                put("phone", "123-456-7890");
                put("value", 100.0);
              }
            },
            new HashMap<String, Object>() {
              {
                put("id", 2L);
                put("name", "Jane");
                put("email", "jane@example.com");
                put("phone", "234-567-8901");
                put("value", 200.0);
              }
            });

    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00");

    // Changes data - only updating some columns
    List<Map<String, Object>> changes =
        Arrays.asList(
            Map.of(
                "id", 1L,
                "name", "John Doe",
                "email", "john.doe@example.com",
                "operation_type", "U"),
            Map.of(
                "id", 3L,
                "name", "Bob",
                "email", "bob@example.com",
                "operation_type", "I"));

    String changesSql = TestUtil.createChangeSql(changes, schema);
    LocalDateTime effectiveTimestamp = LocalDateTime.parse("2025-01-02T00:00:00");

    // Apply SCD2 changes with only specific columns
    swiftLakeEngine
        .applyChangesAsSCD2(tableName)
        .tableFilterSql("id IS NOT NULL")
        .sourceSql(changesSql)
        .effectiveTimestamp(effectiveTimestamp)
        .keyColumns(Arrays.asList("id"))
        .operationTypeColumn("operation_type", "D")
        .effectivePeriodColumns("effective_start", "effective_end")
        .columns(Arrays.asList("id", "name", "email")) // Only update these columns
        .processSourceTables(false)
        .execute();

    // Verify the data after SCD2 merge with limited columns
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    assertThat(actualData)
        .hasSize(4) // 3 current records + 1 historical record
        .anySatisfy(
            row -> {
              // Original record of ID 1 (now with effective_end)
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("name")).isEqualTo("John");
              assertThat(row.get("email")).isEqualTo("john@example.com");
              assertThat(row.get("phone")).isEqualTo("123-456-7890");
              assertThat(row.get("value")).isEqualTo(100.0);
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
            })
        .anySatisfy(
            row -> {
              // Updated record of ID 1 - only specified columns should be updated, others preserved
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("name")).isEqualTo("John Doe"); // Updated
              assertThat(row.get("email")).isEqualTo("john.doe@example.com"); // Updated
              assertThat(row.get("phone")).isNull(); // Not specified in columns list
              assertThat(row.get("value")).isNull(); // Not specified in columns list
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            })
        .anySatisfy(
            row -> {
              // Unchanged record of ID 2
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("name")).isEqualTo("Jane");
              assertThat(row.get("email")).isEqualTo("jane@example.com");
              assertThat(row.get("phone")).isEqualTo("234-567-8901");
              assertThat(row.get("value")).isEqualTo(200.0);
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            })
        .anySatisfy(
            row -> {
              // New record of ID 3 - only specified columns should be populated
              assertThat(row.get("id")).isEqualTo(3L);
              assertThat(row.get("name")).isEqualTo("Bob");
              assertThat(row.get("email")).isEqualTo("bob@example.com");
              assertThat(row.get("phone")).isNull(); // Not specified in columns list
              assertThat(row.get("value")).isNull(); // Not specified in columns list
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            });

    // Apply another set of changes to test column behavior more thoroughly
    List<Map<String, Object>> secondChanges =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "phone",
                "555-123-4567", // Different column than before
                "value",
                150.0, // Different column than before
                "operation_type",
                "U"),
            Map.of(
                "id", 2L,
                "phone", "555-234-5678", // Update just the phone
                "operation_type", "U"));

    String secondChangesSql = TestUtil.createChangeSql(secondChanges, schema);
    LocalDateTime secondEffectiveTimestamp = LocalDateTime.parse("2025-01-03T00:00:00");

    // Apply second SCD2 changes with different columns
    swiftLakeEngine
        .applyChangesAsSCD2(tableName)
        .tableFilterSql("id IS NOT NULL")
        .sourceSql(secondChangesSql)
        .effectiveTimestamp(secondEffectiveTimestamp)
        .keyColumns(Arrays.asList("id"))
        .operationTypeColumn("operation_type", "D")
        .effectivePeriodColumns("effective_start", "effective_end")
        .columns(Arrays.asList("id", "phone", "value")) // Different columns than the first update
        .processSourceTables(false)
        .execute();

    // Verify data after second update
    List<Map<String, Object>> finalData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    assertThat(finalData)
        .hasSize(6) // 3 current records + 3 historical records
        .anySatisfy(
            row -> {
              // Original record of ID 1 (with first effective_end)
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("name")).isEqualTo("John");
              assertThat(row.get("email")).isEqualTo("john@example.com");
              assertThat(row.get("phone")).isEqualTo("123-456-7890");
              assertThat(row.get("value")).isEqualTo(100.0);
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
            })
        .anySatisfy(
            row -> {
              // First updated record of ID 1 (now with effective_end)
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("name")).isEqualTo("John Doe");
              assertThat(row.get("email")).isEqualTo("john.doe@example.com");
              assertThat(row.get("phone")).isNull();
              assertThat(row.get("value")).isNull();
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-02T00:00:00"));
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
            })
        .anySatisfy(
            row -> {
              // Second updated record of ID 1 (current)
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("name")).isNull(); // Not in second columns list
              assertThat(row.get("email")).isNull(); // Not in second columns list
              assertThat(row.get("phone")).isEqualTo("555-123-4567"); // Updated
              assertThat(row.get("value")).isEqualTo(150.0); // Updated
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            })
        .anySatisfy(
            row -> {
              // Original record of ID 2 (now with effective_end)
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("name")).isEqualTo("Jane");
              assertThat(row.get("email")).isEqualTo("jane@example.com");
              assertThat(row.get("phone")).isEqualTo("234-567-8901");
              assertThat(row.get("value")).isEqualTo(200.0);
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-01T00:00:00"));
              assertThat(row.get("effective_end"))
                  .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
            })
        .anySatisfy(
            row -> {
              // Updated record of ID 2 (current)
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("name")).isNull(); // Not in second columns list
              assertThat(row.get("email")).isNull(); // Not in second columns list
              assertThat(row.get("phone")).isEqualTo("555-234-5678"); // Updated
              assertThat(row.get("value")).isNull(); // In columns list but not provided
              assertThat(row.get("effective_start"))
                  .isEqualTo(LocalDateTime.parse("2025-01-03T00:00:00"));
              assertThat(row.get("effective_end")).isNull();
            });

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }
}
