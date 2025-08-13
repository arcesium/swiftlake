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
import static org.assertj.core.api.Assertions.fail;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.TestUtil;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import com.arcesium.swiftlake.writer.TableBatchTransaction;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
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
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SCD1MergeAdvancedIntegrationTest {
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
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  void testProcessSourceTables(boolean isPartitioned, boolean isSnapshotMode) {
    TableIdentifier sourceTableId =
        TableIdentifier.of(
            "test_db", "scd1_source_table_" + UUID.randomUUID().toString().replace('-', '_'));
    String sourceTableName = sourceTableId.toString();
    TableIdentifier targetTableId =
        TableIdentifier.of(
            "test_db", "scd1_target_table_" + UUID.randomUUID().toString().replace('-', '_'));
    String targetTableName = targetTableId.toString();

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()));

    // Create source and target tables
    swiftLakeEngine
        .getCatalog()
        .createTable(
            sourceTableId,
            schema,
            isPartitioned
                ? PartitionSpec.builderFor(schema).identity("id").build()
                : PartitionSpec.unpartitioned());
    swiftLakeEngine.getCatalog().createTable(targetTableId, schema, PartitionSpec.unpartitioned());

    // Insert initial data into target table
    List<Map<String, Object>> targetData =
        Arrays.asList(
            Map.of("id", 1L, "name", "Initial", "value", 100.0),
            Map.of("id", 2L, "name", "Initial", "value", 200.0));
    swiftLakeEngine
        .insertInto(targetTableName)
        .sql(TestUtil.createSelectSql(targetData, schema))
        .processSourceTables(false)
        .execute();

    // Insert data into source table
    List<Map<String, Object>> sourceData =
        Arrays.asList(
            Map.of("id", 1L, "name", "Updated", "value", 150.0),
            Map.of("id", 3L, "name", "New", "value", 300.0),
            Map.of("id", 2L, "name", "Initial", "value", 200.0));
    swiftLakeEngine
        .insertInto(sourceTableName)
        .sql(TestUtil.createSelectSql(sourceData, schema))
        .processSourceTables(false)
        .execute();

    if (isSnapshotMode) {
      // Apply SCD1 snapshot with processSourceTables=true
      // For snapshot mode, include all records that should exist
      swiftLakeEngine
          .applySnapshotAsSCD1(targetTableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(
              "SELECT * FROM "
                  + sourceTableName
                  + " WHERE id = 1 UNION ALL SELECT * FROM "
                  + sourceTableName
                  + " WHERE id = 3 UNION ALL SELECT * FROM "
                  + sourceTableName
                  + " WHERE id = 2")
          .keyColumns(Arrays.asList("id"))
          .processSourceTables(true)
          .execute();
    } else {
      // Apply SCD1 changes with processSourceTables=true
      swiftLakeEngine
          .applyChangesAsSCD1(targetTableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(
              "SELECT *, 'U' as operation_type FROM "
                  + sourceTableName
                  + " WHERE id = 1 UNION ALL SELECT *, 'I' as operation_type FROM "
                  + sourceTableName
                  + " WHERE id = 3")
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .processSourceTables(true)
          .execute();
    }

    // Verify the changes were applied correctly
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "Updated", "value", 150.0),
            Map.of("id", 2L, "name", "Initial", "value", 200.0),
            Map.of("id", 3L, "name", "New", "value", 300.0));

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
            "scd1_execute_sql_once_test_" + UUID.randomUUID().toString().replace('-', '_'));

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()));

    Table table =
        swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    // Insert initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "value", "initial-value-1"),
            Map.of("id", 2L, "value", "initial-value-2"),
            Map.of("id", 3L, "value", "initial-value-3"));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    if (isSnapshotMode) {
      // Use snapshot mode where we specify all desired records
      List<Map<String, Object>> snapshot =
          Arrays.asList(
              Map.of("id", 1L, "value", "updated-value-1"),
              Map.of("id", 2L, "value", "initial-value-2"),
              Map.of("id", 4L, "value", "new-value-4"));

      String snapshotSql = TestUtil.createSelectSql(snapshot, schema);

      // Apply SCD1 snapshot with executeSqlOnceOnly=true
      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(snapshotSql)
          .keyColumns(Arrays.asList("id"))
          .executeSourceSqlOnceOnly(true)
          .processSourceTables(false)
          .execute();

    } else {
      // Use changes mode
      List<Map<String, Object>> changes =
          Arrays.asList(
              Map.of("id", 1L, "value", "updated-value-1", "operation_type", "U"),
              Map.of("id", 3L, "value", "to-be-deleted", "operation_type", "D"),
              Map.of("id", 4L, "value", "new-value-4", "operation_type", "I"));

      String changesSql = TestUtil.createChangeSql(changes, schema);

      // Apply SCD1 changes with executeSqlOnceOnly=true
      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(changesSql)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .executeSourceSqlOnceOnly(true)
          .processSourceTables(false)
          .execute();
    }

    // Verify results
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData)
        .hasSize(3)
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("value")).isEqualTo("updated-value-1");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("value")).isEqualTo("initial-value-2");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(4L);
              assertThat(row.get("value")).isEqualTo("new-value-4");
            });

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSourceMybatisStatement(boolean isSnapshotMode) {
    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "scd1_mybatis_test_" + UUID.randomUUID().toString().replace('-', '_'));

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()));

    swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    // Insert initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "Initial", "value", 100.0),
            Map.of("id", 2L, "name", "Initial", "value", 200.0));
    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

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

    if (isSnapshotMode) {
      // Include all desired records in snapshot
      params.put("id3", 2L); // Keep this record from the initial data
      params.put("name3", "Initial");
      params.put("value3", 200.0);

      // Apply SCD1 snapshot using a MyBatis statement
      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceMybatisStatement("TestMapper.getSnapshotData", params)
          .keyColumns(Arrays.asList("id"))
          .processSourceTables(false)
          .sqlSessionFactory(sqlSessionFactory)
          .execute();
    } else {
      // Apply SCD1 changes using a MyBatis statement
      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceMybatisStatement("TestMapper.getChangeData", params)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .processSourceTables(false)
          .sqlSessionFactory(sqlSessionFactory)
          .execute();
    }

    // Verify the changes were applied correctly
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "Updated", "value", 150.0),
            Map.of("id", 2L, "name", "Initial", "value", 200.0),
            Map.of("id", 3L, "name", "New", "value", 300.0));

    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @CsvSource({
    "true, true, true",
    "true, false, true",
    "false, true, true",
    "false, false, true",
    "true, true, false",
    "true, false, false",
    "false, true, false",
    "false, false, false"
  })
  void testTableFilterAndFilterSql(
      boolean isPartitioned, boolean isExpression, boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd1_table_filter_sql_test_" + UUID.randomUUID().toString().replace('-', '_'));

    swiftLakeEngine
        .getCatalog()
        .createTable(
            tableId,
            schema,
            isPartitioned
                ? PartitionSpec.builderFor(schema).identity("region").build()
                : PartitionSpec.unpartitioned());
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
    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    if (isSnapshotMode) {
      // Snapshot data - include all desired records for the US region
      List<Map<String, Object>> snapshotData =
          Arrays.asList(
              Map.of("id", 1L, "region", "US", "value", 110.0), // Updated
              Map.of("id", 2L, "region", "US", "value", 200.0), // No change
              Map.of("id", 3L, "region", "EU", "value", 330.0),
              Map.of("id", 5L, "region", "ASIA", "value", 550.0),
              Map.of("id", 7L, "region", "US", "value", 700.0)); // New record

      String snapshotSql = TestUtil.createSelectSql(snapshotData, schema);

      // Apply SCD1 snapshot with a table filter for only US region
      if (isExpression) {
        swiftLakeEngine
            .applySnapshotAsSCD1(tableName)
            .tableFilter(Expressions.equal("region", "US"))
            .sourceSql(snapshotSql)
            .keyColumns(Arrays.asList("id", "region"))
            .processSourceTables(false)
            .execute();
      } else {
        swiftLakeEngine
            .applySnapshotAsSCD1(tableName)
            .tableFilterSql("region = 'US'")
            .sourceSql(snapshotSql)
            .keyColumns(Arrays.asList("id", "region"))
            .processSourceTables(false)
            .execute();
      }
    } else {
      // Changes data for all regions
      List<Map<String, Object>> changes =
          Arrays.asList(
              Map.of("id", 1L, "region", "US", "value", 110.0, "operation_type", "U"),
              Map.of("id", 3L, "region", "EU", "value", 330.0, "operation_type", "U"),
              Map.of("id", 5L, "region", "ASIA", "value", 550.0, "operation_type", "U"),
              Map.of("id", 7L, "region", "US", "value", 700.0, "operation_type", "I"));
      String changesSql = TestUtil.createChangeSql(changes, schema);

      // Apply SCD1 changes with a table filter for only US region
      if (isExpression) {
        swiftLakeEngine
            .applyChangesAsSCD1(tableName)
            .tableFilter(Expressions.equal("region", "US"))
            .sourceSql(changesSql)
            .keyColumns(Arrays.asList("id", "region"))
            .operationTypeColumn("operation_type", "D")
            .processSourceTables(false)
            .execute();
      } else {
        swiftLakeEngine
            .applyChangesAsSCD1(tableName)
            .tableFilterSql("region = 'US'")
            .sourceSql(changesSql)
            .keyColumns(Arrays.asList("id", "region"))
            .operationTypeColumn("operation_type", "D")
            .processSourceTables(false)
            .execute();
      }
    }

    // Verify only US region records were updated
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "region", "US", "value", 110.0), // Updated
            Map.of("id", 2L, "region", "US", "value", 200.0), // Unchanged
            Map.of("id", 3L, "region", "EU", "value", 300.0), // Unchanged (outside filter)
            Map.of("id", 4L, "region", "EU", "value", 400.0), // Unchanged (outside filter)
            Map.of("id", 5L, "region", "ASIA", "value", 500.0), // Unchanged (outside filter)
            Map.of("id", 6L, "region", "ASIA", "value", 600.0), // Unchanged (outside filter)
            Map.of("id", 7L, "region", "US", "value", 700.0) // Inserted
            );

    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTableFilterColumns(boolean isPartitioned) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()),
            Types.NestedField.required(3, "department", Types.StringType.get()),
            Types.NestedField.required(4, "value", Types.DoubleType.get()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd1_table_filter_columns_test_" + UUID.randomUUID().toString().replace('-', '_'));

    swiftLakeEngine
        .getCatalog()
        .createTable(
            tableId,
            schema,
            isPartitioned
                ? PartitionSpec.builderFor(schema).identity("region").identity("department").build()
                : PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "region", "US", "department", "HR", "value", 100.0),
            Map.of("id", 2L, "region", "US", "department", "IT", "value", 200.0),
            Map.of("id", 3L, "region", "EU", "department", "HR", "value", 300.0),
            Map.of("id", 4L, "region", "EU", "department", "IT", "value", 400.0));
    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Changes data
    List<Map<String, Object>> changes =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "region",
                "US",
                "department",
                "HR",
                "value",
                110.0,
                "operation_type",
                "U"),
            Map.of(
                "id",
                3L,
                "region",
                "EU",
                "department",
                "HR",
                "value",
                330.0,
                "operation_type",
                "U"),
            Map.of(
                "id",
                5L,
                "region",
                "US",
                "department",
                "HR",
                "value",
                500.0,
                "operation_type",
                "I"));
    String changesSql = TestUtil.createChangeSql(changes, schema);

    // Apply SCD1 changes with tableFilterColumns to filter by department="HR"
    assertThatThrownBy(
            () ->
                swiftLakeEngine
                    .applyChangesAsSCD1(tableName)
                    .tableFilterColumns(Arrays.asList("region", "department"))
                    .sourceSql(changesSql)
                    .keyColumns(Arrays.asList("id"))
                    .operationTypeColumn("operation_type", "D")
                    .processSourceTables(false)
                    .execute())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Filter column region is not a key column");

    swiftLakeEngine
        .applyChangesAsSCD1(tableName)
        .tableFilterColumns(Arrays.asList("region", "department"))
        .sourceSql(changesSql)
        .keyColumns(Arrays.asList("id", "region", "department"))
        .operationTypeColumn("operation_type", "D")
        .processSourceTables(false)
        .execute();

    // Verify only HR department records were updated
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "region", "US", "department", "HR", "value", 110.0), // Updated
            Map.of(
                "id",
                2L,
                "region",
                "US",
                "department",
                "IT",
                "value",
                200.0), // Unchanged (outside filter)
            Map.of("id", 3L, "region", "EU", "department", "HR", "value", 330.0), // Updated
            Map.of(
                "id",
                4L,
                "region",
                "EU",
                "department",
                "IT",
                "value",
                400.0), // Unchanged (outside filter)
            Map.of("id", 5L, "region", "US", "department", "HR", "value", 500.0) // Inserted
            );

    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  void testColumns(boolean isPartitioned, boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get()),
            Types.NestedField.optional(4, "phone", Types.StringType.get()),
            Types.NestedField.optional(5, "value", Types.DoubleType.get()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "scd1_columns_test_" + UUID.randomUUID().toString().replace('-', '_'));

    swiftLakeEngine
        .getCatalog()
        .createTable(
            tableId,
            schema,
            isPartitioned
                ? PartitionSpec.builderFor(schema).identity("id").build()
                : PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

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
    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    if (isSnapshotMode) {
      // Changes data - only updating some columns
      List<Map<String, Object>> snapshot =
          Arrays.asList(
              Map.of(
                  "id", 1L,
                  "name", "John Doe",
                  "email", "john.doe@example.com"),
              Map.of(
                  "id", 2L,
                  "name", "Jane",
                  "email", "jane@example.com"),
              Map.of(
                  "id", 3L,
                  "name", "Bob",
                  "email", "bob@example.com"));
      String snapshotSql = TestUtil.createSelectSql(snapshot, schema);

      // Apply SCD1 changes with only specific columns
      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(snapshotSql)
          .keyColumns(Arrays.asList("id"))
          .columns(Arrays.asList("id", "name", "email")) // Only update these columns
          .processSourceTables(false)
          .execute();
    } else {
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

      // Apply SCD1 changes with only specific columns
      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(changesSql)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .columns(Arrays.asList("id", "name", "email")) // Only update these columns
          .processSourceTables(false)
          .execute();
    }

    // Verify only specified columns were updated, others get null values
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            new HashMap<String, Object>() {
              {
                put("id", 1L);
                put("name", "John Doe"); // Updated
                put("email", "john.doe@example.com"); // Updated
                put("phone", null); // Unchanged
                put("value", null); // Unchanged
              }
            },
            new HashMap<String, Object>() {
              {
                put("id", 2L);
                put("name", "Jane");
                put("email", "jane@example.com");
                put("phone", isSnapshotMode ? null : "234-567-8901");
                put("value", isSnapshotMode ? null : 200.0);
              }
            },
            new HashMap<String, Object>() {
              {
                put("id", 3L);
                put("name", "Bob");
                put("email", "bob@example.com");
                put("phone", null); // Not specified in insert
                put("value", null); // Not specified in insert
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
  void testTableBatchTransaction(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "department", Types.StringType.get()),
            Types.NestedField.required(4, "salary", Types.DoubleType.get()));

    TableIdentifier tableId = TableIdentifier.of("test_db", "scd1_batch_transaction_test");
    swiftLakeEngine
        .getCatalog()
        .createTable(
            tableId, schema, PartitionSpec.builderFor(schema).identity("department").build());
    String tableName = tableId.toString();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "department", "HR", "salary", 50000.0),
            Map.of("id", 2L, "name", "Jane", "department", "IT", "salary", 60000.0),
            Map.of("id", 3L, "name", "Bob", "department", "Finance", "salary", 55000.0));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Create multiple batches of changes
    List<List<Map<String, Object>>> changeBatches = new ArrayList<>();
    List<Pair<String, List<Map<String, Object>>>> snapshotBatches = new ArrayList<>();

    if (isSnapshotMode) {
      // Batch 1: Update records
      snapshotBatches.add(
          Pair.of(
              "department IN ('HR', 'IT')",
              Arrays.asList(
                  Map.of("id", 1L, "name", "John Doe", "department", "HR", "salary", 55000.0),
                  Map.of("id", 2L, "name", "Jane Doe", "department", "IT", "salary", 65000.0))));

      // Batch 2: Insert new records
      snapshotBatches.add(
          Pair.of(
              "department IN ('Marketing', 'Sales')",
              Arrays.asList(
                  Map.of("id", 4L, "name", "Alice", "department", "Marketing", "salary", 52000.0),
                  Map.of("id", 5L, "name", "Charlie", "department", "Sales", "salary", 58000.0))));

      // Batch 3: Delete a record
      snapshotBatches.add(Pair.of("department='Finance'", List.of()));
    } else {
      // Batch 1: Update records
      changeBatches.add(
          Arrays.asList(
              Map.of(
                  "id",
                  1L,
                  "name",
                  "John Doe",
                  "department",
                  "HR",
                  "salary",
                  55000.0,
                  "operation_type",
                  "U"),
              Map.of(
                  "id",
                  2L,
                  "name",
                  "Jane Doe",
                  "department",
                  "IT",
                  "salary",
                  65000.0,
                  "operation_type",
                  "U")));

      // Batch 2: Insert new records
      changeBatches.add(
          Arrays.asList(
              Map.of(
                  "id",
                  4L,
                  "name",
                  "Alice",
                  "department",
                  "Marketing",
                  "salary",
                  52000.0,
                  "operation_type",
                  "I"),
              Map.of(
                  "id",
                  5L,
                  "name",
                  "Charlie",
                  "department",
                  "Sales",
                  "salary",
                  58000.0,
                  "operation_type",
                  "I")));

      // Batch 3: Delete a record
      changeBatches.add(
          Arrays.asList(
              Map.of(
                  "id",
                  3L,
                  "name",
                  "Bob",
                  "department",
                  "Finance",
                  "salary",
                  55000.0,
                  "operation_type",
                  "D")));
    }

    // Create a TableBatchTransaction
    TableBatchTransaction transaction =
        TableBatchTransaction.builderFor(swiftLakeEngine, tableName).build();

    // Process batches in parallel using multiple threads
    ExecutorService executorService =
        Executors.newFixedThreadPool(
            isSnapshotMode ? snapshotBatches.size() : changeBatches.size());
    List<Future<?>> futures = new ArrayList<>();

    if (isSnapshotMode) {
      for (Pair<String, List<Map<String, Object>>> batch : snapshotBatches) {
        futures.add(
            executorService.submit(
                () -> {
                  String snapshotSql = TestUtil.createSelectSql(batch.getRight(), schema);
                  swiftLakeEngine
                      .applySnapshotAsSCD1(transaction)
                      .tableFilterSql(batch.getLeft())
                      .sourceSql(snapshotSql)
                      .keyColumns(Arrays.asList("id", "department"))
                      .processSourceTables(false)
                      .execute();
                }));
      }
    } else {
      for (List<Map<String, Object>> batch : changeBatches) {
        futures.add(
            executorService.submit(
                () -> {
                  String changeSql = TestUtil.createChangeSql(batch, schema);
                  swiftLakeEngine
                      .applyChangesAsSCD1(transaction)
                      .tableFilterColumns(Arrays.asList("department"))
                      .sourceSql(changeSql)
                      .keyColumns(Arrays.asList("id", "department"))
                      .operationTypeColumn("operation_type", "D")
                      .processSourceTables(false)
                      .execute();
                }));
      }
    }
    // Wait for all batches to complete
    for (Future<?> future : futures) {
      try {
        future.get(60, TimeUnit.SECONDS);
      } catch (Exception e) {
        fail("Transaction batch failed: " + e.getMessage());
      }
    }

    executorService.shutdown();

    // Commit the transaction
    transaction.commit();

    // Verify the results
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe", "department", "HR", "salary", 55000.0),
            Map.of("id", 2L, "name", "Jane Doe", "department", "IT", "salary", 65000.0),
            Map.of("id", 4L, "name", "Alice", "department", "Marketing", "salary", 52000.0),
            Map.of("id", 5L, "name", "Charlie", "department", "Sales", "salary", 58000.0)
            // ID 3 should be deleted
            );

    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testBranch(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()));

    TableIdentifier tableId = TableIdentifier.of("test_db", "scd1_branch_test");
    swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();
    Table table = swiftLakeEngine.getTable(tableName);

    // Insert initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "Initial1", "value", 100.0),
            Map.of("id", 2L, "name", "Initial2", "value", 200.0));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    table.refresh();
    // Get the main branch snapshot ID
    long mainSnapshotId = table.currentSnapshot().snapshotId();

    // Create a new branch called "test-branch"
    String branchName = "test-branch";
    table.manageSnapshots().createBranch(branchName, mainSnapshotId).commit();

    // Apply changes on the main branch
    if (isSnapshotMode) {
      List<Map<String, Object>> snapshot =
          Arrays.asList(
              Map.of("id", 1L, "name", "Updated-Main", "value", 150.0),
              Map.of("id", 3L, "name", "New-Main", "value", 300.0),
              Map.of("id", 2L, "name", "Initial2", "value", 200.0));

      String snapshotSql = TestUtil.createSelectSql(snapshot, schema);
      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(snapshotSql)
          .keyColumns(Arrays.asList("id"))
          .processSourceTables(false)
          .execute();
    } else {
      List<Map<String, Object>> mainChanges =
          Arrays.asList(
              Map.of("id", 1L, "name", "Updated-Main", "value", 150.0, "operation_type", "U"),
              Map.of("id", 3L, "name", "New-Main", "value", 300.0, "operation_type", "I"));

      String mainChangesSql = TestUtil.createChangeSql(mainChanges, schema);
      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(mainChangesSql)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .processSourceTables(false)
          .execute();
    }

    // Apply different changes on the test-branch
    if (isSnapshotMode) {
      List<Map<String, Object>> branchSnapshot =
          Arrays.asList(
              Map.of("id", 1L, "name", "Updated-Branch", "value", 160.0),
              Map.of("id", 4L, "name", "New-Branch", "value", 400.0),
              Map.of("id", 2L, "name", "Initial2", "value", 200.0));

      String branchSnapshotSql = TestUtil.createSelectSql(branchSnapshot, schema);
      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(branchSnapshotSql)
          .keyColumns(Arrays.asList("id"))
          .branch(branchName)
          .processSourceTables(false)
          .execute();
    } else {
      List<Map<String, Object>> branchChanges =
          Arrays.asList(
              Map.of("id", 1L, "name", "Updated-Branch", "value", 160.0, "operation_type", "U"),
              Map.of("id", 4L, "name", "New-Branch", "value", 400.0, "operation_type", "I"));

      String branchChangesSql = TestUtil.createChangeSql(branchChanges, schema);
      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(branchChangesSql)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .branch(branchName)
          .processSourceTables(false)
          .execute();
    }
    // Check main branch data
    List<Map<String, Object>> mainBranchData =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(mainBranchData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(
            Arrays.asList(
                Map.of("id", 1L, "name", "Updated-Main", "value", 150.0),
                Map.of("id", 2L, "name", "Initial2", "value", 200.0),
                Map.of("id", 3L, "name", "New-Main", "value", 300.0)));

    // Check test-branch data
    List<Map<String, Object>> branchData =
        TestUtil.getRecordsFromTable(
            swiftLakeEngine, TestUtil.getTableNameForBranch(tableId, branchName));

    assertThat(branchData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(
            Arrays.asList(
                Map.of("id", 1L, "name", "Updated-Branch", "value", 160.0),
                Map.of("id", 2L, "name", "Initial2", "value", 200.0),
                Map.of("id", 4L, "name", "New-Branch", "value", 400.0)));

    if (isSnapshotMode) {
      List<Map<String, Object>> finalBranchSnapshot =
          Arrays.asList(
              Map.of("id", 2L, "name", "Updated-Final", "value", 250.0),
              Map.of("id", 1L, "name", "Updated-Branch", "value", 160.0),
              Map.of("id", 4L, "name", "New-Branch", "value", 400.0));

      String finalSnapshotSql = TestUtil.createSelectSql(finalBranchSnapshot, schema);
      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(finalSnapshotSql)
          .keyColumns(Arrays.asList("id"))
          .branch(branchName) // Use the branch for input
          .processSourceTables(false)
          .execute();
    } else {
      List<Map<String, Object>> finalChanges =
          Arrays.asList(
              Map.of("id", 2L, "name", "Updated-Final", "value", 250.0, "operation_type", "U"));

      String finalChangesSql = TestUtil.createChangeSql(finalChanges, schema);
      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(finalChangesSql)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .branch(branchName) // Use the branch for input
          .processSourceTables(false)
          .execute();
    }

    // Verify changes were applied to the branch
    List<Map<String, Object>> finalBranchData =
        TestUtil.getRecordsFromTable(
            swiftLakeEngine, TestUtil.getTableNameForBranch(tableId, branchName));

    assertThat(finalBranchData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(
            Arrays.asList(
                Map.of("id", 1L, "name", "Updated-Branch", "value", 160.0),
                Map.of("id", 2L, "name", "Updated-Final", "value", 250.0),
                Map.of("id", 4L, "name", "New-Branch", "value", 400.0)));

    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  void testSkipDataSortingSCD1(boolean isPartitioned, boolean isSnapshotMode) throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "sort_key", Types.IntegerType.get()),
            Types.NestedField.required(3, "value", Types.StringType.get()));

    SortOrder sortOrder = SortOrder.builderFor(schema).asc("sort_key").build();
    PartitionSpec partitionSpec =
        isPartitioned
            ? PartitionSpec.builderFor(schema).bucket("id", 1).build()
            : PartitionSpec.unpartitioned();

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd1_skip_data_sorting_test_" + UUID.randomUUID().toString().replace('-', '_'));

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

    // Create SQL for inserting the initial data
    StringBuilder initialSqlBuilder = new StringBuilder("SELECT * FROM (VALUES ");
    String initialValuesSql =
        initialRecords.stream()
            .map(
                r ->
                    String.format("(%d, %d, '%s')", r.get("id"), r.get("sort_key"), r.get("value")))
            .collect(Collectors.joining(","));
    initialSqlBuilder.append(initialValuesSql);
    initialSqlBuilder.append(") AS t(id, sort_key, value)");

    // Insert initial data with skipDataSorting=true to ensure it's not sorted
    swiftLakeEngine
        .insertInto(tableName)
        .sql(initialSqlBuilder.toString())
        .skipDataSorting(true)
        .processSourceTables(false)
        .execute();

    // Prepare SCD1 changes (update some records, add new ones)
    List<Map<String, Object>> changes = new ArrayList<>();
    List<Map<String, Object>> snapshot = new ArrayList<>();
    // Updates for some existing records
    for (int i = 10; i <= 40; i += 10) {
      Map<String, Object> record = new HashMap<>();
      record.put("id", i);
      record.put("sort_key", i);
      record.put("value", "updated_" + String.format("%03d", i));
      record.put("operation_type", "U");
      changes.add(record);
      snapshot.add(record);
    }

    // New records with higher IDs in reverse order
    for (int i = 100; i > 50; i -= 10) {
      Map<String, Object> record = new HashMap<>();
      record.put("id", i);
      record.put("sort_key", i);
      record.put("value", "new_" + String.format("%03d", i));
      record.put("operation_type", "I");
      changes.add(record);
      snapshot.add(record);
    }

    // Expected data after merge
    List<Map<String, Object>> expectedRecords = new ArrayList<>();

    // Add remaining original records
    for (int i = 1; i <= 50; i++) {
      if (i % 10 != 0 || i > 40) { // Skip records that will be updated
        Map<String, Object> record = new HashMap<>();
        record.put("id", i);
        record.put("sort_key", i);
        record.put("value", "initial_" + String.format("%03d", i));
        expectedRecords.add(record);
        snapshot.add(record);
      }
    }

    // Add updated records
    for (int i = 10; i <= 40; i += 10) {
      Map<String, Object> record = new HashMap<>();
      record.put("id", i);
      record.put("sort_key", i);
      record.put("value", "updated_" + String.format("%03d", i));
      expectedRecords.add(record);
    }

    // Add new records
    for (int i = 60; i <= 100; i += 10) {
      Map<String, Object> record = new HashMap<>();
      record.put("id", i);
      record.put("sort_key", i);
      record.put("value", "new_" + String.format("%03d", i));
      expectedRecords.add(record);
    }

    // Create a sorted copy of the expected records for comparison
    List<Map<String, Object>> expectedSortedRecords = new ArrayList<>(expectedRecords);
    expectedSortedRecords.sort(Comparator.comparing(m -> (Integer) m.get("sort_key")));

    String sourceSql =
        isSnapshotMode
            ? TestUtil.createSelectSql(snapshot, schema)
            : TestUtil.createChangeSql(changes, schema);
    if (isSnapshotMode) {
      // Apply SCD1 snapshot with skipDataSorting=true
      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(sourceSql)
          .keyColumns(Arrays.asList("id"))
          .skipDataSorting(true)
          .processSourceTables(false)
          .execute();
    } else {
      // Apply SCD1 changes with skipDataSorting=true
      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(sourceSql)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
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
    swiftLakeEngine
        .insertInto(tableName)
        .sql(initialSqlBuilder.toString())
        .skipDataSorting(true)
        .processSourceTables(false)
        .execute();

    if (isSnapshotMode) {
      // Apply SCD1 snapshot with skipDataSorting=false
      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(sourceSql)
          .keyColumns(Arrays.asList("id"))
          .skipDataSorting(false)
          .processSourceTables(false)
          .execute();
    } else {
      // Apply SCD1 changes with skipDataSorting=false
      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(sourceSql)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
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
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("file_name", "file_row_number")
        .containsExactlyInAnyOrderElementsOf(expectedRecords)
        .as("All records should be present in the table after merge with skipDataSorting=false");

    // With skipDataSorting=false, the data should match exactly the sorted order
    assertThat(isSortedWithoutSkip).isTrue();

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSCD1MergeWithDifferentIsolationLevels(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "year", Types.IntegerType.get()),
            Types.NestedField.required(3, "value", Types.StringType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("year").build();

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "scd1_isolation_test_" + UUID.randomUUID().toString().replace('-', '_'));
    Table table = swiftLakeEngine.getCatalog().createTable(tableId, schema, spec);
    String tableName = tableId.toString();

    // Insert initial data: two records with different years
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "year", 2023, "value", "initial_2023_value"),
            Map.of("id", 2L, "year", 2024, "value", "initial_2024_value"));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Refresh and capture the table state after initial insert
    table.refresh();
    Table tableStateAfterInitialInsert = swiftLakeEngine.getTable(tableName);
    long snapshotIdAfterInitialInsert = tableStateAfterInitialInsert.currentSnapshot().snapshotId();

    // Insert another record
    List<Map<String, Object>> secondInsertData =
        Arrays.asList(Map.of("id", 3L, "year", 2024, "value", "second_2024_value"));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(secondInsertData, schema))
        .processSourceTables(false)
        .execute();

    // Refresh and capture table states after second insert
    table.refresh();
    Table tableStateAfterSecondInsert = swiftLakeEngine.getTable(tableName);
    Table tableStateAfterSecondInsertCopy = swiftLakeEngine.getTable(tableName);

    // Verify second insert created a new snapshot
    assertThat(tableStateAfterSecondInsert.currentSnapshot().snapshotId())
        .isNotEqualTo(snapshotIdAfterInitialInsert);

    // Prepare SCD1 changes that will update ID=1 and insert a new record
    if (isSnapshotMode) {
      List<Map<String, Object>> initialChanges =
          Arrays.asList(
              Map.of("id", 1L, "year", 2023, "value", "updated_2023_value"),
              Map.of("id", 4L, "year", 2025, "value", "new_2025_value"));
      String initialSnapshotSql = TestUtil.createSelectSql(initialChanges, schema);

      // Attempt to apply snapshot with default isolation level should succeed
      // since we're not targeting the same partition that has been modified
      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(initialSnapshotSql)
          .keyColumns(Arrays.asList("id"))
          .processSourceTables(false)
          .execute();

      // Prepare snapshot that will affect the 2024 partition (which has conflicts)
      List<Map<String, Object>> conflictingSnapshot =
          Arrays.asList(Map.of("id", 2L, "year", 2024, "value", "updated_2024_value"));
      String conflictingSnapshotSql = TestUtil.createSelectSql(conflictingSnapshot, schema);

      // Attempting to apply snapshot to the 2024 partition using the old table state with default
      // isolation should fail due to conflicting files
      assertThatThrownBy(
              () -> {
                swiftLakeEngine
                    .applySnapshotAsSCD1(tableStateAfterInitialInsert)
                    .tableFilterSql("year = 2024")
                    .sourceSql(conflictingSnapshotSql)
                    .keyColumns(Arrays.asList("id"))
                    .processSourceTables(false)
                    .execute();
              })
          .hasMessageContaining("Found conflicting files");

      // Use SNAPSHOT isolation with the table at first snapshot state
      swiftLakeEngine
          .applySnapshotAsSCD1(tableStateAfterInitialInsert)
          .tableFilterSql("year = 2024")
          .sourceSql(conflictingSnapshotSql)
          .keyColumns(Arrays.asList("id"))
          .isolationLevel(IsolationLevel.SNAPSHOT)
          .processSourceTables(false)
          .execute();

    } else {
      List<Map<String, Object>> initialChanges =
          Arrays.asList(
              Map.of("id", 1L, "year", 2023, "value", "updated_2023_value", "operation_type", "U"),
              Map.of("id", 4L, "year", 2025, "value", "new_2025_value", "operation_type", "I"));
      String initialChangesSql = TestUtil.createChangeSql(initialChanges, schema);

      // Attempt to apply changes with default isolation level should succeed
      // since we're not targeting the same partition that has been modified
      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(initialChangesSql)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .processSourceTables(false)
          .execute();

      // Prepare changes that will affect the 2024 partition (which has conflicts)
      List<Map<String, Object>> conflictingChanges =
          Arrays.asList(
              Map.of("id", 2L, "year", 2024, "value", "updated_2024_value", "operation_type", "U"),
              Map.of("id", 3L, "year", 2024, "value", "deleted_value", "operation_type", "D"));
      String conflictingChangesSql = TestUtil.createChangeSql(conflictingChanges, schema);

      // Attempting to apply changes to the 2024 partition using the old table state with default
      // isolation should fail due to conflicting files
      assertThatThrownBy(
              () -> {
                swiftLakeEngine
                    .applyChangesAsSCD1(tableStateAfterInitialInsert)
                    .tableFilterSql("year = 2024")
                    .sourceSql(conflictingChangesSql)
                    .keyColumns(Arrays.asList("id"))
                    .operationTypeColumn("operation_type", "D")
                    .processSourceTables(false)
                    .execute();
              })
          .hasMessageContaining("Found conflicting files");

      // Use SNAPSHOT isolation with the table at first snapshot state
      swiftLakeEngine
          .applyChangesAsSCD1(tableStateAfterInitialInsert)
          .tableFilterSql("year = 2024")
          .sourceSql(conflictingChangesSql)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .isolationLevel(IsolationLevel.SNAPSHOT)
          .processSourceTables(false)
          .execute();
    }

    // Refresh and verify the operation succeeded with a new snapshot
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId())
        .isNotEqualTo(tableStateAfterSecondInsert.currentSnapshot().snapshotId());

    if (isSnapshotMode) {
      List<Map<String, Object>> moreChanges =
          Arrays.asList(Map.of("id", 2L, "year", 2024, "value", "updated_again_2024_value"));
      String moreChangesSnapshotSql = TestUtil.createSelectSql(moreChanges, schema);

      // Attempting with SNAPSHOT isolation on outdated state should also fail
      assertThatThrownBy(
              () -> {
                swiftLakeEngine
                    .applySnapshotAsSCD1(tableStateAfterSecondInsert)
                    .tableFilterSql("year = 2024")
                    .sourceSql(moreChangesSnapshotSql)
                    .keyColumns(Arrays.asList("id"))
                    .isolationLevel(IsolationLevel.SNAPSHOT)
                    .processSourceTables(false)
                    .execute();
              })
          .hasMessageContaining("Missing required files to delete");

      // Attempting with SERIALIZABLE isolation on outdated state should also fail
      assertThatThrownBy(
              () -> {
                swiftLakeEngine
                    .applySnapshotAsSCD1(tableStateAfterSecondInsertCopy)
                    .tableFilterSql("year = 2024")
                    .sourceSql(moreChangesSnapshotSql)
                    .keyColumns(Arrays.asList("id"))
                    .isolationLevel(IsolationLevel.SERIALIZABLE)
                    .processSourceTables(false)
                    .execute();
              })
          .hasMessageContaining("Found conflicting files");

      // Use SNAPSHOT isolation with current table state
      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("year = 2024")
          .sourceSql(moreChangesSnapshotSql)
          .keyColumns(Arrays.asList("id"))
          .isolationLevel(IsolationLevel.SNAPSHOT)
          .processSourceTables(false)
          .execute();
    } else {
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
      String moreChangesSql = TestUtil.createChangeSql(moreChanges, schema);

      // Attempting with SNAPSHOT isolation on outdated state should also fail
      assertThatThrownBy(
              () -> {
                swiftLakeEngine
                    .applyChangesAsSCD1(tableStateAfterSecondInsert)
                    .tableFilterSql("year = 2024")
                    .sourceSql(moreChangesSql)
                    .keyColumns(Arrays.asList("id"))
                    .operationTypeColumn("operation_type", "D")
                    .isolationLevel(IsolationLevel.SNAPSHOT)
                    .processSourceTables(false)
                    .execute();
              })
          .hasMessageContaining("Missing required files to delete");

      // Attempting with SERIALIZABLE isolation on outdated state should also fail
      assertThatThrownBy(
              () -> {
                swiftLakeEngine
                    .applyChangesAsSCD1(tableStateAfterSecondInsertCopy)
                    .tableFilterSql("year = 2024")
                    .sourceSql(moreChangesSql)
                    .keyColumns(Arrays.asList("id"))
                    .operationTypeColumn("operation_type", "D")
                    .isolationLevel(IsolationLevel.SERIALIZABLE)
                    .processSourceTables(false)
                    .execute();
              })
          .hasMessageContaining("Found conflicting files");

      // Use SNAPSHOT isolation with current table state
      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("year = 2024")
          .sourceSql(moreChangesSql)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .isolationLevel(IsolationLevel.SNAPSHOT)
          .processSourceTables(false)
          .execute();
    }

    // Verify we have latest data
    table.refresh();
    long updatedSnapshotId = table.currentSnapshot().snapshotId();

    // Read and verify the data after SNAPSHOT isolation changes
    List<Map<String, Object>> dataAfterSnapshotChanges =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    assertThat(dataAfterSnapshotChanges)
        .hasSize(3)
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("year")).isEqualTo(2023);
              assertThat(row.get("value")).isEqualTo("updated_2023_value");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("value"))
                  .isEqualTo("updated_again_2024_value"); // Updated in last operation
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(4L);
              assertThat(row.get("year")).isEqualTo(2025);
              assertThat(row.get("value")).isEqualTo("new_2025_value");
            });

    // Use SERIALIZABLE isolation with current table state for final changes
    if (isSnapshotMode) {
      List<Map<String, Object>> serializableChanges =
          Arrays.asList(
              Map.of("id", 7L, "year", 2024, "value", "serializable_2024_value"),
              Map.of("id", 2L, "year", 2024, "value", "final_update_value"));
      String serializableChangesSnapshotSql = TestUtil.createSelectSql(serializableChanges, schema);

      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("year = 2024")
          .sourceSql(serializableChangesSnapshotSql)
          .keyColumns(Arrays.asList("id"))
          .isolationLevel(IsolationLevel.SERIALIZABLE)
          .processSourceTables(false)
          .execute();
    } else {
      List<Map<String, Object>> serializableChanges =
          Arrays.asList(
              Map.of(
                  "id",
                  7L,
                  "year",
                  2024,
                  "value",
                  "serializable_2024_value",
                  "operation_type",
                  "I"),
              Map.of("id", 2L, "year", 2024, "value", "final_update_value", "operation_type", "U"));
      String serializableChangesSql = TestUtil.createChangeSql(serializableChanges, schema);

      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("year = 2024")
          .sourceSql(serializableChangesSql)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .isolationLevel(IsolationLevel.SERIALIZABLE)
          .processSourceTables(false)
          .execute();
    }
    // The operation should create a new snapshot
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(updatedSnapshotId);

    // Verify final data after all operations
    List<Map<String, Object>> finalData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(finalData)
        .hasSize(4)
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("year")).isEqualTo(2023);
              assertThat(row.get("value")).isEqualTo("updated_2023_value");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("value"))
                  .isEqualTo("final_update_value"); // Updated in last operation
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(4L);
              assertThat(row.get("year")).isEqualTo(2025);
              assertThat(row.get("value")).isEqualTo("new_2025_value");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(7L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("value"))
                  .isEqualTo("serializable_2024_value"); // Added in last operation
            });

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSCD1SnapshotMetadata(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd1_snapshot_metadata_test_" + UUID.randomUUID().toString().replace('-', '_'));
    Table table =
        swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "value", "initial-value-1"),
            Map.of("id", 2L, "value", "initial-value-2"),
            Map.of("id", 3L, "value", "initial-value-3"));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Apply SCD1 merge with snapshot metadata
    Map<String, String> metadata = new HashMap<>();
    metadata.put("user", "scd1-user");
    metadata.put("source", "scd1-test-system");
    metadata.put("timestamp", LocalDateTime.now().toString());

    if (isSnapshotMode) {
      // Prepare SCD1 changes for update and insert with snapshot metadata
      List<Map<String, Object>> snapshotData =
          Arrays.asList(
              Map.of("id", 1L, "value", "updated-value-1"),
              Map.of("id", 4L, "value", "new-value-4"),
              Map.of("id", 2L, "value", "initial-value-2"));

      String sourceSql = TestUtil.createSelectSql(snapshotData, schema);
      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(sourceSql)
          .keyColumns(Arrays.asList("id"))
          .snapshotMetadata(metadata)
          .processSourceTables(false)
          .execute();
    } else {
      // Prepare SCD1 changes for update and insert with snapshot metadata
      List<Map<String, Object>> changes =
          Arrays.asList(
              Map.of("id", 1L, "value", "updated-value-1", "operation_type", "U"),
              Map.of("id", 3L, "value", "to-be-deleted", "operation_type", "D"),
              Map.of("id", 4L, "value", "new-value-4", "operation_type", "I"));

      String changesSql = TestUtil.createChangeSql(changes, schema);
      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(changesSql)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .snapshotMetadata(metadata)
          .processSourceTables(false)
          .execute();
    }
    // Verify the snapshot contains our custom metadata
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();

    Map<String, String> snapshotMetadata = snapshot.summary();
    assertThat(snapshotMetadata)
        .containsEntry("user", "scd1-user")
        .containsEntry("source", "scd1-test-system")
        .containsKey("timestamp");

    // Check the data was correctly modified
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData)
        .hasSize(3)
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("value")).isEqualTo("updated-value-1");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("value")).isEqualTo("initial-value-2");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(4L);
              assertThat(row.get("value")).isEqualTo("new-value-4");
            });

    // Apply second SCD1 merge with different metadata
    Map<String, String> metadata2 = new HashMap<>();
    metadata2.put("user", "another-scd1-user");
    metadata2.put("transaction-id", UUID.randomUUID().toString());
    metadata2.put("operation-count", "2");
    metadata2.put("date", LocalDate.now().toString());

    if (isSnapshotMode) {
      // Apply another set of changes with different metadata
      List<Map<String, Object>> snapshotData2 =
          Arrays.asList(
              Map.of("id", 1L, "value", "updated-value-1"),
              Map.of("id", 4L, "value", "new-value-4"),
              Map.of("id", 2L, "value", "updated-value-2"),
              Map.of("id", 5L, "value", "new-value-5"));

      String sourceSql2 = TestUtil.createSelectSql(snapshotData2, schema);

      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(sourceSql2)
          .keyColumns(Arrays.asList("id"))
          .snapshotMetadata(metadata2)
          .processSourceTables(false)
          .execute();
    } else {
      // Apply another set of changes with different metadata
      List<Map<String, Object>> changes2 =
          Arrays.asList(
              Map.of("id", 2L, "value", "updated-value-2", "operation_type", "U"),
              Map.of("id", 5L, "value", "new-value-5", "operation_type", "I"));

      String changesSql2 = TestUtil.createChangeSql(changes2, schema);

      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(changesSql2)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .snapshotMetadata(metadata2)
          .processSourceTables(false)
          .execute();
    }
    // Verify the new snapshot has the updated metadata
    table.refresh();
    Snapshot newSnapshot = table.currentSnapshot();
    assertThat(newSnapshot.snapshotId()).isNotEqualTo(snapshot.snapshotId());

    Map<String, String> newMetadata = newSnapshot.summary();
    assertThat(newMetadata)
        .containsEntry("user", "another-scd1-user")
        .containsEntry("operation-count", "2")
        .containsKey("transaction-id")
        .containsKey("date");

    // Also verify that the previous metadata is not present in the new snapshot
    assertThat(newMetadata).doesNotContainKey("source");

    // Verify the data after second operation
    List<Map<String, Object>> finalData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(finalData)
        .hasSize(4)
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(1L);
              assertThat(row.get("value")).isEqualTo("updated-value-1");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("value")).isEqualTo("updated-value-2");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(4L);
              assertThat(row.get("value")).isEqualTo("new-value-4");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(5L);
              assertThat(row.get("value")).isEqualTo("new-value-5");
            });

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }
}
