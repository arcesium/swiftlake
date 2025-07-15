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
import com.arcesium.swiftlake.writer.TableBatchTransaction;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DeleteIntegrationTest {
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
  @MethodSource("provideTestCases")
  void testDelete(
      String testName,
      Schema schema,
      PartitionSpec partitionSpec,
      List<Map<String, Object>> initialData,
      String deleteCondition,
      List<Map<String, Object>> expectedData) {
    // Create table
    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table_" + testName);
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    // Insert initial data
    String initialInsertSql = TestUtil.createSelectSql(initialData, schema);
    swiftLakeEngine.insertInto(tableName).sql(initialInsertSql).execute();

    // Perform delete operation
    swiftLakeEngine.deleteFrom(tableName).conditionSql(deleteCondition).execute();

    // Verify results
    List<Map<String, Object>> actualData =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableId.toString());

    assertThat(actualData).containsExactlyInAnyOrderElementsOf(expectedData);
  }

  private static Stream<Arguments> provideTestCases() {
    return Stream.of(
        simpleDeleteTestCase(),
        complexSchemaDeleteTestCase(),
        partitionedTableDeleteTestCase(),
        largeVolumeDeleteTestCase(),
        conditionalDeleteTestCase(),
        deleteAllRecordsTestCase(),
        deleteWithComplexConditionTestCase(),
        deleteNonExistentRecordsTestCase());
  }

  private static Arguments simpleDeleteTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));

    String deleteCondition = "id = 2";

    List<Map<String, Object>> expectedData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 3L, "name", "Bob"));

    return Arguments.of(
        "simple_delete", schema, partitionSpec, initialData, deleteCondition, expectedData);
  }

  private static Arguments complexSchemaDeleteTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(4, "salary", Types.DecimalType.of(10, 2)),
            Types.NestedField.required(5, "department", Types.StringType.get()),
            Types.NestedField.required(6, "hire_date", Types.DateType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "age",
                30,
                "salary",
                new BigDecimal("50000.00"),
                "department",
                "IT",
                "hire_date",
                LocalDate.parse("2020-01-01")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "age",
                28,
                "salary",
                new BigDecimal("55000.00"),
                "department",
                "HR",
                "hire_date",
                LocalDate.parse("2021-01-01")),
            Map.of(
                "id",
                3L,
                "name",
                "Bob",
                "age",
                35,
                "salary",
                new BigDecimal("60000.00"),
                "department",
                "Sales",
                "hire_date",
                LocalDate.parse("2019-01-01")));

    String deleteCondition = "department = 'HR' OR salary > 55000.00";

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "age",
                30,
                "salary",
                new BigDecimal("50000.00"),
                "department",
                "IT",
                "hire_date",
                LocalDate.parse("2020-01-01")));

    return Arguments.of(
        "complex_schema_delete", schema, partitionSpec, initialData, deleteCondition, expectedData);
  }

  private static Arguments partitionedTableDeleteTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "department", Types.StringType.get()),
            Types.NestedField.required(4, "hire_date", Types.DateType.get()));
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("department").year("hire_date").build();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "department",
                "IT",
                "hire_date",
                LocalDate.parse("2020-01-01")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "department",
                "HR",
                "hire_date",
                LocalDate.parse("2021-01-01")),
            Map.of(
                "id",
                3L,
                "name",
                "Bob",
                "department",
                "IT",
                "hire_date",
                LocalDate.parse("2020-06-01")),
            Map.of(
                "id",
                4L,
                "name",
                "Alice",
                "department",
                "Sales",
                "hire_date",
                LocalDate.parse("2021-03-01")));

    String deleteCondition =
        "department = 'IT' AND hire_date BETWEEN DATE'2020-01-01' AND DATE'2020-12-31'";

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "department",
                "HR",
                "hire_date",
                LocalDate.parse("2021-01-01")),
            Map.of(
                "id",
                4L,
                "name",
                "Alice",
                "department",
                "Sales",
                "hire_date",
                LocalDate.parse("2021-03-01")));

    return Arguments.of(
        "partitioned_table_delete",
        schema,
        partitionSpec,
        initialData,
        deleteCondition,
        expectedData);
  }

  private static Arguments largeVolumeDeleteTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()));
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).bucket("id", 10).build();

    List<Map<String, Object>> initialData =
        IntStream.range(0, 10000)
            .mapToObj(i -> Map.of("id", (Object) (long) i, "name", "Name" + i, "value", (double) i))
            .collect(Collectors.toList());

    String deleteCondition = "(id BETWEEN 1 AND 100) OR (id > 500 AND id <= 1000)";

    List<Map<String, Object>> expectedData =
        IntStream.range(0, 10000)
            .filter(i -> !((i >= 1 && i <= 100) || (i > 500 && i <= 1000)))
            .mapToObj(i -> Map.of("id", (Object) (long) i, "name", "Name" + i, "value", (double) i))
            .collect(Collectors.toList());

    return Arguments.of(
        "large_volume_delete", schema, partitionSpec, initialData, deleteCondition, expectedData);
  }

  private static Arguments conditionalDeleteTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()),
            Types.NestedField.required(4, "active", Types.BooleanType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "age", 30, "active", true),
            Map.of("id", 2L, "name", "Jane", "age", 28, "active", true),
            Map.of("id", 3L, "name", "Bob", "age", 35, "active", false),
            Map.of("id", 4L, "name", "Alice", "age", 32, "active", true));

    String deleteCondition = "age > 30 AND active = true";

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "age", 30, "active", true),
            Map.of("id", 2L, "name", "Jane", "age", 28, "active", true),
            Map.of("id", 3L, "name", "Bob", "age", 35, "active", false));

    return Arguments.of(
        "conditional_delete", schema, partitionSpec, initialData, deleteCondition, expectedData);
  }

  private static Arguments deleteAllRecordsTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));

    String deleteCondition = "id IS NOT NULL";

    List<Map<String, Object>> expectedData = new ArrayList<>();

    return Arguments.of(
        "delete_all_records", schema, partitionSpec, initialData, deleteCondition, expectedData);
  }

  private static Arguments deleteWithComplexConditionTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "department", Types.StringType.get()),
            Types.NestedField.required(4, "salary", Types.DecimalType.of(10, 2)),
            Types.NestedField.required(5, "hire_date", Types.DateType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "department",
                "IT",
                "salary",
                new BigDecimal("50000.00"),
                "hire_date",
                LocalDate.parse("2020-01-01")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "department",
                "HR",
                "salary",
                new BigDecimal("55000.00"),
                "hire_date",
                LocalDate.parse("2021-01-01")),
            Map.of(
                "id",
                3L,
                "name",
                "Bob",
                "department",
                "Sales",
                "salary",
                new BigDecimal("60000.00"),
                "hire_date",
                LocalDate.parse("2019-01-01")),
            Map.of(
                "id",
                4L,
                "name",
                "Alice",
                "department",
                "IT",
                "salary",
                new BigDecimal("52000.00"),
                "hire_date",
                LocalDate.parse("2022-01-01")));

    String deleteCondition =
        "(department = 'IT' AND salary > 51000.00) OR (department = 'Sales' AND hire_date < DATE'2020-01-01')";

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "department",
                "IT",
                "salary",
                new BigDecimal("50000.00"),
                "hire_date",
                LocalDate.parse("2020-01-01")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "department",
                "HR",
                "salary",
                new BigDecimal("55000.00"),
                "hire_date",
                LocalDate.parse("2021-01-01")));

    return Arguments.of(
        "delete_with_complex_condition",
        schema,
        partitionSpec,
        initialData,
        deleteCondition,
        expectedData);
  }

  private static Arguments deleteNonExistentRecordsTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));

    String deleteCondition = "id < -1000";

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));

    return Arguments.of(
        "delete_non_existent_records",
        schema,
        partitionSpec,
        initialData,
        deleteCondition,
        expectedData);
  }

  @Test
  void testDeleteWithExpressionCondition() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table_expression_condition");
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "age", 30),
            Map.of("id", 2L, "name", "Jane", "age", 25),
            Map.of("id", 3L, "name", "Bob", "age", 35));

    String initialInsertSql = TestUtil.createSelectSql(initialData, schema);
    swiftLakeEngine.insertInto(tableName).sql(initialInsertSql).execute();

    swiftLakeEngine
        .deleteFrom(tableName)
        .condition(
            Expressions.and(Expressions.greaterThan("age", 25), Expressions.lessThan("age", 35)))
        .execute();

    List<Map<String, Object>> actualData =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableId.toString());

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 2L, "name", "Jane", "age", 25),
            Map.of("id", 3L, "name", "Bob", "age", 35));

    assertThat(actualData).containsExactlyInAnyOrderElementsOf(expectedData);
  }

  @Test
  void testDeleteWithCustomBranch() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table_custom_branch");
    String tableName = tableId.toString();
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));

    String initialInsertSql = TestUtil.createSelectSql(initialData, schema);
    swiftLakeEngine.insertInto(tableName).sql(initialInsertSql).execute();
    String branchName = "test-branch";
    swiftLakeEngine.getTable(tableName).manageSnapshots().createBranch(branchName).commit();

    swiftLakeEngine.deleteFrom(tableName).conditionSql("id = 2").branch(branchName).execute();

    List<Map<String, Object>> mainBranchData =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    List<Map<String, Object>> testBranchData =
        TestUtil.getRecordsFromTable(
            swiftLakeEngine, TestUtil.getTableNameForBranch(tableId, branchName));

    assertThat(mainBranchData).hasSize(3);
    assertThat(testBranchData)
        .hasSize(2)
        .containsExactlyInAnyOrder(
            Map.of("id", 1L, "name", "John"), Map.of("id", 3L, "name", "Bob"));
  }

  @Test
  void testDeleteWithSnapshotMetadata() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table_snapshot_metadata");
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));

    String initialInsertSql = TestUtil.createSelectSql(initialData, schema);
    swiftLakeEngine.insertInto(tableName).sql(initialInsertSql).execute();

    Map<String, String> snapshotMetadata = new HashMap<>();
    snapshotMetadata.put("action", "delete");
    snapshotMetadata.put("user", "test-user");

    swiftLakeEngine
        .deleteFrom(tableName)
        .conditionSql("id = 2")
        .snapshotMetadata(snapshotMetadata)
        .execute();

    org.apache.iceberg.Snapshot latestSnapshot =
        swiftLakeEngine.getTable(tableName).currentSnapshot();
    assertThat(latestSnapshot.summary()).containsAllEntriesOf(snapshotMetadata);
  }

  /** Test for table batch transaction option */
  @Test
  void testDeleteWithTableBatchTransaction() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "department", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("department").build();

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_delete_batch_transaction");
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    // Insert initial data with multiple departments (multiple partitions)
    List<Map<String, Object>> initialData =
        IntStream.range(0, 100)
            .mapToObj(
                i ->
                    Map.of(
                        "id", (Object) (long) i,
                        "name", "User" + i,
                        "department", "Dept" + (i / 10)))
            .collect(Collectors.toList());

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    long beforeSnapshotId = swiftLakeEngine.getTable(tableName).currentSnapshot().snapshotId();

    // Delete records from multiple partitions using tableBatchTransaction
    TableBatchTransaction transaction =
        TableBatchTransaction.builderFor(swiftLakeEngine, tableName).build();
    swiftLakeEngine.deleteFrom(transaction).conditionSql("id BETWEEN 0 AND 15").execute();

    assertThatThrownBy(
            () ->
                swiftLakeEngine
                    .deleteFrom(transaction)
                    .conditionSql("id BETWEEN 16 AND 25")
                    .execute())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Found conflicting files that can contain records matching");

    ExecutorService executorService = Executors.newFixedThreadPool(10);
    List<Future<?>> futures = new ArrayList<>();

    // Create another transaction
    TableBatchTransaction newTransaction =
        TableBatchTransaction.builderFor(swiftLakeEngine, tableName).build();
    for (int i = 0; i < 10; i++) {
      int idStart = i * 10 + 0;
      int idEnd = i * 10 + 4;

      futures.add(
          executorService.submit(
              () -> {
                // Simulate some processing time
                try {
                  Thread.sleep(50);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
                swiftLakeEngine
                    .deleteFrom(newTransaction)
                    .conditionSql("id BETWEEN " + idStart + " AND " + idEnd)
                    .execute();
              }));
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

    // Commit all deletes in one commit
    newTransaction.commit();

    // Verify that we have only one new snapshot after the batch delete
    org.apache.iceberg.Table table = swiftLakeEngine.getTable(tableName);
    long afterSnapshotId = table.currentSnapshot().snapshotId();
    assertThat(afterSnapshotId).isNotEqualTo(beforeSnapshotId);

    // Verify the data was deleted correctly
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    // Should have exactly half the records (only odd IDs)
    assertThat(actualData).hasSize(50);

    // Verify that all remaining IDs are odd
    actualData.forEach(
        row -> {
          Long id = (Long) row.get("id");
          assertThat(id % 10).isGreaterThan(4);
        });

    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSkipDataSortingDelete(boolean isPartitioned) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.IntegerType.get()));

    PartitionSpec partitionSpec =
        isPartitioned
            ? PartitionSpec.builderFor(schema).bucket("id", 1).build()
            : PartitionSpec.unpartitioned();

    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").build();

    TableIdentifier tableId = TableIdentifier.of("test_db", "delete_skip_sorting_test");
    swiftLakeEngine
        .getCatalog()
        .buildTable(tableId, schema)
        .withPartitionSpec(partitionSpec)
        .withSortOrder(sortOrder)
        .create();

    String tableName = tableId.toString();

    // Insert initial data in descending order
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 5L, "name", "Echo", "value", 500),
            Map.of("id", 4L, "name", "Delta", "value", 400),
            Map.of("id", 3L, "name", "Charlie", "value", 300),
            Map.of("id", 2L, "name", "Bob", "value", 200),
            Map.of("id", 1L, "name", "Alice", "value", 100));

    // Insert with skipDataSorting to preserve the descending order
    swiftLakeEngine
        .insertInto(tableName)
        .sql(
            "SELECT * FROM ("
                + TestUtil.createSelectSql(initialData, schema)
                + ") ORDER BY id DESC")
        .skipDataSorting(true)
        .processSourceTables(false)
        .execute();

    // Verify the data was inserted in descending order
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 5L, "name", "Echo", "value", 500),
            Map.of("id", 4L, "name", "Delta", "value", 400),
            Map.of("id", 3L, "name", "Charlie", "value", 300),
            Map.of("id", 2L, "name", "Bob", "value", 200),
            Map.of("id", 1L, "name", "Alice", "value", 100));

    List<Map<String, Object>> actualData =
        TestUtil.getRecordsFromTableInFileOrder(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyElementsOf(expectedData);

    // Delete with skipDataSorting=true option
    swiftLakeEngine.deleteFrom(tableName).conditionSql("id = 3").skipDataSorting(true).execute();

    // The order should still be preserved (except for the deleted record)
    expectedData =
        Arrays.asList(
            Map.of("id", 5L, "name", "Echo", "value", 500),
            Map.of("id", 4L, "name", "Delta", "value", 400),
            Map.of("id", 2L, "name", "Bob", "value", 200),
            Map.of("id", 1L, "name", "Alice", "value", 100));

    actualData = TestUtil.getRecordsFromTableInFileOrder(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyElementsOf(expectedData);

    // Delete with skipDataSorting=false option
    // This should reorder the data according to the sort order
    swiftLakeEngine.deleteFrom(tableName).conditionSql("id = 4").skipDataSorting(false).execute();

    // The data should now be in ascending order as per the sort order
    expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "Alice", "value", 100),
            Map.of("id", 2L, "name", "Bob", "value", 200),
            Map.of("id", 5L, "name", "Echo", "value", 500));

    actualData = TestUtil.getRecordsFromTableInFileOrder(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyElementsOf(expectedData);

    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testDeleteWithDifferentIsolationLevels() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "year", Types.IntegerType.get()),
            Types.NestedField.required(3, "value", Types.StringType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("year").build();

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "delete_isolation_test_" + UUID.randomUUID().toString().replace('-', '_'));
    Table table = swiftLakeEngine.getCatalog().createTable(tableId, schema, spec);
    String tableName = tableId.toString();

    // Insert initial data: records with different years
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "year", 2023, "value", "initial_2023_value_1"),
            Map.of("id", 2L, "year", 2023, "value", "initial_2023_value_2"),
            Map.of("id", 3L, "year", 2024, "value", "initial_2024_value_1"),
            Map.of("id", 4L, "year", 2024, "value", "initial_2024_value_2"));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Refresh and capture the table state after initial insert
    table.refresh();
    Table tableStateAfterInitialInsert = swiftLakeEngine.getTable(tableName);
    Table tableStateAfterInitialInsertCopy = swiftLakeEngine.getTable(tableName);
    long snapshotIdAfterInitialInsert = tableStateAfterInitialInsert.currentSnapshot().snapshotId();

    // Insert another record
    List<Map<String, Object>> secondInsertData =
        Arrays.asList(Map.of("id", 5L, "year", 2024, "value", "second_2024_value"));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(secondInsertData, schema))
        .processSourceTables(false)
        .execute();

    // Refresh and capture table states after second insert
    table.refresh();
    Table tableStateAfterSecondInsert = swiftLakeEngine.getTable(tableName);

    // Verify second insert created a new snapshot
    assertThat(tableStateAfterSecondInsert.currentSnapshot().snapshotId())
        .isNotEqualTo(snapshotIdAfterInitialInsert);

    // Delete ID=1 record in the 2023 partition
    swiftLakeEngine.deleteFrom(tableName).conditionSql("id = 1").execute();

    // Delete should succeed since it's working on the most recent table state
    List<Map<String, Object>> dataAfterFirstDelete =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(dataAfterFirstDelete).hasSize(4).noneMatch(row -> row.get("id").equals(1L));

    // Try to delete from the 2024 partition using the original table state
    // This should fail due to conflicting files, as we're using the default isolation level
    assertThatThrownBy(
            () -> {
              swiftLakeEngine
                  .deleteFrom(tableStateAfterInitialInsert)
                  .conditionSql("year = 2024")
                  .execute();
            })
        .hasMessageContaining("Found conflicting files");

    // Try again with SNAPSHOT isolation level
    // This should succeed since SNAPSHOT isolation ignores new files
    swiftLakeEngine
        .deleteFrom(tableStateAfterInitialInsertCopy)
        .conditionSql("year = 2024")
        .isolationLevel(org.apache.iceberg.IsolationLevel.SNAPSHOT)
        .execute();

    // Refresh and verify the delete succeeded
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId())
        .isNotEqualTo(tableStateAfterSecondInsert.currentSnapshot().snapshotId());

    // Verify the data after SNAPSHOT isolation delete
    List<Map<String, Object>> dataAfterSnapshotDelete =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(dataAfterSnapshotDelete)
        .hasSize(2)
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(2L);
              assertThat(row.get("year")).isEqualTo(2023);
              assertThat(row.get("value")).isEqualTo("initial_2023_value_2");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(5L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("value")).isEqualTo("second_2024_value");
            });

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }
}
