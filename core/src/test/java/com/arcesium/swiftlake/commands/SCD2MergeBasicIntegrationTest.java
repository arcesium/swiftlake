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
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expressions;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SCD2MergeBasicIntegrationTest {
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
  void testSCD2Merge(
      String testName,
      Schema schema,
      PartitionSpec partitionSpec,
      List<Map<String, Object>> initialData,
      List<SCD2MergeInput> mergeInputList,
      List<Map<String, Object>> expectedData,
      List<Consumer<Table>> evolveSchema,
      List<Pair<Class<? extends Throwable>, String>> errors) {
    // Create table
    TableIdentifier tableId = TableIdentifier.of("test_db", "scd2_test_table_" + testName);
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    // Insert initial data
    String currentFlagColumn = null;
    if (!mergeInputList.isEmpty()) {
      currentFlagColumn = mergeInputList.get(0).currentFlagColumn;
    }
    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00", currentFlagColumn);
    LocalDateTime initialEffectiveTimestamp = LocalDateTime.parse("2025-01-01T00:00:00");

    for (int i = 0; i < mergeInputList.size(); i++) {
      if (evolveSchema != null && evolveSchema.size() > i && evolveSchema.get(i) != null) {
        evolveSchema.get(i).accept(swiftLakeEngine.getTable(tableName));
        schema = swiftLakeEngine.getTable(tableName).schema();
      }
      SCD2MergeInput mergeInput = mergeInputList.get(i);
      String sourceSql =
          mergeInput.isSnapshot
              ? TestUtil.createSnapshotSql(mergeInput.inputData, schema)
              : TestUtil.createChangeSql(mergeInput.inputData, schema);
      LocalDateTime effectiveTimestamp = initialEffectiveTimestamp.plusDays(i + 1);
      Runnable mergeFunc =
          () ->
              SCD2MergeIntegrationTestUtil.performSCD2Merge(
                  swiftLakeEngine,
                  mergeInput.isSnapshot,
                  tableName,
                  mergeInput.tableFilter,
                  mergeInput.keyColumns,
                  sourceSql,
                  effectiveTimestamp,
                  mergeInput.changeTrackingColumns,
                  mergeInput.changeTrackingMetadata,
                  mergeInput.currentFlagColumn);
      if (errors != null && errors.size() > i) {
        var throwableAssert = assertThatThrownBy(() -> mergeFunc.run());
        var error = errors.get(i);
        if (error.getLeft() != null) {
          throwableAssert.isInstanceOf(error.getLeft());
        }
        if (error.getRight() != null) {
          throwableAssert.hasMessageContaining(error.getRight());
        }
      } else {
        mergeFunc.run();
      }
    }

    // Verify results
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    expectedData = SCD2MergeIntegrationTestUtil.addNullEffectiveEnd(expectedData);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  private static Stream<Arguments> provideTestCases() {
    return Stream.of(
        simpleSchemaTestCase(true),
        simpleSchemaTestCase(false),
        complexTypesTestCase(true),
        complexTypesTestCase(false),
        noChangesTestCase(true),
        noChangesTestCase(false),
        allInsertsTestCase(true),
        allInsertsTestCase(false),
        allDeletesTestCase(true),
        allDeletesTestCase(false),
        emptySourceTestCase(),
        multipleOperationsTestCase(true),
        multipleOperationsTestCase(false),
        nullValuesTestCase(true),
        nullValuesTestCase(false),
        changeTrackingColumnsTestCase(true),
        changeTrackingColumnsTestCase(false),
        currentFlagColumnTestCase(true),
        currentFlagColumnTestCase(false),
        longHistoryChainTestCase(true),
        longHistoryChainTestCase(false),
        extremeValuesTestCase(true),
        extremeValuesTestCase(false),
        unicodeAndSpecialCharactersTestCase(true),
        unicodeAndSpecialCharactersTestCase(false),
        timeZoneHandlingTestCase(true),
        timeZoneHandlingTestCase(false),
        schemaEvolutionTestCase(true),
        schemaEvolutionTestCase(false),
        multiColumnKeyTestCase(true),
        multiColumnKeyTestCase(false),
        errorHandlingTestCase(true),
        errorHandlingTestCase(false));
  }

  private static Arguments simpleSchemaTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<Map<String, Object>> inputData = null;

    if (isSnapshotMerge) {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John Doe"),
              Map.of("id", 2L, "name", "Jane"),
              Map.of("id", 3L, "name", "Bob"));
    } else {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John Doe", "operation_type", "U"),
              Map.of("id", 3L, "name", "Bob", "operation_type", "I"));
    }

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                1L,
                "name",
                "John Doe",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00")),
            Map.of(
                "id",
                3L,
                "name",
                "Bob",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD2MergeInput mergeInput =
        new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge);
    return Arguments.of(
        "simple_schema" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments complexTypesTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "string_col", Types.StringType.get()),
            Types.NestedField.required(3, "int_col", Types.IntegerType.get()),
            Types.NestedField.required(4, "double_col", Types.DoubleType.get()),
            Types.NestedField.required(5, "decimal_col", Types.DecimalType.of(10, 2)),
            Types.NestedField.required(6, "bool_col", Types.BooleanType.get()),
            Types.NestedField.required(7, "date_col", Types.DateType.get()),
            Types.NestedField.required(8, "timestamp_col", Types.TimestampType.withoutZone()),
            Types.NestedField.required(
                9,
                "struct_col",
                Types.StructType.of(
                    Types.NestedField.required(10, "nested_int", Types.IntegerType.get()),
                    Types.NestedField.required(11, "nested_string", Types.StringType.get()))),
            Types.NestedField.required(
                12, "list_col", Types.ListType.ofRequired(13, Types.StringType.get())),
            Types.NestedField.required(
                14,
                "map_col",
                Types.MapType.ofRequired(15, 16, Types.StringType.get(), Types.IntegerType.get())),
            Types.NestedField.required(17, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(18, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("id").build();

    List<Map<String, Object>> initialData =
        Arrays.asList(createComplexRow(1L, "2025-01-01"), createComplexRow(2L, "2025-01-01"));

    List<Map<String, Object>> inputData = null;
    if (isSnapshotMerge) {
      inputData =
          Arrays.asList(
              updateComplexRow(1L, "2025-01-01", null),
              createComplexRow(2L, "2025-01-01"),
              createComplexRow(3L, "2025-01-01"));
    } else {
      inputData =
          Arrays.asList(
              updateComplexRow(1L, "2025-01-01", "U"), createComplexRow(3L, "2025-01-01", "I"));
    }
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            createExpectedComplexRow(
                1L, "2025-01-01", "2025-01-01T00:00:00", "2025-01-02T00:00:00", false),
            createExpectedComplexRow(1L, "2025-01-01", "2025-01-02T00:00:00", null, true),
            createExpectedComplexRow(2L, "2025-01-01", "2025-01-01T00:00:00", null, false),
            createExpectedComplexRow(3L, "2025-01-01", "2025-01-02T00:00:00", null, false));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD2MergeInput mergeInput =
        new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge);
    return Arguments.of(
        "complex_types" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments noChangesTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<Map<String, Object>> inputData = null;

    if (isSnapshotMerge) {
      inputData = initialData;
    } else {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John", "operation_type", "U"),
              Map.of("id", 2L, "name", "Jane", "operation_type", "U"));
    }

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00")));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD2MergeInput mergeInput =
        new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge);
    return Arguments.of(
        "no_changes" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments allInsertsTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData = new ArrayList<>();

    List<Map<String, Object>> inputData = null;
    if (isSnapshotMerge) {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John"),
              Map.of("id", 2L, "name", "Jane"),
              Map.of("id", 3L, "name", "Bob"));
    } else {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John", "operation_type", "I"),
              Map.of("id", 2L, "name", "Jane", "operation_type", "I"),
              Map.of("id", 3L, "name", "Bob", "operation_type", "I"));
    }
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                3L,
                "name",
                "Bob",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD2MergeInput mergeInput =
        new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge);
    return Arguments.of(
        "all_inserts" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments allDeletesTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));

    List<Map<String, Object>> inputData =
        isSnapshotMerge
            ? Arrays.asList()
            : Arrays.asList(
                Map.of("id", 1L, "name", "John", "operation_type", "D"),
                Map.of("id", 2L, "name", "Jane", "operation_type", "D"),
                Map.of("id", 3L, "name", "Bob", "operation_type", "D"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                3L,
                "name",
                "Bob",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD2MergeInput mergeInput =
        new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge);
    return Arguments.of(
        "all_deletes" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments emptySourceTestCase() {
    boolean isSnapshotMerge = false;
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<Map<String, Object>> inputData = new ArrayList<>();

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00")));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD2MergeInput mergeInput =
        new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge);
    return Arguments.of(
        "empty_source" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments multipleOperationsTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<List<Map<String, Object>>> inputDataList = null;
    if (isSnapshotMerge) {
      inputDataList =
          Arrays.asList(
              Arrays.asList(
                  Map.of("id", 1L, "name", "John Doe"),
                  Map.of("id", 2L, "name", "Jane"),
                  Map.of("id", 3L, "name", "Bob")),
              Arrays.asList(
                  Map.of("id", 2L, "name", "Jane Doe"),
                  Map.of("id", 3L, "name", "Bob"),
                  Map.of("id", 5L, "name", "Jane Doe")),
              Arrays.asList(
                  Map.of("id", 5L, "name", "Jane Doe"),
                  Map.of("id", 4L, "name", "Alice"),
                  Map.of("id", 3L, "name", "Robert")));
    } else {
      inputDataList =
          Arrays.asList(
              Arrays.asList(
                  Map.of("id", 1L, "name", "John Doe", "operation_type", "U"),
                  Map.of("id", 3L, "name", "Bob", "operation_type", "I")),
              Arrays.asList(
                  Map.of("id", 2L, "name", "Jane Doe", "operation_type", "U"),
                  Map.of("id", 1L, "name", "John", "operation_type", "D"),
                  Map.of("id", 5L, "name", "Jane Doe", "operation_type", "I")),
              Arrays.asList(
                  Map.of("id", 2L, "name", "Jane Doe", "operation_type", "D"),
                  Map.of("id", 4L, "name", "Alice", "operation_type", "I"),
                  Map.of("id", 3L, "name", "Robert", "operation_type", "U")));
    }
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                1L,
                "name",
                "John Doe",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-03T00:00:00")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-03T00:00:00")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane Doe",
                "effective_start",
                LocalDateTime.parse("2025-01-03T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-04T00:00:00")),
            Map.of(
                "id",
                3L,
                "name",
                "Bob",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-04T00:00:00")),
            Map.of(
                "id",
                3L,
                "name",
                "Robert",
                "effective_start",
                LocalDateTime.parse("2025-01-04T00:00:00")),
            Map.of(
                "id",
                4L,
                "name",
                "Alice",
                "effective_start",
                LocalDateTime.parse("2025-01-04T00:00:00")),
            Map.of(
                "id",
                5L,
                "name",
                "Jane Doe",
                "effective_start",
                LocalDateTime.parse("2025-01-03T00:00:00")));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    List<SCD2MergeInput> mergeInputs =
        inputDataList.stream()
            .map(
                inputData ->
                    new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge))
            .collect(Collectors.toList());

    return Arguments.of(
        "multiple_operations" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        mergeInputs,
        expectedData,
        null,
        null);
  }

  private static Arguments longHistoryChainTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData = Arrays.asList(Map.of("id", 1L, "value", "Initial"));

    List<List<Map<String, Object>>> inputDataList = new ArrayList<>();
    List<Map<String, Object>> expectedData = new ArrayList<>();
    LocalDateTime initialEffectiveStart = LocalDateTime.parse("2025-01-01T00:00:00");
    expectedData.add(
        new HashMap<>() {
          {
            put("id", 1L);
            put("value_2", "Initial");
            put("value_3", null);
            put("value_5", null);
            put("effective_start", initialEffectiveStart);
            put("effective_end", initialEffectiveStart.plusDays(1));
          }
        });

    LocalDate epochDay = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC).toLocalDate();
    List<Consumer<Table>> evolveSchemaList = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      if (i == 5) {
        evolveSchemaList.add(
            (table) -> {
              table.updateSpec().addField("id").commit();
            });
      } else if (i == 10) {
        evolveSchemaList.add(
            (table) -> {
              table.updateSchema().updateColumn("id", Types.LongType.get()).commit();
              table.updateSpec().removeField("id").addField(Expressions.bucket("id", 5)).commit();
            });
      } else if (i == 20) {
        evolveSchemaList.add(
            (table) -> {
              table.updateSchema().renameColumn("value", "value_2").commit();
              table.updateSpec().addField(Expressions.bucket("value_2", 5)).commit();
            });
      } else if (i == 30) {
        evolveSchemaList.add(
            (table) -> {
              // new order value_2,id,effective_start,effective_end
              table.updateSchema().moveFirst("value_2").commit();
            });
      } else if (i == 35) {
        evolveSchemaList.add(
            (table) -> {
              // new order value_2,effective_end,id,effective_start
              table.updateSchema().moveAfter("effective_end", "value_2").commit();
            });
      } else if (i == 40) {
        evolveSchemaList.add(
            (table) -> {
              // new order value_2,id,effective_end,effective_start
              table.updateSchema().moveBefore("id", "effective_end").commit();
            });
      } else if (i == 50) {
        evolveSchemaList.add(
            (table) -> {
              table
                  .updateSchema()
                  .addColumn("value_3", Types.StringType.get())
                  .addColumn("value_4", Types.DecimalType.of(28, 10))
                  .addColumn("value_5", Types.DateType.get())
                  .commit();
            });
      } else if (i == 55) {
        evolveSchemaList.add(
            (table) -> {
              table.updateSchema().updateColumn("value_4", Types.DecimalType.of(38, 10)).commit();
              table
                  .updateSpec()
                  .addField(Expressions.bucket("value_3", 3))
                  .addField(Expressions.bucket("value_4", 4))
                  .addField(Expressions.month("value_5"))
                  .commit();
            });
      } else if (i == 60) {
        evolveSchemaList.add(
            (table) -> {
              table.updateSpec().removeField(Expressions.bucket("value_3", 3)).commit();
              table.updateSchema().deleteColumn("value_3").commit();
              table.updateSchema().addColumn("value_3", Types.StringType.get()).commit();
              table.updateSpec().addField(Expressions.truncate("value_3", 3)).commit();
            });
      } else if (i == 70) {
        evolveSchemaList.add(
            (table) -> {
              table.updateSpec().removeField(Expressions.month("value_5")).commit();
              table.updateSchema().deleteColumn("value_5").commit();
              table.updateSchema().renameColumn("value_4", "value_5").commit();
            });
      } else {
        evolveSchemaList.add(null);
      }

      LocalDateTime expectedEffectiveStart = initialEffectiveStart.plusDays(i + 1);
      LocalDateTime expectedEffectiveEnd = initialEffectiveStart.plusDays(i + 2);
      String expectedStringValue = "Update" + i;
      BigDecimal decimal28PrecisionValue =
          new BigDecimal(
                  new StringBuilder()
                      .append((i + "").repeat(18).substring(0, 18))
                      .append(".")
                      .append((i + "").repeat(10).substring(0, 10))
                      .toString())
              .setScale(10, RoundingMode.HALF_UP);
      BigDecimal decimal38PrecisionValue =
          new BigDecimal(
                  new StringBuilder()
                      .append((i + "").repeat(28).substring(0, 28))
                      .append(".")
                      .append((i + "").repeat(10).substring(0, 10))
                      .toString())
              .setScale(10, RoundingMode.HALF_UP);

      if (i < 10) {
        inputDataList.add(
            Arrays.asList(Map.of("id", 1, "value", "Update" + i, "operation_type", "U")));
        expectedData.add(
            new HashMap<>() {
              {
                put("id", 1L);
                put("value_2", expectedStringValue);
                put("value_3", null);
                put("value_5", null);
                put("effective_start", expectedEffectiveStart);
                put("effective_end", expectedEffectiveEnd);
              }
            });
      } else if (i < 20) {
        inputDataList.add(
            Arrays.asList(Map.of("id", 1L, "value", "Update" + i, "operation_type", "U")));
        expectedData.add(
            new HashMap<>() {
              {
                put("id", 1L);
                put("value_2", expectedStringValue);
                put("value_3", null);
                put("value_5", null);
                put("effective_start", expectedEffectiveStart);
                put("effective_end", expectedEffectiveEnd);
              }
            });
      } else if (i < 50) {
        inputDataList.add(
            Arrays.asList(Map.of("id", 1L, "value_2", "Update" + i, "operation_type", "U")));
        expectedData.add(
            new HashMap<>() {
              {
                put("id", 1L);
                put("value_2", expectedStringValue);
                put("value_3", null);
                put("value_5", null);
                put("effective_start", expectedEffectiveStart);
                put("effective_end", expectedEffectiveEnd);
              }
            });
      } else if (i < 55) {
        inputDataList.add(
            Arrays.asList(
                Map.of(
                    "id",
                    1L,
                    "value_2",
                    "Update" + i,
                    "value_3",
                    "Update" + i,
                    "value_4",
                    decimal28PrecisionValue,
                    "value_5",
                    epochDay.plusDays(i),
                    "operation_type",
                    "U")));
        expectedData.add(
            new HashMap<>() {
              {
                put("id", 1L);
                put("value_2", expectedStringValue);
                put("value_3", null);
                put("value_5", decimal28PrecisionValue);
                put("effective_start", expectedEffectiveStart);
                put("effective_end", expectedEffectiveEnd);
              }
            });
      } else if (i < 60) {
        inputDataList.add(
            Arrays.asList(
                Map.of(
                    "id",
                    1L,
                    "value_2",
                    "Update" + i,
                    "value_3",
                    "Update" + i,
                    "value_4",
                    decimal38PrecisionValue,
                    "value_5",
                    epochDay.plusDays(i),
                    "operation_type",
                    "U")));
        expectedData.add(
            new HashMap<>() {
              {
                put("id", 1L);
                put("value_2", expectedStringValue);
                put("value_3", null);
                put("value_5", decimal38PrecisionValue);
                put("effective_start", expectedEffectiveStart);
                put("effective_end", expectedEffectiveEnd);
              }
            });
      } else if (i < 70) {
        inputDataList.add(
            Arrays.asList(
                Map.of(
                    "id",
                    1L,
                    "value_2",
                    "Update" + i,
                    "value_3",
                    "Update" + i,
                    "value_4",
                    decimal38PrecisionValue,
                    "value_5",
                    epochDay.plusDays(i),
                    "operation_type",
                    "U")));
        expectedData.add(
            new HashMap<>() {
              {
                put("id", 1L);
                put("value_2", expectedStringValue);
                put("value_3", expectedStringValue);
                put("value_5", decimal38PrecisionValue);
                put("effective_start", expectedEffectiveStart);
                put("effective_end", expectedEffectiveEnd);
              }
            });
      } else {
        inputDataList.add(
            Arrays.asList(
                Map.of(
                    "id",
                    1L,
                    "value_2",
                    "Update" + i,
                    "value_3",
                    "Update" + i,
                    "value_5",
                    decimal38PrecisionValue,
                    "operation_type",
                    "U")));

        var index = i;
        expectedData.add(
            new HashMap<>() {
              {
                put("id", 1L);
                put("value_2", expectedStringValue);
                put("value_3", expectedStringValue);
                put("value_5", decimal38PrecisionValue);
                put("effective_start", expectedEffectiveStart);
                put("effective_end", (index == 99) ? null : expectedEffectiveEnd);
              }
            });
      }
    }

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    List<SCD2MergeInput> mergeInputs =
        inputDataList.stream()
            .map(
                inputData ->
                    new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge))
            .collect(Collectors.toList());

    return Arguments.of(
        "long_history_chain" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        mergeInputs,
        expectedData,
        evolveSchemaList,
        null);
  }

  private static Map<String, Object> createComplexRow(Long id, String date) {
    return createComplexRow(id, date, null);
  }

  private static Map<String, Object> createComplexRow(Long id, String date, String operationType) {
    Map<String, Object> row = new HashMap<>();
    row.put("id", id);
    row.put("string_col", "string_" + id);
    row.put("int_col", (id % 2 == 0 ? 1 : -1) * id.intValue());
    row.put("double_col", (id % 2 == 0 ? 1 : -1) * id.doubleValue());
    row.put(
        "decimal_col", new BigDecimal(id + ".99").multiply(new BigDecimal(id % 2 == 0 ? 1 : -1)));
    row.put("bool_col", id % 2 == 0);
    row.put("date_col", LocalDate.parse(date));
    row.put("timestamp_col", LocalDateTime.parse(date + "T12:00:00"));
    row.put("struct_col", Map.of("nested_int", id.intValue(), "nested_string", "nested_" + id));
    row.put("list_col", Arrays.asList("list_item_1_" + id, "list_item_2_" + id));
    row.put("map_col", Map.of("key1_" + id, 1, "key2_" + id, 2));
    if (operationType != null) {
      row.put("operation_type", operationType);
    }
    return row;
  }

  private static Map<String, Object> updateComplexRow(Long id, String date, String operationType) {
    Map<String, Object> row = createComplexRow(id, date, operationType);
    row.put("string_col", "updated_string_" + id);
    row.put("int_col", id.intValue() + 100);
    return row;
  }

  private static Map<String, Object> createExpectedComplexRow(
      Long id, String date, String effectiveStart, String effectiveEnd, boolean isUpdated) {
    Map<String, Object> row = createComplexRow(id, date);
    if (isUpdated) {
      row.put("string_col", "updated_string_" + id);
      row.put("int_col", id.intValue() + 100);
    }
    row.put("effective_start", LocalDateTime.parse(effectiveStart));
    row.put("effective_end", effectiveEnd != null ? LocalDateTime.parse(effectiveEnd) : null);
    return row;
  }

  private static Arguments nullValuesTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "nullable_string", Types.StringType.get()),
            Types.NestedField.optional(3, "nullable_int", Types.IntegerType.get()),
            Types.NestedField.required(4, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(5, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "nullable_string", "value", "nullable_int", 10));

    List<Map<String, Object>> inputData =
        Arrays.asList(
            new HashMap<>() {
              {
                put("id", 1L);
                put("nullable_string", null);
                put("nullable_int", null);
                if (!isSnapshotMerge) {
                  put("operation_type", "U");
                }
              }
            },
            new HashMap<>() {
              {
                put("id", 2L);
                put("nullable_string", "new");
                put("nullable_int", null);
                if (!isSnapshotMerge) {
                  put("operation_type", "I");
                }
              }
            });

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            new HashMap<>() {
              {
                put("id", 1L);
                put("nullable_string", "value");
                put("nullable_int", 10);
                put("effective_start", LocalDateTime.parse("2025-01-01T00:00:00"));
                put("effective_end", LocalDateTime.parse("2025-01-02T00:00:00"));
              }
            },
            new HashMap<>() {
              {
                put("id", 1L);
                put("nullable_string", null);
                put("nullable_int", null);
                put("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
              }
            },
            new HashMap<>() {
              {
                put("id", 2L);
                put("nullable_string", "new");
                put("nullable_int", null);
                put("effective_start", LocalDateTime.parse("2025-01-02T00:00:00"));
              }
            });

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD2MergeInput mergeInput =
        new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge);
    return Arguments.of(
        "null_values" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments extremeValuesTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "long_string", Types.StringType.get()),
            Types.NestedField.required(3, "big_number", Types.DecimalType.of(38, 10)),
            Types.NestedField.required(4, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(5, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    String longString = "a".repeat(100000);
    BigDecimal bigNumber = new BigDecimal("12345678901234567890.1234567890");

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "long_string", "initial", "big_number", BigDecimal.ONE));

    List<Map<String, Object>> inputData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "long_string",
                longString,
                "big_number",
                bigNumber,
                "operation_type",
                "U"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "long_string",
                "initial",
                "big_number",
                BigDecimal.ONE.setScale(10),
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                1L,
                "long_string",
                longString,
                "big_number",
                bigNumber,
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD2MergeInput mergeInput =
        new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge);
    return Arguments.of(
        "extreme_values" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments unicodeAndSpecialCharactersTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "special_string", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    String specialString = "!@#$%^&*()_+{ ¡™£¢∞§¶•ªº}[]|\\:;こんにちは\"'<你好，世界>,.?/~`";

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "special_string", "normal"));

    List<Map<String, Object>> inputData =
        Arrays.asList(Map.of("id", 1L, "special_string", specialString, "operation_type", "U"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "special_string",
                "normal",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                1L,
                "special_string",
                specialString,
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD2MergeInput mergeInput =
        new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge);
    return Arguments.of(
        "special_characters" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments timeZoneHandlingTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "timestamp_with_tz", Types.TimestampType.withZone()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    OffsetDateTime utcTime = OffsetDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    OffsetDateTime changedTime =
        utcTime.withOffsetSameInstant(ZoneOffset.ofHours(-5)).plusSeconds(2);

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "timestamp_with_tz", utcTime));

    List<Map<String, Object>> inputData =
        Arrays.asList(Map.of("id", 1L, "timestamp_with_tz", changedTime, "operation_type", "U"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "timestamp_with_tz",
                utcTime,
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                1L,
                "timestamp_with_tz",
                changedTime.withOffsetSameInstant(ZoneOffset.UTC),
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD2MergeInput mergeInput =
        new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge);
    return Arguments.of(
        "time_zone_handling" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments schemaEvolutionTestCase(boolean isSnapshotMerge) {
    Schema initialSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));

    Consumer<Table> evolveSchema =
        (table) -> {
          table.updateSchema().addColumn("email", Types.StringType.get()).commit();
        };

    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData = Arrays.asList(Map.of("id", 1L, "name", "John"));

    List<Map<String, Object>> inputData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John Doe",
                "email",
                "john.doe@example.com",
                "operation_type",
                "U"),
            Map.of("id", 2L, "name", "Jane", "email", "jane@example.com", "operation_type", "I"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            new HashMap<>() {
              {
                put("id", 1L);
                put("name", "John");
                put("email", null);
                put("effective_start", LocalDateTime.parse("2025-01-01T00:00:00"));
                put("effective_end", LocalDateTime.parse("2025-01-02T00:00:00"));
              }
            },
            Map.of(
                "id",
                1L,
                "name",
                "John Doe",
                "email",
                "john.doe@example.com",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "email",
                "jane@example.com",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD2MergeInput mergeInput =
        new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge);
    return Arguments.of(
        "schema_evolution",
        initialSchema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        Arrays.asList(evolveSchema),
        null);
  }

  private static Arguments multiColumnKeyTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id1", Types.LongType.get()),
            Types.NestedField.required(2, "id2", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.StringType.get()),
            Types.NestedField.required(4, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(5, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id1", 1L, "id2", "A", "value", "Initial1"),
            Map.of("id1", 1L, "id2", "B", "value", "Initial2"),
            Map.of("id1", 2L, "id2", "A", "value", "Initial3"));

    List<Map<String, Object>> inputData = null;
    if (isSnapshotMerge) {
      inputData =
          Arrays.asList(
              Map.of("id1", 1L, "id2", "A", "value", "Updated1"),
              Map.of("id1", 1L, "id2", "B", "value", "Initial2"),
              Map.of("id1", 2L, "id2", "B", "value", "New"));
    } else {
      inputData =
          Arrays.asList(
              Map.of("id1", 1L, "id2", "A", "value", "Updated1", "operation_type", "U"),
              Map.of("id1", 2L, "id2", "B", "value", "New", "operation_type", "I"),
              Map.of("id1", 2L, "id2", "A", "value", "ToDelete", "operation_type", "D"));
    }
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id1",
                1L,
                "id2",
                "A",
                "value",
                "Initial1",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id1",
                1L,
                "id2",
                "B",
                "value",
                "Initial2",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00")),
            Map.of(
                "id1",
                2L,
                "id2",
                "A",
                "value",
                "Initial3",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id1",
                1L,
                "id2",
                "A",
                "value",
                "Updated1",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id1",
                2L,
                "id2",
                "B",
                "value",
                "New",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")));

    List<String> keyColumns = Arrays.asList("id1", "id2");
    String tableFilter = "id1 IS NOT NULL";
    SCD2MergeInput mergeInput =
        new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge);
    return Arguments.of(
        "multi_column_key" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments errorHandlingTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()),
            Types.NestedField.required(4, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(5, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    // Initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "value", 100.0),
            Map.of("id", 2L, "name", "Jane", "value", 200.0));

    List<SCD2MergeInput> mergeInputs = new ArrayList<>();
    List<Pair<Class<? extends Throwable>, String>> errors = new ArrayList<>();
    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";

    // Missing required columns
    List<Map<String, Object>> inputData = null;
    if (isSnapshotMerge) {
      inputData = Arrays.asList(Map.of("id", 1L, "value", 300.0), Map.of("id", 2L));
    } else {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "value", 300.0, "operation_type", "U"),
              Map.of("id", 2L, "operation_type", "I"));
    }
    mergeInputs.add(new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge));
    errors.add(
        Pair.of(
            ValidationException.class, "Required fields cannot contain null values - name,value"));

    // Null value for required columns
    inputData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe", "value", 300.0, "operation_type", "U"),
            new HashMap<String, Object>() {
              {
                put("id", 3L);
                put("name", "Bob");
                put("value", null);
                put("operation_type", "I");
              }
            });
    mergeInputs.add(new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge));
    errors.add(
        Pair.of(ValidationException.class, "Required fields cannot contain null values - value"));

    // Duplicate records
    if (isSnapshotMerge) {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John Doe", "value", 300.0),
              Map.of("id", 1L, "name", "Bob", "value", 400.0));
    } else {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John Doe", "value", 300.0, "operation_type", "U"),
              Map.of("id", 1L, "name", "Bob", "value", 400.0, "operation_type", "D"));
    }
    mergeInputs.add(new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge));
    errors.add(
        Pair.of(
            ValidationException.class,
            "Merge operation matched a single row from target table data with multiple rows of the source data"));
    if (isSnapshotMerge) {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John Doe", "value", 300.0),
              Map.of("id", 2L, "name", "Jane", "value", 200.0),
              Map.of("id", 3L, "name", "Bob", "value", 400.0));
    } else {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John Doe", "value", 300.0, "operation_type", "U"),
              Map.of("id", 3L, "name", "Bob", "value", 400.0, "operation_type", "I"));
    }
    mergeInputs.add(new SCD2MergeInput(inputData, tableFilter, keyColumns, isSnapshotMerge));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "value",
                100.0,
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-05T00:00:00")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "value",
                200.0,
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00")),
            Map.of(
                "id",
                1L,
                "name",
                "John Doe",
                "value",
                300.0,
                "effective_start",
                LocalDateTime.parse("2025-01-05T00:00:00")),
            Map.of(
                "id",
                3L,
                "name",
                "Bob",
                "value",
                400.0,
                "effective_start",
                LocalDateTime.parse("2025-01-05T00:00:00")));

    return Arguments.of(
        "error_handling" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        mergeInputs,
        expectedData,
        null,
        errors);
  }

  private static Arguments changeTrackingColumnsTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "value", Types.DecimalType.of(38, 10)),
            Types.NestedField.required(4, "updated_at", Types.TimestampType.withoutZone()),
            Types.NestedField.required(5, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(6, "effective_end", Types.TimestampType.withoutZone()));

    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    // Initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "value",
                new BigDecimal(100.0),
                "updated_at",
                LocalDateTime.parse("2025-01-01T10:00:00")),
            new HashMap<String, Object>() {
              {
                put("id", 2L);
                put("name", null);
                put("value", new BigDecimal(200.0));
                put("updated_at", LocalDateTime.parse("2025-01-01T11:00:00"));
              }
            });

    List<String> keyColumns = Arrays.asList("id");
    List<String> changeTrackingColumns = Arrays.asList("name", "value");
    Map<String, ChangeTrackingMetadata<?>> changeTrackingMetadata =
        Map.of(
            "value",
            new ChangeTrackingMetadata<>(0.05, null),
            "name",
            new ChangeTrackingMetadata<>(null, ""));
    String tableFilter = "id IS NOT NULL";
    List<SCD2MergeInput> mergeInputs = new ArrayList<>();

    List<Map<String, Object>> inputData = null;

    if (isSnapshotMerge) {
      inputData =
          Arrays.asList(
              Map.of(
                  "id",
                  1L,
                  "name",
                  "John",
                  "value",
                  new BigDecimal(100.05),
                  "updated_at",
                  LocalDateTime.parse("2025-01-02T08:00:00")),
              Map.of(
                  "id",
                  2L,
                  "name",
                  "",
                  "value",
                  new BigDecimal(200.0),
                  "updated_at",
                  LocalDateTime.parse("2025-01-02T09:00:00")),
              Map.of(
                  "id",
                  3L,
                  "name",
                  "Bob",
                  "value",
                  new BigDecimal(300.0),
                  "updated_at",
                  LocalDateTime.parse("2025-01-02T10:00:00")),
              Map.of(
                  "id",
                  4L,
                  "name",
                  "Alice",
                  "value",
                  new BigDecimal(400.0),
                  "updated_at",
                  LocalDateTime.parse("2025-01-02T11:00:00")));
    } else {
      inputData =
          Arrays.asList(
              Map.of(
                  "id",
                  1L,
                  "name",
                  "John",
                  "value",
                  new BigDecimal(100.05),
                  "updated_at",
                  LocalDateTime.parse("2025-01-02T08:00:00"),
                  "operation_type",
                  "U"),
              Map.of(
                  "id",
                  2L,
                  "name",
                  "",
                  "value",
                  new BigDecimal(200.0),
                  "updated_at",
                  LocalDateTime.parse("2025-01-02T09:00:00"),
                  "operation_type",
                  "U"),
              Map.of(
                  "id",
                  3L,
                  "name",
                  "Bob",
                  "value",
                  new BigDecimal(300.0),
                  "updated_at",
                  LocalDateTime.parse("2025-01-02T10:00:00"),
                  "operation_type",
                  "I"),
              Map.of(
                  "id",
                  4L,
                  "name",
                  "Alice",
                  "value",
                  new BigDecimal(400.0),
                  "updated_at",
                  LocalDateTime.parse("2025-01-02T11:00:00"),
                  "operation_type",
                  "I"));
    }

    mergeInputs.add(
        new SCD2MergeInput(
            inputData,
            tableFilter,
            keyColumns,
            null,
            changeTrackingColumns,
            changeTrackingMetadata,
            isSnapshotMerge));

    if (isSnapshotMerge) {
      inputData =
          Arrays.asList(
              Map.of(
                  "id",
                  1L,
                  "name",
                  "John",
                  "value",
                  new BigDecimal(100.06),
                  "updated_at",
                  LocalDateTime.parse("2025-01-03T08:00:00")),
              Map.of(
                  "id",
                  2L,
                  "name",
                  "Jane",
                  "value",
                  new BigDecimal(200.0),
                  "updated_at",
                  LocalDateTime.parse("2025-01-03T09:00:00")),
              Map.of(
                  "id",
                  3L,
                  "name",
                  "Bob",
                  "value",
                  new BigDecimal(299.95),
                  "updated_at",
                  LocalDateTime.parse("2025-01-03T10:00:00")),
              Map.of(
                  "id",
                  4L,
                  "name",
                  "Alice",
                  "value",
                  new BigDecimal(399.94),
                  "updated_at",
                  LocalDateTime.parse("2025-01-03T11:00:00")));
    } else {
      inputData =
          Arrays.asList(
              Map.of(
                  "id",
                  1L,
                  "name",
                  "John",
                  "value",
                  new BigDecimal(100.06),
                  "updated_at",
                  LocalDateTime.parse("2025-01-03T08:00:00"),
                  "operation_type",
                  "U"),
              Map.of(
                  "id",
                  2L,
                  "name",
                  "Jane",
                  "value",
                  new BigDecimal(200.0),
                  "updated_at",
                  LocalDateTime.parse("2025-01-03T09:00:00"),
                  "operation_type",
                  "U"),
              Map.of(
                  "id",
                  3L,
                  "name",
                  "Bob",
                  "value",
                  new BigDecimal(299.95),
                  "updated_at",
                  LocalDateTime.parse("2025-01-03T10:00:00"),
                  "operation_type",
                  "U"),
              Map.of(
                  "id",
                  4L,
                  "name",
                  "Alice",
                  "value",
                  new BigDecimal(399.94),
                  "updated_at",
                  LocalDateTime.parse("2025-01-03T11:00:00"),
                  "operation_type",
                  "U"));
    }

    mergeInputs.add(
        new SCD2MergeInput(
            inputData,
            tableFilter,
            keyColumns,
            null,
            changeTrackingColumns,
            changeTrackingMetadata,
            isSnapshotMerge));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "value",
                new BigDecimal(100.0).setScale(10, RoundingMode.HALF_UP),
                "updated_at",
                LocalDateTime.parse("2025-01-01T10:00:00"),
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-03T00:00:00")),
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "value",
                new BigDecimal(100.06).setScale(10, RoundingMode.HALF_UP),
                "updated_at",
                LocalDateTime.parse("2025-01-03T08:00:00"),
                "effective_start",
                LocalDateTime.parse("2025-01-03T00:00:00")),
            new HashMap<String, Object>() {
              {
                put("id", 2L);
                put("name", null);
                put("value", new BigDecimal(200.0).setScale(10, RoundingMode.HALF_UP));
                put("updated_at", LocalDateTime.parse("2025-01-01T11:00:00"));
                put("effective_start", LocalDateTime.parse("2025-01-01T00:00:00"));
                put("effective_end", LocalDateTime.parse("2025-01-03T00:00:00"));
              }
            },
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "value",
                new BigDecimal(200.0).setScale(10, RoundingMode.HALF_UP),
                "updated_at",
                LocalDateTime.parse("2025-01-03T09:00:00"),
                "effective_start",
                LocalDateTime.parse("2025-01-03T00:00:00")),
            Map.of(
                "id",
                3L,
                "name",
                "Bob",
                "value",
                new BigDecimal(300.0).setScale(10, RoundingMode.HALF_UP),
                "updated_at",
                LocalDateTime.parse("2025-01-02T10:00:00"),
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                4L,
                "name",
                "Alice",
                "value",
                new BigDecimal(400.0).setScale(10, RoundingMode.HALF_UP),
                "updated_at",
                LocalDateTime.parse("2025-01-02T11:00:00"),
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-03T00:00:00")),
            Map.of(
                "id",
                4L,
                "name",
                "Alice",
                "value",
                new BigDecimal(399.94).setScale(10, RoundingMode.HALF_UP),
                "updated_at",
                LocalDateTime.parse("2025-01-03T11:00:00"),
                "effective_start",
                LocalDateTime.parse("2025-01-03T00:00:00")));

    return Arguments.of(
        "change_tracking_columns" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        mergeInputs,
        expectedData,
        null,
        null);
  }

  private static Arguments currentFlagColumnTestCase(boolean isSnapshotMerge) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()),
            Types.NestedField.required(5, "is_current", Types.BooleanType.get()));

    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<List<Map<String, Object>>> inputDataList = null;
    if (isSnapshotMerge) {
      inputDataList =
          Arrays.asList(
              Arrays.asList(
                  Map.of("id", 1L, "name", "John Doe"),
                  Map.of("id", 2L, "name", "Jane"),
                  Map.of("id", 3L, "name", "Bob")),
              Arrays.asList(
                  Map.of("id", 2L, "name", "Jane Doe"),
                  Map.of("id", 3L, "name", "Bob"),
                  Map.of("id", 5L, "name", "Jane Doe")),
              Arrays.asList(
                  Map.of("id", 5L, "name", "Jane Doe"),
                  Map.of("id", 4L, "name", "Alice"),
                  Map.of("id", 3L, "name", "Robert")));
    } else {
      inputDataList =
          Arrays.asList(
              Arrays.asList(
                  Map.of("id", 1L, "name", "John Doe", "operation_type", "U"),
                  Map.of("id", 3L, "name", "Bob", "operation_type", "I")),
              Arrays.asList(
                  Map.of("id", 2L, "name", "Jane Doe", "operation_type", "U"),
                  Map.of("id", 1L, "name", "John", "operation_type", "D"),
                  Map.of("id", 5L, "name", "Jane Doe", "operation_type", "I")),
              Arrays.asList(
                  Map.of("id", 2L, "name", "Jane Doe", "operation_type", "D"),
                  Map.of("id", 4L, "name", "Alice", "operation_type", "I"),
                  Map.of("id", 3L, "name", "Robert", "operation_type", "U")));
    }
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00"),
                "is_current",
                false),
            Map.of(
                "id",
                1L,
                "name",
                "John Doe",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-03T00:00:00"),
                "is_current",
                false),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-03T00:00:00"),
                "is_current",
                false),
            Map.of(
                "id",
                2L,
                "name",
                "Jane Doe",
                "effective_start",
                LocalDateTime.parse("2025-01-03T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-04T00:00:00"),
                "is_current",
                false),
            Map.of(
                "id",
                3L,
                "name",
                "Bob",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-04T00:00:00"),
                "is_current",
                false),
            Map.of(
                "id",
                3L,
                "name",
                "Robert",
                "effective_start",
                LocalDateTime.parse("2025-01-04T00:00:00"),
                "is_current",
                true),
            Map.of(
                "id",
                4L,
                "name",
                "Alice",
                "effective_start",
                LocalDateTime.parse("2025-01-04T00:00:00"),
                "is_current",
                true),
            Map.of(
                "id",
                5L,
                "name",
                "Jane Doe",
                "effective_start",
                LocalDateTime.parse("2025-01-03T00:00:00"),
                "is_current",
                true));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    List<SCD2MergeInput> mergeInputs =
        inputDataList.stream()
            .map(
                inputData ->
                    new SCD2MergeInput(
                        inputData,
                        tableFilter,
                        keyColumns,
                        "is_current",
                        null,
                        null,
                        isSnapshotMerge))
            .collect(Collectors.toList());

    return Arguments.of(
        "current_flag_column" + (isSnapshotMerge ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        mergeInputs,
        expectedData,
        null,
        null);
  }

  @ParameterizedTest
  @MethodSource("providePartitionStrategies")
  void testDifferentPartitionStrategies(
      PartitionSpec partitionSpec,
      List<Pair<String, Long>> partitionLevelRecordCounts,
      boolean isSnapshotMode) {
    Schema schema = getSchemaForPartitionStrategiesTest();

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd2_partition_strategy_test_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "date",
                LocalDate.parse("2025-01-01"),
                "timestamp",
                LocalDateTime.parse("2025-01-01T02:45:33"),
                "name",
                "John"),
            Map.of(
                "id",
                2L,
                "date",
                LocalDate.parse("2025-01-02"),
                "timestamp",
                LocalDateTime.parse("2025-01-01T02:45:33"),
                "name",
                "Jane"));

    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00");

    List<Map<String, Object>> inputData = null;
    if (isSnapshotMode) {
      inputData =
          Arrays.asList(
              Map.of(
                  "id",
                  1L,
                  "date",
                  LocalDate.parse("2024-02-02"),
                  "timestamp",
                  LocalDateTime.parse("2024-04-04T12:00:00"),
                  "name",
                  "John Doe"),
              Map.of(
                  "id",
                  2L,
                  "date",
                  LocalDate.parse("2025-01-02"),
                  "timestamp",
                  LocalDateTime.parse("2025-01-01T02:45:33"),
                  "name",
                  "Jane"),
              Map.of(
                  "id",
                  3L,
                  "date",
                  LocalDate.parse("2025-01-03"),
                  "timestamp",
                  LocalDateTime.parse("2025-02-02T06:06:06"),
                  "name",
                  "Bob"));
    } else {
      inputData =
          Arrays.asList(
              Map.of(
                  "id",
                  1L,
                  "date",
                  LocalDate.parse("2024-02-02"),
                  "timestamp",
                  LocalDateTime.parse("2024-04-04T12:00:00"),
                  "name",
                  "John Doe",
                  "operation_type",
                  "U"),
              Map.of(
                  "id",
                  3L,
                  "date",
                  LocalDate.parse("2025-01-03"),
                  "timestamp",
                  LocalDateTime.parse("2025-02-02T06:06:06"),
                  "name",
                  "Bob",
                  "operation_type",
                  "I"));
    }
    String changeSql = TestUtil.createChangeSql(inputData, schema);
    SCD2MergeIntegrationTestUtil.performSCD2Merge(
        swiftLakeEngine,
        isSnapshotMode,
        tableName,
        "id IS NOT NULL",
        Arrays.asList("id"),
        changeSql,
        LocalDateTime.parse("2025-01-02T00:00:00"));

    // Verify the results
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "date",
                LocalDate.parse("2025-01-01"),
                "timestamp",
                LocalDateTime.parse("2025-01-01T02:45:33"),
                "name",
                "John",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                1L,
                "date",
                LocalDate.parse("2024-02-02"),
                "timestamp",
                LocalDateTime.parse("2024-04-04T12:00:00"),
                "name",
                "John Doe",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                2L,
                "date",
                LocalDate.parse("2025-01-02"),
                "timestamp",
                LocalDateTime.parse("2025-01-01T02:45:33"),
                "name",
                "Jane",
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00")),
            Map.of(
                "id",
                3L,
                "date",
                LocalDate.parse("2025-01-03"),
                "timestamp",
                LocalDateTime.parse("2025-02-02T06:06:06"),
                "name",
                "Bob",
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")));
    expectedData = SCD2MergeIntegrationTestUtil.addNullEffectiveEnd(expectedData);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    // Verify partition level record count
    List<Pair<String, Long>> expectedRecordCounts =
        swiftLakeEngine
            .getPartitionLevelRecordCounts(
                swiftLakeEngine.getTable(tableName), Expressions.alwaysTrue())
            .stream()
            .map(
                r ->
                    Pair.of(
                        r.getLeft().getPartitionValues().stream()
                            .map(
                                pair -> {
                                  Type sourceType = schema.findType(pair.getLeft().sourceId());
                                  Type resultType =
                                      pair.getLeft().transform().getResultType(sourceType);
                                  return new StringBuilder()
                                      .append(pair.getLeft().name())
                                      .append("=")
                                      .append(
                                          pair.getLeft()
                                              .transform()
                                              .toHumanString(resultType, getPartitionValue(pair)))
                                      .toString();
                                })
                            .collect(Collectors.joining("/")),
                        r.getRight()))
            .collect(Collectors.toList());
    assertThat(partitionLevelRecordCounts)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedRecordCounts);

    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  private static <T> T getPartitionValue(Pair<PartitionField, Object> partitionValuePair) {
    return (T) partitionValuePair.getValue();
  }

  private static Schema getSchemaForPartitionStrategiesTest() {
    return new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "date", Types.DateType.get()),
        Types.NestedField.required(3, "timestamp", Types.TimestampType.withoutZone()),
        Types.NestedField.required(4, "name", Types.StringType.get()),
        Types.NestedField.required(5, "effective_start", Types.TimestampType.withoutZone()),
        Types.NestedField.optional(6, "effective_end", Types.TimestampType.withoutZone()));
  }

  private static Stream<Arguments> providePartitionStrategies() {
    Schema schema = getSchemaForPartitionStrategiesTest();
    var argumentsList =
        Arrays.asList(
            Arrays.asList(PartitionSpec.unpartitioned(), Arrays.asList(Pair.of("", 4L))),
            Arrays.asList(
                PartitionSpec.builderFor(schema).identity("id").build(),
                Arrays.asList(Pair.of("id=1", 2L), Pair.of("id=2", 1L), Pair.of("id=3", 1L))),
            Arrays.asList(
                PartitionSpec.builderFor(schema).year("date").build(),
                Arrays.asList(Pair.of("date_year=2024", 1L), Pair.of("date_year=2025", 3L))),
            Arrays.asList(
                PartitionSpec.builderFor(schema).identity("id").year("date").build(),
                Arrays.asList(
                    Pair.of("id=1/date_year=2024", 1L),
                    Pair.of("id=1/date_year=2025", 1L),
                    Pair.of("id=2/date_year=2025", 1L),
                    Pair.of("id=3/date_year=2025", 1L))),
            Arrays.asList(
                PartitionSpec.builderFor(schema).month("date").bucket("id", 5).build(),
                Arrays.asList(
                    Pair.of("date_month=2025-01/id_bucket=1", 1L),
                    Pair.of("date_month=2024-02/id_bucket=1", 1L),
                    Pair.of("date_month=2025-01/id_bucket=0", 1L),
                    Pair.of("date_month=2025-01/id_bucket=2", 1L))),
            Arrays.asList(
                PartitionSpec.builderFor(schema)
                    .month("date")
                    .truncate("name", 3)
                    .bucket("effective_end", 1)
                    .build(),
                Arrays.asList(
                    Pair.of("date_month=2025-01/name_trunc=Joh/effective_end_bucket=0", 1L),
                    Pair.of("date_month=2024-02/name_trunc=Joh/effective_end_bucket=null", 1L),
                    Pair.of("date_month=2025-01/name_trunc=Jan/effective_end_bucket=null", 1L),
                    Pair.of("date_month=2025-01/name_trunc=Bob/effective_end_bucket=null", 1L))),
            Arrays.asList(
                PartitionSpec.builderFor(schema).hour("timestamp").identity("date").build(),
                Arrays.asList(
                    Pair.of("timestamp_hour=2025-01-01-02/date=2025-01-01", 1L),
                    Pair.of("timestamp_hour=2025-01-01-02/date=2025-01-02", 1L),
                    Pair.of("timestamp_hour=2024-04-04-12/date=2024-02-02", 1L),
                    Pair.of("timestamp_hour=2025-02-02-06/date=2025-01-03", 1L))),
            Arrays.asList(
                PartitionSpec.builderFor(schema).day("timestamp").truncate("id", 2).build(),
                Arrays.asList(
                    Pair.of("timestamp_day=2025-01-01/id_trunc=0", 1L),
                    Pair.of("timestamp_day=2024-04-04/id_trunc=0", 1L),
                    Pair.of("timestamp_day=2025-01-01/id_trunc=2", 1L),
                    Pair.of("timestamp_day=2025-02-02/id_trunc=2", 1L))),
            Arrays.asList(
                PartitionSpec.builderFor(schema).month("timestamp").month("date").build(),
                Arrays.asList(
                    Pair.of("timestamp_month=2025-01/date_month=2025-01", 2L),
                    Pair.of("timestamp_month=2024-04/date_month=2024-02", 1L),
                    Pair.of("timestamp_month=2025-02/date_month=2025-01", 1L))));

    List<Arguments> finalArgumentsList = new ArrayList<>();
    for (var arguments : argumentsList) {
      Object[] arr = Arrays.copyOf(arguments.toArray(), arguments.size() + 1);
      arr[arguments.size()] = true;
      finalArgumentsList.add(Arguments.of(arr));
      arr = Arrays.copyOf(arguments.toArray(), arguments.size() + 1);
      arr[arguments.size()] = false;
      finalArgumentsList.add(Arguments.of(arr));
    }
    return finalArgumentsList.stream();
  }

  @ParameterizedTest
  @Execution(ExecutionMode.CONCURRENT)
  @ValueSource(booleans = {true, false})
  void testConcurrentOperations(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("id").bucket("effective_end", 1).build();

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd2_concurrent_operations_test_"
                + (isSnapshotMode ? "snapshot_mode" : "changes_mode"));
    swiftLakeEngine
        .getCatalog()
        .createTable(tableId, schema, partitionSpec, Map.of("commit.retry.num-retries", "10"));
    String tableName = tableId.toString();

    LocalDateTime initialEffectiveTimestamp = LocalDateTime.parse("2025-01-01T00:00:00");
    List<Map<String, Object>> expectedData = new ArrayList<>();

    // Perform multiple concurrent merge operations
    for (int j = 0; j < 3; j++) {
      LocalDateTime effectiveTimestamp = initialEffectiveTimestamp.plusDays(j);
      int index = j;
      IntStream.range(0, 10)
          .parallel()
          .forEach(
              i -> {
                List<Map<String, Object>> data =
                    Arrays.asList(
                        Map.of("id", (long) i, "name", "name_" + i + index, "operation_type", "I"));
                String changeSql = TestUtil.createChangeSql(data, schema);
                SCD2MergeIntegrationTestUtil.performSCD2Merge(
                    swiftLakeEngine,
                    isSnapshotMode,
                    tableName,
                    "id = " + i,
                    Arrays.asList("id"),
                    changeSql,
                    effectiveTimestamp);
              });

      expectedData.addAll(
          IntStream.range(0, 10)
              .mapToObj(
                  i ->
                      new HashMap<String, Object>() {
                        {
                          put("id", (long) i);
                          put("name", "name_" + i + index);
                          put("effective_start", effectiveTimestamp);
                          put("effective_end", index < 2 ? effectiveTimestamp.plusDays(1) : null);
                        }
                      })
              .collect(Collectors.toList()));
    }

    // Verify the results
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRetroactiveChangesValidation(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()),
            Types.NestedField.required(3, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "effective_end", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd2_retroactive_changes_test_" + (isSnapshotMode ? "snapshot_mode" : "changes_mode"));
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    List<Map<String, Object>> initialData = Arrays.asList(Map.of("id", 1L, "value", "Initial"));
    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00");

    // Make a change
    List<Map<String, Object>> inputData1 =
        Arrays.asList(Map.of("id", 1L, "value", "Change 1", "operation_type", "U"));
    String changeSql1 = TestUtil.createChangeSql(inputData1, schema);
    SCD2MergeIntegrationTestUtil.performSCD2Merge(
        swiftLakeEngine,
        isSnapshotMode,
        tableName,
        "id IS NOT NULL",
        Arrays.asList("id"),
        changeSql1,
        LocalDateTime.parse("2025-01-02T00:00:00"));

    // Make a retroactive change
    List<Map<String, Object>> inputData2 =
        Arrays.asList(Map.of("id", 1L, "value", "Retroactive Change", "operation_type", "U"));
    String changeSql2 = TestUtil.createChangeSql(inputData2, schema);

    assertThatThrownBy(
            () ->
                SCD2MergeIntegrationTestUtil.performSCD2Merge(
                    swiftLakeEngine,
                    isSnapshotMode,
                    tableName,
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    changeSql2,
                    LocalDateTime.parse("2022-12-31T00:00:00")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Out-of-order records detected");
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testPartitionEvolutionWithDataMovement(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "year", Types.IntegerType.get()),
            Types.NestedField.required(4, "month", Types.IntegerType.get()),
            Types.NestedField.required(5, "value", Types.DoubleType.get()),
            Types.NestedField.required(6, "effective_start", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(7, "effective_end", Types.TimestampType.withoutZone()));

    // Start with partitioning by year
    PartitionSpec initialSpec = PartitionSpec.builderFor(schema).identity("year").build();
    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "scd2_partition_evolution_data_movement_test_"
                + (isSnapshotMode ? "snapshot_mode" : "changes_mode"));
    swiftLakeEngine.getCatalog().createTable(tableId, schema, initialSpec);
    String tableName = tableId.toString();

    // Initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "year", 2025, "month", 1, "value", 100.0),
            Map.of("id", 2L, "name", "Jane", "year", 2025, "month", 2, "value", 200.0));

    SCD2MergeIntegrationTestUtil.insertDataIntoSCD2Table(
        swiftLakeEngine, tableName, schema, initialData, "2025-01-01T00:00:00");

    // Perform first SCD2 merge
    List<Map<String, Object>> inputData1 = null;
    if (isSnapshotMode) {
      inputData1 =
          Arrays.asList(
              Map.of("id", 1L, "name", "John Doe", "year", 2025, "month", 1, "value", 150.0),
              Map.of("id", 2L, "name", "Jane", "year", 2025, "month", 2, "value", 200.0),
              Map.of("id", 3L, "name", "Bob", "year", 2025, "month", 3, "value", 300.0));
    } else {
      inputData1 =
          Arrays.asList(
              Map.of(
                  "id",
                  1L,
                  "name",
                  "John Doe",
                  "year",
                  2025,
                  "month",
                  1,
                  "value",
                  150.0,
                  "operation_type",
                  "U"),
              Map.of(
                  "id",
                  3L,
                  "name",
                  "Bob",
                  "year",
                  2025,
                  "month",
                  3,
                  "value",
                  300.0,
                  "operation_type",
                  "I"));
    }
    String changeSql1 = TestUtil.createChangeSql(inputData1, schema);
    SCD2MergeIntegrationTestUtil.performSCD2Merge(
        swiftLakeEngine,
        isSnapshotMode,
        tableName,
        "id IS NOT NULL",
        Arrays.asList("id"),
        changeSql1,
        LocalDateTime.parse("2025-01-02T00:00:00"));

    // Evolve partition spec to include month
    swiftLakeEngine.getTable(tableName).updateSpec().addField("month").commit();

    // Perform second SCD2 merge with new partition spec
    List<Map<String, Object>> inputData2 = null;
    if (isSnapshotMode) {
      inputData2 =
          Arrays.asList(
              Map.of("id", 1L, "name", "John Doe", "year", 2025, "month", 1, "value", 150.0),
              Map.of("id", 3L, "name", "Bob", "year", 2025, "month", 3, "value", 300.0),
              Map.of("id", 2L, "name", "Jane Doe", "year", 2025, "month", 2, "value", 250.0),
              Map.of("id", 4L, "name", "Alice", "year", 2025, "month", 4, "value", 400.0));
    } else {
      inputData2 =
          Arrays.asList(
              Map.of(
                  "id",
                  2L,
                  "name",
                  "Jane Doe",
                  "year",
                  2025,
                  "month",
                  2,
                  "value",
                  250.0,
                  "operation_type",
                  "U"),
              Map.of(
                  "id",
                  4L,
                  "name",
                  "Alice",
                  "year",
                  2025,
                  "month",
                  4,
                  "value",
                  400.0,
                  "operation_type",
                  "I"));
    }
    String changeSql2 = TestUtil.createChangeSql(inputData2, schema);
    SCD2MergeIntegrationTestUtil.performSCD2Merge(
        swiftLakeEngine,
        isSnapshotMode,
        tableName,
        "id IS NOT NULL",
        Arrays.asList("id"),
        changeSql2,
        LocalDateTime.parse("2025-01-03T00:00:00"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "John",
                "year",
                2025,
                "month",
                1,
                "value",
                100.0,
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                1L,
                "name",
                "John Doe",
                "year",
                2025,
                "month",
                1,
                "value",
                150.0,
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane",
                "year",
                2025,
                "month",
                2,
                "value",
                200.0,
                "effective_start",
                LocalDateTime.parse("2025-01-01T00:00:00"),
                "effective_end",
                LocalDateTime.parse("2025-01-03T00:00:00")),
            Map.of(
                "id",
                2L,
                "name",
                "Jane Doe",
                "year",
                2025,
                "month",
                2,
                "value",
                250.0,
                "effective_start",
                LocalDateTime.parse("2025-01-03T00:00:00")),
            Map.of(
                "id",
                3L,
                "name",
                "Bob",
                "year",
                2025,
                "month",
                3,
                "value",
                300.0,
                "effective_start",
                LocalDateTime.parse("2025-01-02T00:00:00")),
            Map.of(
                "id",
                4L,
                "name",
                "Alice",
                "year",
                2025,
                "month",
                4,
                "value",
                400.0,
                "effective_start",
                LocalDateTime.parse("2025-01-03T00:00:00")));

    expectedData = SCD2MergeIntegrationTestUtil.addNullEffectiveEnd(expectedData);
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    // Verify that data is properly partitioned
    List<DataFile> dataFiles =
        swiftLakeEngine
            .getIcebergScanExecutor()
            .executeTableScan(swiftLakeEngine.getTable(tableName), Expressions.alwaysTrue())
            .getScanResult()
            .getValue();
    assertThat(dataFiles).hasSize(4);
    var partitions =
        dataFiles.stream()
            .map(
                f ->
                    Arrays.asList(
                        f.partition().get(0, Integer.class), f.partition().get(1, Integer.class)))
            .collect(Collectors.toList());
    assertThat(partitions)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(
            Arrays.asList(
                Arrays.asList(2025, 1),
                Arrays.asList(2025, 2),
                Arrays.asList(2025, 3),
                Arrays.asList(2025, 4)));
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  public static class SCD2MergeInput {
    boolean isSnapshot;
    List<Map<String, Object>> inputData;
    String tableFilter;
    List<String> keyColumns;
    String currentFlagColumn;
    List<String> changeTrackingColumns;
    Map<String, ChangeTrackingMetadata<?>> changeTrackingMetadata;

    public SCD2MergeInput(
        List<Map<String, Object>> inputData,
        String tableFilter,
        List<String> keyColumns,
        boolean isSnapshot) {
      this.inputData = inputData;
      this.tableFilter = tableFilter;
      this.keyColumns = keyColumns;
      this.isSnapshot = isSnapshot;
    }

    public SCD2MergeInput(
        List<Map<String, Object>> inputData,
        String tableFilter,
        List<String> keyColumns,
        String currentFlagColumn,
        List<String> changeTrackingColumns,
        Map<String, ChangeTrackingMetadata<?>> changeTrackingMetadata,
        boolean isSnapshot) {
      this.isSnapshot = isSnapshot;
      this.inputData = inputData;
      this.tableFilter = tableFilter;
      this.keyColumns = keyColumns;
      this.currentFlagColumn = currentFlagColumn;
      this.changeTrackingColumns = changeTrackingColumns;
      this.changeTrackingMetadata = changeTrackingMetadata;
    }
  }
}
