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
import com.arcesium.swiftlake.metrics.CommitMetrics;
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
import java.util.Collections;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SCD1MergeBasicIntegrationTest {
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
  void testSCD1Merge(
      String testName,
      Schema schema,
      PartitionSpec partitionSpec,
      List<Map<String, Object>> initialData,
      List<SCD1MergeInput> mergeInputList,
      List<Map<String, Object>> expectedData,
      List<Consumer<Table>> evolveSchema,
      List<Pair<Class<? extends Throwable>, String>> errors) {
    // Create table
    TableIdentifier tableId = TableIdentifier.of("test_db", "scd1_test_table_" + testName);
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    // Insert initial data
    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    for (int i = 0; i < mergeInputList.size(); i++) {
      if (evolveSchema != null && evolveSchema.size() > i && evolveSchema.get(i) != null) {
        evolveSchema.get(i).accept(swiftLakeEngine.getTable(tableName));
        schema = swiftLakeEngine.getTable(tableName).schema();
      }
      SCD1MergeInput mergeInput = mergeInputList.get(i);

      Runnable mergeFunc;
      if (mergeInput.isSnapshot) {
        // Snapshot mode
        String sourceSql = TestUtil.createSelectSql(mergeInput.inputData, schema);
        mergeFunc =
            () -> {
              var builder =
                  swiftLakeEngine
                      .applySnapshotAsSCD1(tableName)
                      .tableFilterSql(mergeInput.tableFilter)
                      .sourceSql(sourceSql)
                      .keyColumns(mergeInput.keyColumns)
                      .processSourceTables(false);

              // Apply optional parameters
              if (mergeInput.valueColumns != null) {
                builder.valueColumns(mergeInput.valueColumns);
              }

              if (mergeInput.valueColumnMetadata != null) {
                mergeInput.valueColumnMetadata.forEach(
                    (column, metadata) -> builder.valueColumnMetadata(column, metadata));
              }

              if (mergeInput.skipEmptySource != null) {
                builder.skipEmptySource(mergeInput.skipEmptySource);
              }

              builder.execute();
            };
      } else {
        // Changes mode
        String changeSql =
            TestUtil.createChangeSql(mergeInput.inputData, schema, mergeInput.operationTypeColumn);
        mergeFunc =
            () ->
                swiftLakeEngine
                    .applyChangesAsSCD1(tableName)
                    .tableFilterSql(mergeInput.tableFilter)
                    .sourceSql(changeSql)
                    .keyColumns(mergeInput.keyColumns)
                    .operationTypeColumn(
                        mergeInput.operationTypeColumn, mergeInput.deleteOperationValue)
                    .processSourceTables(false)
                    .execute();
      }

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
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  private static Stream<Arguments> provideTestCases() {
    return Stream.concat(
        Stream.of(
            simpleSchemaTestCase(false),
            simpleSchemaTestCase(true),
            complexTypesTestCase(false),
            complexTypesTestCase(true),
            noChangesTestCase(false),
            noChangesTestCase(true),
            allInsertsTestCase(false),
            allInsertsTestCase(true),
            allDeletesTestCase(false),
            allDeletesTestCase(true),
            emptySourceTestCase(false),
            emptySourceTestCase(true),
            multipleOperationsTestCase(false),
            multipleOperationsTestCase(true),
            nullValuesTestCase(false),
            nullValuesTestCase(true),
            replaceAllTestCase(false),
            replaceAllTestCase(true),
            partialMergeWithFiltersTestCase(false),
            partialMergeWithFiltersTestCase(true),
            longHistoryChainTestCase(false),
            longHistoryChainTestCase(true),
            extremeValuesTestCase(false),
            extremeValuesTestCase(true),
            unicodeAndSpecialCharactersTestCase(false),
            unicodeAndSpecialCharactersTestCase(true),
            timeZoneHandlingTestCase(false),
            timeZoneHandlingTestCase(true),
            schemaEvolutionTestCase(false),
            schemaEvolutionTestCase(true),
            multiColumnKeyTestCase(false),
            multiColumnKeyTestCase(true),
            errorHandlingTestCase(false),
            errorHandlingTestCase(true),
            valueColumnsTestCase(),
            valueColumnsWithMaxDeltaTestCase(),
            valueColumnsWithNullReplacementTestCase(),
            skipEmptySourceTestCase()),
        provideOperationTypeValidationTestCases());
  }

  private static Arguments simpleSchemaTestCase(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<Map<String, Object>> inputData;

    if (isSnapshotMode) {
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
            Map.of("id", 1L, "name", "John Doe"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode);
    return Arguments.of(
        "simple_schema" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments complexTypesTestCase(boolean isSnapshotMode) {
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
                Types.MapType.ofRequired(15, 16, Types.StringType.get(), Types.IntegerType.get())));
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("id").build();

    List<Map<String, Object>> initialData =
        Arrays.asList(createComplexRow(1L, "2025-01-01"), createComplexRow(2L, "2025-01-01"));

    List<Map<String, Object>> inputData;

    if (isSnapshotMode) {
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
            createExpectedComplexRow(1L, "2025-01-01", true),
            createExpectedComplexRow(2L, "2025-01-01", false),
            createExpectedComplexRow(3L, "2025-01-01", false));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode);
    return Arguments.of(
        "complex_types" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments noChangesTestCase(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<Map<String, Object>> inputData;

    if (isSnapshotMode) {
      inputData = Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));
    } else {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John", "operation_type", "U"),
              Map.of("id", 2L, "name", "Jane", "operation_type", "U"));
    }

    List<Map<String, Object>> expectedData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode);
    return Arguments.of(
        "no_changes" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments allInsertsTestCase(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData = new ArrayList<>();

    List<Map<String, Object>> inputData;

    if (isSnapshotMode) {
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
            Map.of("id", 1L, "name", "John"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode);
    return Arguments.of(
        "all_inserts" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments allDeletesTestCase(boolean isSnapshotMode) {
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

    List<Map<String, Object>> inputData;

    if (isSnapshotMode) {
      // Empty snapshot - will delete all records matching the filter
      inputData = new ArrayList<>();
    } else {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John", "operation_type", "D"),
              Map.of("id", 2L, "name", "Jane", "operation_type", "D"),
              Map.of("id", 3L, "name", "Bob", "operation_type", "D"));
    }

    // Empty result - all records deleted
    List<Map<String, Object>> expectedData = Arrays.asList();

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode);
    return Arguments.of(
        "all_deletes" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments emptySourceTestCase(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    // Empty input data
    List<Map<String, Object>> inputData = new ArrayList<>();

    List<Map<String, Object>> expectedData;

    if (isSnapshotMode) {
      // In snapshot mode, empty source means delete all records that match the filter
      expectedData = new ArrayList<>();
    } else {
      // In changes mode, empty source means no changes
      expectedData =
          Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));
    }

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode);
    return Arguments.of(
        "empty_source" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments multipleOperationsTestCase(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<List<Map<String, Object>>> inputDataList;

    if (isSnapshotMode) {
      inputDataList =
          Arrays.asList(
              // First snapshot
              Arrays.asList(
                  Map.of("id", 1L, "name", "John Doe"),
                  Map.of("id", 2L, "name", "Jane"),
                  Map.of("id", 3L, "name", "Bob")),
              // Second snapshot
              Arrays.asList(
                  Map.of("id", 2L, "name", "Jane Doe"),
                  Map.of("id", 3L, "name", "Bob"),
                  Map.of("id", 5L, "name", "Jane Doe")),
              // Third snapshot
              Arrays.asList(
                  Map.of("id", 3L, "name", "Robert"),
                  Map.of("id", 4L, "name", "Alice"),
                  Map.of("id", 5L, "name", "Jane Doe")));
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
            Map.of("id", 3L, "name", "Robert"),
            Map.of("id", 4L, "name", "Alice"),
            Map.of("id", 5L, "name", "Jane Doe"));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    List<SCD1MergeInput> mergeInputs =
        inputDataList.stream()
            .map(
                inputData -> new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode))
            .collect(Collectors.toList());

    return Arguments.of(
        "multiple_operations" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        mergeInputs,
        expectedData,
        null,
        null);
  }

  private static Arguments longHistoryChainTestCase(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData = Arrays.asList(Map.of("id", 1L, "value", "Initial"));

    List<List<Map<String, Object>>> inputDataList = new ArrayList<>();
    List<Map<String, Object>> expectedData = new ArrayList<>();

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
              // new order value_2,id
              table.updateSchema().moveFirst("value_2").commit();
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
      } else if (i < 20) {
        inputDataList.add(
            Arrays.asList(Map.of("id", 1L, "value", "Update" + i, "operation_type", "U")));
      } else if (i < 50) {
        inputDataList.add(
            Arrays.asList(Map.of("id", 1L, "value_2", "Update" + i, "operation_type", "U")));
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
        if (index == 99) {
          expectedData.add(
              new HashMap<>() {
                {
                  put("id", 1L);
                  put("value_2", expectedStringValue);
                  put("value_3", expectedStringValue);
                  put("value_5", decimal38PrecisionValue);
                }
              });
        }
      }
    }

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    List<SCD1MergeInput> mergeInputs =
        inputDataList.stream()
            .map(
                inputData -> new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode))
            .collect(Collectors.toList());

    return Arguments.of(
        "long_history_chain" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
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
      Long id, String date, boolean isUpdated) {
    Map<String, Object> row = createComplexRow(id, date);
    if (isUpdated) {
      row.put("string_col", "updated_string_" + id);
      row.put("int_col", id.intValue() + 100);
    }
    return row;
  }

  private static Arguments nullValuesTestCase(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "nullable_string", Types.StringType.get()),
            Types.NestedField.optional(3, "nullable_int", Types.IntegerType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "nullable_string", "value", "nullable_int", 10));

    List<Map<String, Object>> inputData;

    if (isSnapshotMode) {
      inputData =
          Arrays.asList(
              new HashMap<>() {
                {
                  put("id", 1L);
                  put("nullable_string", null);
                  put("nullable_int", null);
                }
              },
              new HashMap<>() {
                {
                  put("id", 2L);
                  put("nullable_string", "new");
                  put("nullable_int", null);
                }
              });
    } else {
      inputData =
          Arrays.asList(
              new HashMap<>() {
                {
                  put("id", 1L);
                  put("nullable_string", null);
                  put("nullable_int", null);
                  put("operation_type", "U");
                }
              },
              new HashMap<>() {
                {
                  put("id", 2L);
                  put("nullable_string", "new");
                  put("nullable_int", null);
                  put("operation_type", "I");
                }
              });
    }

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            new HashMap<>() {
              {
                put("id", 1L);
                put("nullable_string", null);
                put("nullable_int", null);
              }
            },
            new HashMap<>() {
              {
                put("id", 2L);
                put("nullable_string", "new");
                put("nullable_int", null);
              }
            });

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode);
    return Arguments.of(
        "null_values" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments extremeValuesTestCase(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "long_string", Types.StringType.get()),
            Types.NestedField.required(3, "big_number", Types.DecimalType.of(38, 10)));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    String longString = "a".repeat(100000);
    BigDecimal bigNumber = new BigDecimal("12345678901234567890.1234567890");

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "long_string", "initial", "big_number", BigDecimal.ONE));

    List<Map<String, Object>> inputData;

    if (isSnapshotMode) {
      inputData =
          Arrays.asList(
              Map.of(
                  "id", 1L,
                  "long_string", longString,
                  "big_number", bigNumber));
    } else {
      inputData =
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
    }

    List<Map<String, Object>> expectedData =
        Arrays.asList(Map.of("id", 1L, "long_string", longString, "big_number", bigNumber));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode);
    return Arguments.of(
        "extreme_values" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments unicodeAndSpecialCharactersTestCase(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "special_string", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    String specialString = "!@#$%^&*()_+{ ¡™£¢∞§¶•ªº}[]|\\:;こんにちは\"'<你好，世界>,.?/~`";

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "special_string", "normal"));

    List<Map<String, Object>> inputData;

    if (isSnapshotMode) {
      inputData = Arrays.asList(Map.of("id", 1L, "special_string", specialString));
    } else {
      inputData =
          Arrays.asList(Map.of("id", 1L, "special_string", specialString, "operation_type", "U"));
    }

    List<Map<String, Object>> expectedData =
        Arrays.asList(Map.of("id", 1L, "special_string", specialString));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode);
    return Arguments.of(
        "special_characters" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments timeZoneHandlingTestCase(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "timestamp_with_tz", Types.TimestampType.withZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    OffsetDateTime utcTime = OffsetDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    OffsetDateTime changedTime =
        utcTime.withOffsetSameInstant(ZoneOffset.ofHours(-5)).plusSeconds(2);

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "timestamp_with_tz", utcTime));

    List<Map<String, Object>> inputData;

    if (isSnapshotMode) {
      inputData = Arrays.asList(Map.of("id", 1L, "timestamp_with_tz", changedTime));
    } else {
      inputData =
          Arrays.asList(Map.of("id", 1L, "timestamp_with_tz", changedTime, "operation_type", "U"));
    }

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id", 1L, "timestamp_with_tz", changedTime.withOffsetSameInstant(ZoneOffset.UTC)));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode);
    return Arguments.of(
        "time_zone_handling" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments schemaEvolutionTestCase(boolean isSnapshotMode) {
    Schema initialSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    Consumer<Table> evolveSchema =
        (table) -> {
          table.updateSchema().addColumn("email", Types.StringType.get()).commit();
        };

    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData = Arrays.asList(Map.of("id", 1L, "name", "John"));

    List<Map<String, Object>> inputData;

    if (isSnapshotMode) {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John Doe", "email", "john.doe@example.com"),
              Map.of("id", 2L, "name", "Jane", "email", "jane@example.com"));
    } else {
      inputData =
          Arrays.asList(
              Map.of(
                  "id", 1L,
                  "name", "John Doe",
                  "email", "john.doe@example.com",
                  "operation_type", "U"),
              Map.of("id", 2L, "name", "Jane", "email", "jane@example.com", "operation_type", "I"));
    }

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe", "email", "john.doe@example.com"),
            Map.of("id", 2L, "name", "Jane", "email", "jane@example.com"));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode);
    return Arguments.of(
        "schema_evolution" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        initialSchema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        Arrays.asList(evolveSchema),
        null);
  }

  private static Arguments multiColumnKeyTestCase(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id1", Types.LongType.get()),
            Types.NestedField.required(2, "id2", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id1", 1L, "id2", "A", "value", "Initial1"),
            Map.of("id1", 1L, "id2", "B", "value", "Initial2"),
            Map.of("id1", 2L, "id2", "A", "value", "Initial3"));

    List<Map<String, Object>> inputData;
    List<Map<String, Object>> expectedData;

    if (isSnapshotMode) {
      // In snapshot mode, records in the target table are kept if they match the filter and their
      // key is in the snapshot
      inputData =
          Arrays.asList(
              Map.of("id1", 1L, "id2", "A", "value", "Updated1"),
              Map.of("id1", 1L, "id2", "B", "value", "Initial2"), // keep this one unchanged
              Map.of("id1", 2L, "id2", "B", "value", "New")); // new record

      expectedData =
          Arrays.asList(
              Map.of("id1", 1L, "id2", "A", "value", "Updated1"),
              Map.of("id1", 1L, "id2", "B", "value", "Initial2"),
              Map.of("id1", 2L, "id2", "B", "value", "New"));

      // Note: id1=2, id2=A will be deleted because it's not in the snapshot
    } else {
      inputData =
          Arrays.asList(
              Map.of("id1", 1L, "id2", "A", "value", "Updated1", "operation_type", "U"),
              Map.of("id1", 2L, "id2", "B", "value", "New", "operation_type", "I"),
              Map.of("id1", 2L, "id2", "A", "value", "ToDelete", "operation_type", "D"));

      expectedData =
          Arrays.asList(
              Map.of("id1", 1L, "id2", "A", "value", "Updated1"),
              Map.of("id1", 1L, "id2", "B", "value", "Initial2"),
              Map.of("id1", 2L, "id2", "B", "value", "New"));
    }

    List<String> keyColumns = Arrays.asList("id1", "id2");
    String tableFilter = "id1 IS NOT NULL";
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode);
    return Arguments.of(
        "multi_column_key" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments errorHandlingTestCase(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    // Initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "value", 100.0),
            Map.of("id", 2L, "name", "Jane", "value", 200.0));

    List<SCD1MergeInput> mergeInputs = new ArrayList<>();
    List<Pair<Class<? extends Throwable>, String>> errors = new ArrayList<>();
    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";

    // Missing required columns
    List<Map<String, Object>> inputData;
    if (isSnapshotMode) {
      inputData = Arrays.asList(Map.of("id", 1L, "value", 300.0), Map.of("id", 2L));
    } else {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "value", 300.0, "operation_type", "U"),
              Map.of("id", 2L, "operation_type", "I"));
    }

    mergeInputs.add(new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode));
    errors.add(
        Pair.of(
            ValidationException.class, "Required fields cannot contain null values - name,value"));

    // Null value for required columns
    if (isSnapshotMode) {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John Doe", "value", 300.0),
              new HashMap<String, Object>() {
                {
                  put("id", 3L);
                  put("name", "Bob");
                  put("value", null);
                }
              });
    } else {
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
    }

    mergeInputs.add(new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode));
    errors.add(
        Pair.of(ValidationException.class, "Required fields cannot contain null values - value"));

    // Duplicate records
    if (isSnapshotMode) {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John Doe", "value", 300.0),
              Map.of("id", 1L, "name", "Bob", "value", 400.0));

      mergeInputs.add(new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode));
      errors.add(
          Pair.of(
              ValidationException.class,
              "Merge operation matched a single row from target table data with multiple rows of the source data"));
    } else {
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "name", "John Doe", "value", 300.0, "operation_type", "U"),
              Map.of("id", 1L, "name", "Bob", "value", 400.0, "operation_type", "D"));

      mergeInputs.add(new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode));
      errors.add(
          Pair.of(
              ValidationException.class,
              "Merge operation matched a single row from target table data with multiple rows of the source data"));
    }

    // More duplicate cases
    if (isSnapshotMode) {
      inputData =
          Arrays.asList(
              Map.of("id", 2L, "name", "Jane", "value", 200.0),
              Map.of("id", 2L, "name", "Jane", "value", 200.0));

      mergeInputs.add(new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode));
      errors.add(
          Pair.of(
              ValidationException.class,
              "Merge operation matched a single row from target table data with multiple rows of the source data"));
    } else {
      // Duplicate records - same operation type
      inputData =
          Arrays.asList(
              Map.of("id", 2L, "name", "Jane", "value", 200.0, "operation_type", "U"),
              Map.of("id", 2L, "name", "Jane", "value", 200.0, "operation_type", "D"));

      mergeInputs.add(new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode));
      errors.add(
          Pair.of(
              ValidationException.class,
              "Merge operation matched a single row from target table data with multiple rows of the source data"));
    }

    if (isSnapshotMode) {
      inputData =
          Arrays.asList(
              Map.of("id", 2L, "name", "Jane", "value", 200.0),
              Map.of("id", 2L, "name", "Jane", "value", 200.0));

      mergeInputs.add(new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode));
      errors.add(
          Pair.of(
              ValidationException.class,
              "Merge operation matched a single row from target table data with multiple rows of the source data"));
    } else {
      // Duplicate records - same operation type
      inputData =
          Arrays.asList(
              Map.of("id", 2L, "name", "Jane", "value", 200.0, "operation_type", "U"),
              Map.of("id", 2L, "name", "Jane", "value", 200.0, "operation_type", "U"));

      mergeInputs.add(new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode));
      errors.add(
          Pair.of(
              ValidationException.class,
              "Merge operation matched a single row from target table data with multiple rows of the source data"));
    }

    // Null key columns
    if (isSnapshotMode) {
      inputData = Arrays.asList(Map.of("id", 1, "name", "John", "value", 101.0));
    } else {
      inputData =
          Arrays.asList(Map.of("id", 1, "name", "John", "value", 101.0, "operation_type", "U"));
    }
    mergeInputs.add(
        new SCD1MergeInput(inputData, "id = 1", null, isSnapshotMode, null, null, null));
    errors.add(Pair.of(ValidationException.class, "Key columns cannot be null or empty"));

    // Empty key columns
    if (isSnapshotMode) {
      inputData = Arrays.asList(Map.of("id", 1, "name", "John", "value", 101.0));
    } else {
      inputData =
          Arrays.asList(Map.of("id", 1, "name", "John", "value", 101.0, "operation_type", "U"));
    }
    mergeInputs.add(
        new SCD1MergeInput(
            inputData,
            "id = 1",
            Collections.emptyList(), // empty key columns
            isSnapshotMode,
            null,
            null,
            null));
    errors.add(Pair.of(ValidationException.class, "Key columns cannot be null or empty"));

    // Key column not in schema
    if (isSnapshotMode) {
      inputData = Arrays.asList(Map.of("id", 1, "name", "John", "value", 101.0));
    } else {
      inputData =
          Arrays.asList(Map.of("id", 1, "name", "John", "value", 101.0, "operation_type", "U"));
    }
    mergeInputs.add(
        new SCD1MergeInput(
            inputData,
            "id = 1",
            Arrays.asList("id", "non_existent_column"), // invalid key column
            isSnapshotMode,
            null,
            null,
            null));
    errors.add(Pair.of(ValidationException.class, "Invalid key column non_existent_column"));

    // Table filter column not in schema
    inputData = Arrays.asList(Map.of("id", 1, "name", "John", "value", 101.0));
    if (isSnapshotMode) {
      mergeInputs.add(
          new SCD1MergeInput(
              inputData,
              "non_existent_column = 1", // invalid filter column
              Arrays.asList("id"),
              isSnapshotMode,
              null,
              null,
              null));
      errors.add(Pair.of(ValidationException.class, "Column does not exist non_existent_column"));
    } else {
      mergeInputs.add(
          new SCD1MergeInput(
              inputData,
              "non_existent_column = 1", // invalid filter column
              Arrays.asList("id"),
              isSnapshotMode,
              null,
              null,
              null));
      errors.add(Pair.of(ValidationException.class, "Column does not exist non_existent_column"));
    }

    if (isSnapshotMode) {
      // Empty value columns list
      inputData = Arrays.asList(Map.of("id", 1, "name", "John", "value", 101.0));
      mergeInputs.add(
          new SCD1MergeInput(
              inputData, "id = 1", Arrays.asList("id"), true, Collections.emptyList(), null, null));
      errors.add(Pair.of(ValidationException.class, "Value columns cannot be empty"));

      // Value column not in schema
      inputData = Arrays.asList(Map.of("id", 1, "name", "John", "value", 101.0));
      mergeInputs.add(
          new SCD1MergeInput(
              inputData,
              "id = 1",
              Arrays.asList("id"),
              true,
              Arrays.asList("name", "non_existent_column"),
              null,
              null));
      errors.add(Pair.of(ValidationException.class, "Invalid value column non_existent_column"));

      // Column cannot be both key and value
      inputData = Arrays.asList(Map.of("id", 1, "name", "John", "value", 101.0));
      mergeInputs.add(
          new SCD1MergeInput(
              inputData,
              "id = 1",
              Arrays.asList("id"),
              true,
              Arrays.asList("id", "value"),
              null,
              null));
      errors.add(
          Pair.of(ValidationException.class, "cannot be both a key column and a value column"));

      // Value column metadata for non-value column
      inputData = Arrays.asList(Map.of("id", 1, "name", "John", "value", 101.0));
      Map<String, ValueColumnMetadata<?>> valueMetadata4 = new HashMap<>();
      valueMetadata4.put("value", new ValueColumnMetadata<>(0.1, null));
      valueMetadata4.put("non_value_column", new ValueColumnMetadata<>(0.5, null));
      mergeInputs.add(
          new SCD1MergeInput(
              inputData,
              "id = 1",
              Arrays.asList("id"),
              true,
              Arrays.asList("name", "value"),
              valueMetadata4,
              null));
      errors.add(Pair.of(ValidationException.class, "Invalid value column non_value_column"));

      // Cannot specify both max delta and null value
      inputData = Arrays.asList(Map.of("id", 1, "name", "John", "value", 101.0));
      Map<String, ValueColumnMetadata<?>> valueMetadata5 = new HashMap<>();
      valueMetadata5.put("value", new ValueColumnMetadata<>(0.1, 0.0));
      mergeInputs.add(
          new SCD1MergeInput(
              inputData,
              "id = 1",
              Arrays.asList("id"),
              true,
              Arrays.asList("name", "value"),
              valueMetadata5,
              null));
      errors.add(
          Pair.of(
              ValidationException.class,
              "Provide either max delta value or null value for the value column"));
    }

    // Final valid input
    if (isSnapshotMode) {
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

    mergeInputs.add(new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe", "value", 300.0),
            Map.of("id", 2L, "name", "Jane", "value", 200.0),
            Map.of("id", 3L, "name", "Bob", "value", 400.0));

    return Arguments.of(
        "error_handling" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        mergeInputs,
        expectedData,
        null,
        errors);
  }

  private static Stream<Arguments> provideOperationTypeValidationTestCases() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    // Common initial data for all tests
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "value", 100.0),
            Map.of("id", 2L, "name", "Jane", "value", 200.0));

    return Stream.of(
        // Delete value provided without operation type column
        Arguments.of(
            "missing_op_type_column",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(Map.of("id", 1L, "name", "John Updated", "value", 101.0)),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    null, // Null operation column
                    "D" // But has delete value
                    )),
            initialData, // No change expected since it errors
            null,
            Collections.singletonList(
                Pair.of(
                    ValidationException.class,
                    "Operation type column must be specified when delete operation value is provided"))),

        // Empty operation type column
        Arguments.of(
            "empty_op_type_column",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(Map.of("id", 1L, "name", "John Updated", "value", 101.0)),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    "", // Empty operation column
                    "D")),
            initialData, // No change expected since it errors
            null,
            Collections.singletonList(
                Pair.of(ValidationException.class, "Operation type column cannot be empty"))),

        // Missing delete operation value
        Arguments.of(
            "missing_delete_value",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(
                        Map.of("id", 1L, "name", "John Updated", "value", 101.0, "op_type", "U")),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    "op_type",
                    null // Null delete value
                    )),
            initialData, // No change expected since it errors
            null,
            Collections.singletonList(
                Pair.of(ValidationException.class, "Delete operation value is mandatory"))),

        // Empty delete operation value
        Arguments.of(
            "empty_delete_value",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(
                        Map.of("id", 1L, "name", "John Updated", "value", 101.0, "op_type", "U")),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    "op_type",
                    "" // Empty delete value
                    )),
            initialData, // No change expected since it errors
            null,
            Collections.singletonList(
                Pair.of(ValidationException.class, "Delete operation value is mandatory"))),

        // Operation type column with spaces - successful case with update and delete
        Arguments.of(
            "op_col_with_spaces",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(
                        Map.of(
                            "id",
                            1L,
                            "name",
                            "John Updated",
                            "value",
                            101.0,
                            "op type with spaces",
                            "U"),
                        Map.of(
                            "id", 2L, "name", "Jane", "value", 200.0, "op type with spaces", "D")),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    "op type with spaces",
                    "D")),
            Arrays.asList(Map.of("id", 1L, "name", "John Updated", "value", 101.0)),
            null,
            null // No validation errors
            ),

        // Operation type column with double quotes
        Arguments.of(
            "op_col_with_double_quotes",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(
                        new HashMap<String, Object>() {
                          {
                            put("id", 1L);
                            put("name", "John Updated");
                            put("value", 101.0);
                            put("op\"quote", "U");
                          }
                        },
                        new HashMap<String, Object>() {
                          {
                            put("id", 2L);
                            put("name", "Jane");
                            put("value", 200.0);
                            put("op\"quote", "D");
                          }
                        }),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    "op\"quote",
                    "D")),
            Arrays.asList(Map.of("id", 1L, "name", "John Updated", "value", 101.0)),
            null,
            null // No validation errors
            ),

        // Operation type column with single quotes
        Arguments.of(
            "op_col_with_single_quotes",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(
                        new HashMap<String, Object>() {
                          {
                            put("id", 1L);
                            put("name", "John Updated");
                            put("value", 101.0);
                            put("op'quote", "U");
                          }
                        },
                        new HashMap<String, Object>() {
                          {
                            put("id", 2L);
                            put("name", null);
                            put("value", null);
                            put("op'quote", "D");
                          }
                        }),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    "op'quote",
                    "D")),
            Arrays.asList(Map.of("id", 1L, "name", "John Updated", "value", 101.0)),
            null,
            null // No validation errors
            ),

        // Operation type column with special characters
        Arguments.of(
            "op_col_with_special_chars",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(
                        new HashMap<String, Object>() {
                          {
                            put("id", 1L);
                            put("name", "John Updated");
                            put("value", 101.0);
                            put("op!@#$%", "U");
                          }
                        },
                        new HashMap<String, Object>() {
                          {
                            put("id", 2L);
                            put("name", null);
                            put("value", null);
                            put("op!@#$%", "D");
                          }
                        }),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    "op!@#$%",
                    "D")),
            Arrays.asList(Map.of("id", 1L, "name", "John Updated", "value", 101.0)),
            null,
            null // No validation errors
            ),

        // Operation type column with control characters
        Arguments.of(
            "op_col_with_control_chars",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(
                        new HashMap<String, Object>() {
                          {
                            put("id", 1L);
                            put("name", "John Updated");
                            put("value", 101.0);
                            put("op\t\n\r", "U");
                          }
                        },
                        new HashMap<String, Object>() {
                          {
                            put("id", 2L);
                            put("name", null);
                            put("value", null);
                            put("op\t\n\r", "D");
                          }
                        }),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    "op\t\n\r",
                    "D")),
            Arrays.asList(Map.of("id", 1L, "name", "John Updated", "value", 101.0)),
            null,
            null // No validation errors
            ),

        // Delete value with special characters
        Arguments.of(
            "delete_value_with_special_chars",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(
                        Map.of(
                            "id",
                            1L,
                            "name",
                            "John Updated",
                            "value",
                            101.0,
                            "operation_type",
                            "U"),
                        Map.of(
                            "id",
                            2L,
                            "name",
                            "Jane",
                            "value",
                            200.0,
                            "operation_type",
                            "D!@#$%^&*()")),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    "operation_type",
                    "D!@#$%^&*()")),
            Arrays.asList(Map.of("id", 1L, "name", "John Updated", "value", 101.0)),
            null,
            null // No validation errors
            ),

        // Delete value with double quotes
        Arguments.of(
            "delete_value_with_double_quotes",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(
                        Map.of(
                            "id",
                            1L,
                            "name",
                            "John Updated",
                            "value",
                            101.0,
                            "operation_type",
                            "U"),
                        Map.of(
                            "id",
                            2L,
                            "name",
                            "Jane",
                            "value",
                            200.0,
                            "operation_type",
                            "D\"delete\"")),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    "operation_type",
                    "D\"delete\"")),
            Arrays.asList(Map.of("id", 1L, "name", "John Updated", "value", 101.0)),
            null,
            null // No validation errors
            ),

        // Delete value with single quotes
        Arguments.of(
            "delete_value_with_single_quotes",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(
                        Map.of(
                            "id",
                            1L,
                            "name",
                            "John Updated",
                            "value",
                            101.0,
                            "operation_type",
                            "U"),
                        Map.of(
                            "id",
                            2L,
                            "name",
                            "Jane",
                            "value",
                            200.0,
                            "operation_type",
                            "D'delete'")),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    "operation_type",
                    "D'delete'")),
            Arrays.asList(Map.of("id", 1L, "name", "John Updated", "value", 101.0)),
            null,
            null // No validation errors
            ),

        // Delete value with spaces
        Arguments.of(
            "delete_value_with_spaces",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(
                        Map.of(
                            "id",
                            1L,
                            "name",
                            "John Updated",
                            "value",
                            101.0,
                            "operation_type",
                            "U"),
                        Map.of(
                            "id",
                            2L,
                            "name",
                            "Jane",
                            "value",
                            200.0,
                            "operation_type",
                            " D delete ")),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    "operation_type",
                    " D delete ")),
            Arrays.asList(Map.of("id", 1L, "name", "John Updated", "value", 101.0)),
            null,
            null // No validation errors
            ),

        // Delete value with control characters
        Arguments.of(
            "delete_value_with_control_chars",
            schema,
            partitionSpec,
            initialData,
            Collections.singletonList(
                new SCD1MergeInput(
                    Arrays.asList(
                        Map.of(
                            "id",
                            1L,
                            "name",
                            "John Updated",
                            "value",
                            101.0,
                            "operation_type",
                            "U"),
                        Map.of(
                            "id", 2L, "name", "Jane", "value", 200.0, "operation_type", "D\t\n\r")),
                    "id IS NOT NULL",
                    Arrays.asList("id"),
                    false,
                    null,
                    null,
                    null,
                    "operation_type",
                    "D\t\n\r")),
            Arrays.asList(Map.of("id", 1L, "name", "John Updated", "value", 101.0)),
            null,
            null // No validation errors
            ));
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
            "scd1_partition_strategy_test_" + UUID.randomUUID().toString().replace('-', '_'));
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

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

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
                  3L,
                  "date",
                  LocalDate.parse("2025-01-03"),
                  "timestamp",
                  LocalDateTime.parse("2025-02-02T06:06:06"),
                  "name",
                  "Bob"),
              Map.of(
                  "id",
                  2L,
                  "date",
                  LocalDate.parse("2025-01-02"),
                  "timestamp",
                  LocalDateTime.parse("2025-01-01T02:45:33"),
                  "name",
                  "Jane"));
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

    if (isSnapshotMode) {
      String snapshotSql = TestUtil.createSelectSql(inputData, schema);
      swiftLakeEngine
          .applySnapshotAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(snapshotSql)
          .keyColumns(Arrays.asList("id"))
          .processSourceTables(false)
          .execute();
    } else {
      String changeSql = TestUtil.createChangeSql(inputData, schema);
      swiftLakeEngine
          .applyChangesAsSCD1(tableName)
          .tableFilterSql("id IS NOT NULL")
          .sourceSql(changeSql)
          .keyColumns(Arrays.asList("id"))
          .operationTypeColumn("operation_type", "D")
          .processSourceTables(false)
          .execute();
    }

    // Verify the results
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    List<Map<String, Object>> expectedData =
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
        Types.NestedField.required(4, "name", Types.StringType.get()));
  }

  private static Stream<Arguments> providePartitionStrategies() {
    Schema schema = getSchemaForPartitionStrategiesTest();
    List<Arguments> baseArguments =
        Arrays.asList(
            Arguments.of(PartitionSpec.unpartitioned(), Arrays.asList(Pair.of("", 3L))),
            Arguments.of(
                PartitionSpec.builderFor(schema).identity("id").build(),
                Arrays.asList(Pair.of("id=1", 1L), Pair.of("id=2", 1L), Pair.of("id=3", 1L))),
            Arguments.of(
                PartitionSpec.builderFor(schema).year("date").build(),
                Arrays.asList(Pair.of("date_year=2024", 1L), Pair.of("date_year=2025", 2L))),
            Arguments.of(
                PartitionSpec.builderFor(schema).identity("id").year("date").build(),
                Arrays.asList(
                    Pair.of("id=1/date_year=2024", 1L),
                    Pair.of("id=2/date_year=2025", 1L),
                    Pair.of("id=3/date_year=2025", 1L))),
            Arguments.of(
                PartitionSpec.builderFor(schema).month("date").bucket("id", 5).build(),
                Arrays.asList(
                    Pair.of("date_month=2024-02/id_bucket=1", 1L),
                    Pair.of("date_month=2025-01/id_bucket=0", 1L),
                    Pair.of("date_month=2025-01/id_bucket=2", 1L))),
            Arguments.of(
                PartitionSpec.builderFor(schema).month("date").truncate("name", 3).build(),
                Arrays.asList(
                    Pair.of("date_month=2024-02/name_trunc=Joh", 1L),
                    Pair.of("date_month=2025-01/name_trunc=Jan", 1L),
                    Pair.of("date_month=2025-01/name_trunc=Bob", 1L))),
            Arguments.of(
                PartitionSpec.builderFor(schema).hour("timestamp").identity("date").build(),
                Arrays.asList(
                    Pair.of("timestamp_hour=2025-01-01-02/date=2025-01-02", 1L),
                    Pair.of("timestamp_hour=2024-04-04-12/date=2024-02-02", 1L),
                    Pair.of("timestamp_hour=2025-02-02-06/date=2025-01-03", 1L))),
            Arguments.of(
                PartitionSpec.builderFor(schema).day("timestamp").truncate("id", 2).build(),
                Arrays.asList(
                    Pair.of("timestamp_day=2024-04-04/id_trunc=0", 1L),
                    Pair.of("timestamp_day=2025-01-01/id_trunc=2", 1L),
                    Pair.of("timestamp_day=2025-02-02/id_trunc=2", 1L))),
            Arguments.of(
                PartitionSpec.builderFor(schema).month("timestamp").month("date").build(),
                Arrays.asList(
                    Pair.of("timestamp_month=2025-01/date_month=2025-01", 1L),
                    Pair.of("timestamp_month=2024-04/date_month=2024-02", 1L),
                    Pair.of("timestamp_month=2025-02/date_month=2025-01", 1L))));

    List<Arguments> allArguments = new ArrayList<>();
    for (Arguments baseArgument : baseArguments) {
      Object[] baseArgs = baseArgument.get();

      // Create arguments for changes mode (isSnapshotMode = false)
      Object[] changesArgs = Arrays.copyOf(baseArgs, baseArgs.length + 1);
      changesArgs[baseArgs.length] = false;
      allArguments.add(Arguments.of(changesArgs));

      // Create arguments for snapshot mode (isSnapshotMode = true)
      Object[] snapshotArgs = Arrays.copyOf(baseArgs, baseArgs.length + 1);
      snapshotArgs[baseArgs.length] = true;
      allArguments.add(Arguments.of(snapshotArgs));
    }

    return allArguments.stream();
  }

  @Test
  @Execution(ExecutionMode.CONCURRENT)
  void testSnapshotModeConcurrentOperations() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("id").build();

    TableIdentifier tableId =
        TableIdentifier.of("test_db", "scd1_snapshot_concurrent_operations_test");
    swiftLakeEngine
        .getCatalog()
        .createTable(tableId, schema, partitionSpec, Map.of("commit.retry.num-retries", "10"));
    String tableName = tableId.toString();

    List<Map<String, Object>> expectedData = new ArrayList<>();

    // Perform multiple concurrent snapshot operations
    for (int j = 0; j < 3; j++) {
      int index = j;
      IntStream.range(0, 10)
          .parallel()
          .forEach(
              i -> {
                List<Map<String, Object>> data =
                    Arrays.asList(Map.of("id", (long) i, "name", "name_" + i + index));
                String sourceSql = TestUtil.createSelectSql(data, schema);
                swiftLakeEngine
                    .applySnapshotAsSCD1(tableName)
                    .tableFilterSql("id = " + i)
                    .sourceSql(sourceSql)
                    .keyColumns(Arrays.asList("id"))
                    .processSourceTables(false)
                    .execute();
              });
    }

    expectedData.addAll(
        IntStream.range(0, 10)
            .mapToObj(
                i ->
                    new HashMap<String, Object>() {
                      {
                        put("id", (long) i);
                        put("name", "name_" + i + 2); // Last iteration's value should win
                      }
                    })
            .collect(Collectors.toList()));

    // Verify the results
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  @Execution(ExecutionMode.CONCURRENT)
  void testConcurrentOperations() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("id").build();

    TableIdentifier tableId = TableIdentifier.of("test_db", "scd1_concurrent_operations_test");
    swiftLakeEngine
        .getCatalog()
        .createTable(tableId, schema, partitionSpec, Map.of("commit.retry.num-retries", "10"));
    String tableName = tableId.toString();

    List<Map<String, Object>> expectedData = new ArrayList<>();

    // Perform multiple concurrent merge operations
    for (int j = 0; j < 3; j++) {
      int index = j;
      IntStream.range(0, 10)
          .parallel()
          .forEach(
              i -> {
                List<Map<String, Object>> data =
                    Arrays.asList(
                        Map.of("id", (long) i, "name", "name_" + i + index, "operation_type", "I"));
                String changeSql = TestUtil.createChangeSql(data, schema);
                swiftLakeEngine
                    .applyChangesAsSCD1(tableName)
                    .tableFilterSql("id = " + i)
                    .sourceSql(changeSql)
                    .keyColumns(Arrays.asList("id"))
                    .operationTypeColumn("operation_type", "D")
                    .processSourceTables(false)
                    .execute();
              });
    }

    expectedData.addAll(
        IntStream.range(0, 10)
            .mapToObj(
                i ->
                    new HashMap<String, Object>() {
                      {
                        put("id", (long) i);
                        put("name", "name_" + i + 2);
                      }
                    })
            .collect(Collectors.toList()));

    // Verify the results
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testPartitionEvolutionWithSnapshotMode() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "year", Types.IntegerType.get()),
            Types.NestedField.required(4, "month", Types.IntegerType.get()),
            Types.NestedField.required(5, "value", Types.DoubleType.get()));

    // Start with partitioning by year
    PartitionSpec initialSpec = PartitionSpec.builderFor(schema).identity("year").build();
    TableIdentifier tableId =
        TableIdentifier.of("test_db", "scd1_snapshot_partition_evolution_data_movement_test");
    swiftLakeEngine.getCatalog().createTable(tableId, schema, initialSpec);
    String tableName = tableId.toString();

    // Initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "year", 2025, "month", 1, "value", 100.0),
            Map.of("id", 2L, "name", "Jane", "year", 2025, "month", 2, "value", 200.0));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Snapshot data with changes
    List<Map<String, Object>> snapshot1 =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe", "year", 2025, "month", 1, "value", 150.0),
            Map.of("id", 2L, "name", "Jane", "year", 2025, "month", 2, "value", 200.0),
            Map.of("id", 3L, "name", "Bob", "year", 2025, "month", 3, "value", 300.0));

    // Apply first snapshot
    String sourceSql1 = TestUtil.createSelectSql(snapshot1, schema);
    swiftLakeEngine
        .applySnapshotAsSCD1(tableName)
        .tableFilterSql("id IS NOT NULL")
        .sourceSql(sourceSql1)
        .keyColumns(Arrays.asList("id"))
        .processSourceTables(false)
        .execute();

    // Evolve partition spec to include month
    swiftLakeEngine.getTable(tableName).updateSpec().addField("month").commit();

    // Second snapshot data with more changes
    List<Map<String, Object>> snapshot2 =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe", "year", 2025, "month", 1, "value", 150.0),
            Map.of(
                "id",
                2L,
                "name",
                "Jane Doe", // changed
                "year",
                2025,
                "month",
                2,
                "value",
                250.0), // changed
            Map.of("id", 3L, "name", "Bob", "year", 2025, "month", 3, "value", 300.0),
            Map.of("id", 4L, "name", "Alice", "year", 2025, "month", 4, "value", 400.0)); // new

    // Apply second snapshot with new partition spec
    String sourceSql2 = TestUtil.createSelectSql(snapshot2, schema);
    swiftLakeEngine
        .applySnapshotAsSCD1(tableName)
        .tableFilterSql("id IS NOT NULL")
        .sourceSql(sourceSql2)
        .keyColumns(Arrays.asList("id"))
        .processSourceTables(false)
        .execute();

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe", "year", 2025, "month", 1, "value", 150.0),
            Map.of("id", 2L, "name", "Jane Doe", "year", 2025, "month", 2, "value", 250.0),
            Map.of("id", 3L, "name", "Bob", "year", 2025, "month", 3, "value", 300.0),
            Map.of("id", 4L, "name", "Alice", "year", 2025, "month", 4, "value", 400.0));

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

  @Test
  void testPartitionEvolutionWithDataMovement() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "year", Types.IntegerType.get()),
            Types.NestedField.required(4, "month", Types.IntegerType.get()),
            Types.NestedField.required(5, "value", Types.DoubleType.get()));

    // Start with partitioning by year
    PartitionSpec initialSpec = PartitionSpec.builderFor(schema).identity("year").build();
    TableIdentifier tableId =
        TableIdentifier.of("test_db", "scd1_partition_evolution_data_movement_test");
    swiftLakeEngine.getCatalog().createTable(tableId, schema, initialSpec);
    String tableName = tableId.toString();

    // Initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "year", 2025, "month", 1, "value", 100.0),
            Map.of("id", 2L, "name", "Jane", "year", 2025, "month", 2, "value", 200.0));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Perform first SCD1 merge
    List<Map<String, Object>> inputData1 =
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
    String changeSql1 = TestUtil.createChangeSql(inputData1, schema);
    swiftLakeEngine
        .applyChangesAsSCD1(tableName)
        .tableFilterSql("id IS NOT NULL")
        .sourceSql(changeSql1)
        .keyColumns(Arrays.asList("id"))
        .operationTypeColumn("operation_type", "D")
        .processSourceTables(false)
        .execute();

    // Evolve partition spec to include month
    swiftLakeEngine.getTable(tableName).updateSpec().addField("month").commit();

    // Perform second SCD1 merge with new partition spec
    List<Map<String, Object>> inputData2 =
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
    String changeSql2 = TestUtil.createChangeSql(inputData2, schema);
    swiftLakeEngine
        .applyChangesAsSCD1(tableName)
        .tableFilterSql("id IS NOT NULL")
        .sourceSql(changeSql2)
        .keyColumns(Arrays.asList("id"))
        .operationTypeColumn("operation_type", "D")
        .processSourceTables(false)
        .execute();

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe", "year", 2025, "month", 1, "value", 150.0),
            Map.of("id", 2L, "name", "Jane Doe", "year", 2025, "month", 2, "value", 250.0),
            Map.of("id", 3L, "name", "Bob", "year", 2025, "month", 3, "value", 300.0),
            Map.of("id", 4L, "name", "Alice", "year", 2025, "month", 4, "value", 400.0));

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

  private static Arguments valueColumnsTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "value", Types.DoubleType.get()),
            Types.NestedField.optional(4, "salary", Types.DoubleType.get()),
            Types.NestedField.optional(5, "status", Types.StringType.get()),
            Types.NestedField.optional(6, "created_at", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    // Initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of(
                "id",
                1,
                "name",
                "John",
                "value",
                100.0,
                "salary",
                50.0,
                "status",
                "Active",
                "created_at",
                LocalDateTime.parse("2023-01-01T12:00:00")));

    // Input data - only name and value should be considered for value comparison
    List<Map<String, Object>> inputData =
        Arrays.asList(
            Map.of(
                "id",
                1,
                "name",
                "John",
                "value",
                100.0,
                "salary",
                60.0, // changed
                "status",
                "Updated", // changed
                "created_at",
                LocalDateTime.parse("2023-02-01T12:00:00"))); // changed

    // Expected result - only specified value columns should be compared
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1,
                "name",
                "John",
                "value",
                100.0,
                "salary",
                50.0, // unchanged because not in valueColumns
                "status",
                "Active", // unchanged because not in valueColumns
                "created_at",
                LocalDateTime.parse("2023-01-01T12:00:00"))); // unchanged

    List<String> keyColumns = Arrays.asList("id");
    List<String> valueColumns = Arrays.asList("name", "value");
    String tableFilter = "id = 1";
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, true, valueColumns, null, null);

    return Arguments.of(
        "value_columns",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments valueColumnsWithMaxDeltaTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "value", Types.DoubleType.get()),
            Types.NestedField.optional(4, "salary", Types.DoubleType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    // Initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1, "name", "John", "value", 100.0, "salary", 50.0));

    // Input sequence
    List<SCD1MergeInput> mergeInputs = new ArrayList<>();

    // First update - small delta within tolerance
    List<Map<String, Object>> inputData1 =
        Arrays.asList(Map.of("id", 1, "name", "John", "value", 100.01, "salary", 50.3));

    Map<String, ValueColumnMetadata<?>> valueMetadata1 = new HashMap<>();
    valueMetadata1.put("value", new ValueColumnMetadata<>(0.1, null));
    valueMetadata1.put("salary", new ValueColumnMetadata<>(0.5, null));

    mergeInputs.add(
        new SCD1MergeInput(
            inputData1,
            "id = 1",
            Arrays.asList("id"),
            true,
            Arrays.asList("name", "value", "salary"),
            valueMetadata1,
            null));

    // Second update - one change exceeds tolerance
    List<Map<String, Object>> inputData2 =
        Arrays.asList(
            Map.of(
                "id", 1, "name", "John", "value", 100.02, "salary",
                55.0)); // exceeds the 0.5 delta threshold

    mergeInputs.add(
        new SCD1MergeInput(
            inputData2,
            "id = 1",
            Arrays.asList("id"),
            true,
            Arrays.asList("name", "value", "salary"),
            valueMetadata1,
            null));

    // Expected result - after the second update, salary should have changed
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id", 1, "name", "John", "value", 100.02, // small change
                "salary", 55.0)); // exceeded threshold

    return Arguments.of(
        "value_columns_max_delta",
        schema,
        partitionSpec,
        initialData,
        mergeInputs,
        expectedData,
        null,
        null);
  }

  private static Arguments valueColumnsWithNullReplacementTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "value", Types.DoubleType.get()),
            Types.NestedField.optional(4, "salary", Types.DoubleType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    // Initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1, "name", "John", "value", 100.0, "salary", 50.0));

    // Input data with nulls treated as specific values
    Map<String, Object> nullData = new HashMap<>();
    nullData.put("id", 1);
    nullData.put("name", "John");
    nullData.put("value", null);
    nullData.put("salary", null);

    List<Map<String, Object>> inputData = Arrays.asList(nullData);

    Map<String, ValueColumnMetadata<?>> valueMetadata1 = new HashMap<>();
    valueMetadata1.put("value", new ValueColumnMetadata<>(null, 100.0));
    valueMetadata1.put("salary", new ValueColumnMetadata<>(null, 50.0));

    List<SCD1MergeInput> mergeInputs = new ArrayList<>();
    List<String> keyColumns = Arrays.asList("id");
    List<String> valueColumns = Arrays.asList("name", "value", "salary");
    String tableFilter = "id = 1";
    mergeInputs.add(
        new SCD1MergeInput(
            inputData, tableFilter, keyColumns, true, valueColumns, valueMetadata1, null));

    // Second update - nulls with different replacement values
    Map<String, Object> nullData2 = new HashMap<>();
    nullData2.put("id", 1);
    nullData2.put("name", "John");
    nullData2.put("value", null);
    nullData2.put("salary", null);

    List<Map<String, Object>> inputData2 = Arrays.asList(nullData2);

    Map<String, ValueColumnMetadata<?>> valueMetadata2 = new HashMap<>();
    valueMetadata2.put("value", new ValueColumnMetadata<>(null, 0.0));
    valueMetadata2.put("salary", new ValueColumnMetadata<>(null, 1.0));

    mergeInputs.add(
        new SCD1MergeInput(
            inputData2, tableFilter, keyColumns, true, valueColumns, valueMetadata2, null));

    // Third update - mixed nulls and non-nulls
    Map<String, Object> mixedData = new HashMap<>();
    mixedData.put("id", 1);
    mixedData.put("name", "John");
    mixedData.put("value", 200.0);
    mixedData.put("salary", null);

    List<Map<String, Object>> inputData3 = Arrays.asList(mixedData);

    Map<String, ValueColumnMetadata<?>> valueMetadata3 = new HashMap<>();
    valueMetadata3.put("value", new ValueColumnMetadata<>(null, 0.0));
    valueMetadata3.put("salary", new ValueColumnMetadata<>(null, 0.0));

    mergeInputs.add(
        new SCD1MergeInput(
            inputData3, tableFilter, keyColumns, true, valueColumns, valueMetadata3, null));

    // Expected final state
    Map<String, Object> expected = new HashMap<>();
    expected.put("id", 1);
    expected.put("name", "John");
    expected.put("value", 200.0);
    expected.put("salary", null);

    List<Map<String, Object>> expectedData = Arrays.asList(expected);
    return Arguments.of(
        "value_columns_null_replacement",
        schema,
        partitionSpec,
        initialData,
        mergeInputs,
        expectedData,
        null,
        null);
  }

  private static Arguments skipEmptySourceTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "value", Types.DoubleType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    // Initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1, "name", "John", "value", 100.0),
            Map.of("id", 2, "name", "Jane", "value", 200.0));

    // Empty source data
    List<Map<String, Object>> emptyInputData = new ArrayList<>();

    // Test multiple scenarios with empty source
    List<SCD1MergeInput> mergeInputs = new ArrayList<>();

    // First test: skipEmptySource=true - should not delete records
    mergeInputs.add(
        new SCD1MergeInput(
            emptyInputData,
            "id = 1",
            Arrays.asList("id"),
            true,
            null,
            null,
            true)); // skip empty source

    // Second test: skipEmptySource=false - should delete records matching the filter
    mergeInputs.add(
        new SCD1MergeInput(
            emptyInputData,
            "id = 2",
            Arrays.asList("id"),
            true,
            null,
            null,
            false)); // don't skip empty source

    // After the first operation, nothing should change
    // After the second operation, id=2 should be deleted
    List<Map<String, Object>> expectedData =
        Arrays.asList(Map.of("id", 1, "name", "John", "value", 100.0));

    return Arguments.of(
        "skip_empty_source",
        schema,
        partitionSpec,
        initialData,
        mergeInputs,
        expectedData,
        null,
        null);
  }

  private static Arguments replaceAllTestCase(boolean isSnapshotMode) {
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

    List<List<Map<String, Object>>> inputDataList;

    if (isSnapshotMode) {
      inputDataList =
          Arrays.asList(
              // First snapshot removes Bob, updates John
              Arrays.asList(Map.of("id", 1L, "name", "John Doe"), Map.of("id", 2L, "name", "Jane")),
              // Second snapshot replaces all with completely new data
              Arrays.asList(
                  Map.of("id", 4L, "name", "Alice"),
                  Map.of("id", 5L, "name", "Charlie"),
                  Map.of("id", 6L, "name", "David")));
    } else {
      // Changes mode equivalent operations
      inputDataList =
          Arrays.asList(
              // First set of changes: update John, delete Bob
              Arrays.asList(
                  Map.of("id", 1L, "name", "John Doe", "operation_type", "U"),
                  Map.of("id", 3L, "name", "Bob", "operation_type", "D")),
              // Second set of changes: delete existing records, insert new ones
              Arrays.asList(
                  Map.of("id", 1L, "name", "John Doe", "operation_type", "D"),
                  Map.of("id", 2L, "name", "Jane", "operation_type", "D"),
                  Map.of("id", 4L, "name", "Alice", "operation_type", "I"),
                  Map.of("id", 5L, "name", "Charlie", "operation_type", "I"),
                  Map.of("id", 6L, "name", "David", "operation_type", "I")));
    }

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 4L, "name", "Alice"),
            Map.of("id", 5L, "name", "Charlie"),
            Map.of("id", 6L, "name", "David"));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    List<SCD1MergeInput> mergeInputs =
        inputDataList.stream()
            .map(
                inputData -> new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode))
            .collect(Collectors.toList());

    return Arguments.of(
        "replace_all" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        mergeInputs,
        expectedData,
        null,
        null);
  }

  private static Arguments partialMergeWithFiltersTestCase(boolean isSnapshotMode) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()));
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("region").build();

    // Initial data with multiple regions
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "region", "EMEA", "value", 100.0),
            Map.of("id", 2L, "region", "EMEA", "value", 200.0),
            Map.of("id", 3L, "region", "APAC", "value", 300.0),
            Map.of("id", 4L, "region", "APAC", "value", 400.0),
            Map.of("id", 5L, "region", "AMER", "value", 500.0));

    List<Map<String, Object>> inputData;

    if (isSnapshotMode) {
      // Snapshot data for EMEA region only
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "region", "EMEA", "value", 110.0),
              Map.of("id", 2L, "region", "EMEA", "value", 220.0),
              Map.of("id", 6L, "region", "EMEA", "value", 600.0));
    } else {
      // Changes mode equivalent
      inputData =
          Arrays.asList(
              Map.of("id", 1L, "region", "EMEA", "value", 110.0, "operation_type", "U"),
              Map.of("id", 2L, "region", "EMEA", "value", 220.0, "operation_type", "U"),
              Map.of("id", 6L, "region", "EMEA", "value", 600.0, "operation_type", "I"));
    }

    // Expected data - EMEA records updated/added/deleted as specified in the operation
    // but APAC and AMER records are left untouched
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "region", "EMEA", "value", 110.0),
            Map.of("id", 2L, "region", "EMEA", "value", 220.0),
            Map.of("id", 6L, "region", "EMEA", "value", 600.0),
            Map.of("id", 3L, "region", "APAC", "value", 300.0),
            Map.of("id", 4L, "region", "APAC", "value", 400.0),
            Map.of("id", 5L, "region", "AMER", "value", 500.0));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "region = 'EMEA'"; // Filter only affects EMEA records
    SCD1MergeInput mergeInput =
        new SCD1MergeInput(inputData, tableFilter, keyColumns, isSnapshotMode);

    return Arguments.of(
        "partial_merge_with_filters" + (isSnapshotMode ? "_snapshot_mode" : "_changes_mode"),
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  @Test
  void testFilesSkippedWithNoChanges() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()),
            Types.NestedField.required(4, "salary", Types.DoubleType.get()),
            Types.NestedField.required(5, "status", Types.StringType.get()),
            Types.NestedField.optional(6, "created_at", Types.TimestampType.withoutZone()));

    // Create table
    TableIdentifier tableId = TableIdentifier.of("test_db", "files_skipped_test");
    Table testTable = swiftLakeEngine.getCatalog().createTable(tableId, schema);
    String tableName = tableId.toString();

    // Insert data in multiple batches to create different data files
    String initialInsertSql1 =
        "SELECT * FROM (VALUES"
            + "(1, 'A1', 100.0, 50.0, 'A', TIMESTAMP '2023-01-01 12:00:00'),"
            + "(2, 'A2', 200.0, 60.0, 'A', TIMESTAMP '2023-01-01 12:00:00')"
            + ") AS t(id, name, value, salary, status, created_at)";
    swiftLakeEngine
        .insertInto(tableName)
        .sql(initialInsertSql1)
        .processSourceTables(false)
        .execute();

    String initialInsertSql2 =
        "SELECT * FROM (VALUES"
            + "(3, 'B1', 300.0, 70.0, 'B', TIMESTAMP '2023-01-01 12:00:00'),"
            + "(4, 'B2', 400.0, 80.0, 'B', TIMESTAMP '2023-01-01 12:00:00')"
            + ") AS t(id, name, value, salary, status, created_at)";
    swiftLakeEngine
        .insertInto(tableName)
        .sql(initialInsertSql2)
        .processSourceTables(false)
        .execute();

    String initialInsertSql3 =
        "SELECT * FROM (VALUES"
            + "(5, 'C1', 500.0, 90.0, 'C', TIMESTAMP '2023-01-01 12:00:00')"
            + ") AS t(id, name, value, salary, status, created_at)";
    swiftLakeEngine
        .insertInto(tableName)
        .sql(initialInsertSql3)
        .processSourceTables(false)
        .execute();

    // Apply snapshot merge that only changes some records
    String updateSql =
        "SELECT * FROM (VALUES"
            + "(1, 'A1 Updated', 150.0, 50.0, 'A', TIMESTAMP '2023-01-01 12:00:00'),"
            + "(2, 'A2', 200.0, 65.0, 'A', TIMESTAMP '2023-01-01 12:00:00'),"
            + "(3, 'B1', 300.0, 70.0, 'B', TIMESTAMP '2023-01-01 12:00:00'),"
            + "(4, 'B2', 400.0, 80.0, 'B', TIMESTAMP '2023-01-01 12:00:00')"
            + ") AS t(id, name, value, salary, status, created_at)";

    CommitMetrics commitMetrics =
        swiftLakeEngine
            .applySnapshotAsSCD1(testTable)
            .tableFilterSql("status IN ('A', 'B')") // Filter includes categories A and B
            .sourceSql(updateSql)
            .keyColumns(Arrays.asList("id"))
            .valueColumns(Arrays.asList("name", "value", "salary")) // Only these are value columns
            .execute();

    // Verify metrics
    assertThat(commitMetrics.getRemovedFilesCount()).isEqualTo(1);
    assertThat(commitMetrics.getAddedFilesCount()).isEqualTo(1);

    // Verify results
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id",
                1L,
                "name",
                "A1 Updated",
                "value",
                150.0,
                "salary",
                50.0,
                "status",
                "A",
                "created_at",
                TestUtil.parseTimestamp("2023-01-01 12:00:00")),
            Map.of(
                "id",
                2L,
                "name",
                "A2",
                "value",
                200.0,
                "salary",
                65.0,
                "status",
                "A",
                "created_at",
                TestUtil.parseTimestamp("2023-01-01 12:00:00")),
            Map.of(
                "id",
                3L,
                "name",
                "B1",
                "value",
                300.0,
                "salary",
                70.0,
                "status",
                "B",
                "created_at",
                TestUtil.parseTimestamp("2023-01-01 12:00:00")),
            Map.of(
                "id",
                4L,
                "name",
                "B2",
                "value",
                400.0,
                "salary",
                80.0,
                "status",
                "B",
                "created_at",
                TestUtil.parseTimestamp("2023-01-01 12:00:00")),
            Map.of(
                "id",
                5L,
                "name",
                "C1",
                "value",
                500.0,
                "salary",
                90.0,
                "status",
                "C",
                "created_at",
                TestUtil.parseTimestamp("2023-01-01 12:00:00")));

    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  public static class SCD1MergeInput {
    List<Map<String, Object>> inputData;
    String tableFilter;
    List<String> keyColumns;
    boolean isSnapshot;
    List<String> valueColumns;
    Map<String, ValueColumnMetadata<?>> valueColumnMetadata;
    Boolean skipEmptySource;
    String operationTypeColumn;
    String deleteOperationValue;

    public SCD1MergeInput(
        List<Map<String, Object>> inputData, String tableFilter, List<String> keyColumns) {
      this(inputData, tableFilter, keyColumns, false, null, null, null);
    }

    public SCD1MergeInput(
        List<Map<String, Object>> inputData,
        String tableFilter,
        List<String> keyColumns,
        boolean isSnapshot) {
      this(inputData, tableFilter, keyColumns, isSnapshot, null, null, null);
    }

    public SCD1MergeInput(
        List<Map<String, Object>> inputData,
        String tableFilter,
        List<String> keyColumns,
        boolean isSnapshot,
        List<String> valueColumns,
        Map<String, ValueColumnMetadata<?>> valueColumnMetadata,
        Boolean skipEmptySource) {
      this(
          inputData,
          tableFilter,
          keyColumns,
          isSnapshot,
          valueColumns,
          valueColumnMetadata,
          skipEmptySource,
          isSnapshot ? null : "operation_type",
          isSnapshot ? null : "D");
    }

    public SCD1MergeInput(
        List<Map<String, Object>> inputData,
        String tableFilter,
        List<String> keyColumns,
        boolean isSnapshot,
        List<String> valueColumns,
        Map<String, ValueColumnMetadata<?>> valueColumnMetadata,
        Boolean skipEmptySource,
        String operationTypeColumn,
        String deleteOperationValue) {
      this.inputData = inputData;
      this.tableFilter = tableFilter;
      this.keyColumns = keyColumns;
      this.isSnapshot = isSnapshot;
      this.valueColumns = valueColumns;
      this.valueColumnMetadata = valueColumnMetadata;
      this.skipEmptySource = skipEmptySource;
      this.operationTypeColumn = operationTypeColumn;
      this.deleteOperationValue = deleteOperationValue;
    }
  }
}
