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
      String sourceSql = TestUtil.createChangeSql(mergeInput.inputData, schema);
      Runnable mergeFunc =
          () ->
              swiftLakeEngine
                  .applyChangesAsSCD1(tableName)
                  .tableFilterSql(mergeInput.tableFilter)
                  .sourceSql(sourceSql)
                  .keyColumns(mergeInput.keyColumns)
                  .operationTypeColumn("operation_type", "D")
                  .processSourceTables(false)
                  .execute();
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
    return Stream.of(
        simpleSchemaTestCase(),
        complexTypesTestCase(),
        noChangesTestCase(),
        allInsertsTestCase(),
        allDeletesTestCase(),
        emptySourceTestCase(),
        multipleOperationsTestCase(),
        nullValuesTestCase(),
        longHistoryChainTestCase(),
        extremeValuesTestCase(),
        unicodeAndSpecialCharactersTestCase(),
        timeZoneHandlingTestCase(),
        schemaEvolutionTestCase(),
        multiColumnKeyTestCase(),
        errorHandlingTestCase());
  }

  private static Arguments simpleSchemaTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<Map<String, Object>> inputData = null;

    inputData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe", "operation_type", "U"),
            Map.of("id", 3L, "name", "Bob", "operation_type", "I"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput = new SCD1MergeInput(inputData, tableFilter, keyColumns);
    return Arguments.of(
        "simple_schema",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments complexTypesTestCase() {
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

    List<Map<String, Object>> inputData = null;

    inputData =
        Arrays.asList(
            updateComplexRow(1L, "2025-01-01", "U"), createComplexRow(3L, "2025-01-01", "I"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            createExpectedComplexRow(1L, "2025-01-01", true),
            createExpectedComplexRow(2L, "2025-01-01", false),
            createExpectedComplexRow(3L, "2025-01-01", false));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput = new SCD1MergeInput(inputData, tableFilter, keyColumns);
    return Arguments.of(
        "complex_types",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments noChangesTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<Map<String, Object>> inputData = null;

    inputData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "operation_type", "U"),
            Map.of("id", 2L, "name", "Jane", "operation_type", "U"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput = new SCD1MergeInput(inputData, tableFilter, keyColumns);
    return Arguments.of(
        "no_changes",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments allInsertsTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData = new ArrayList<>();

    List<Map<String, Object>> inputData = null;
    inputData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "operation_type", "I"),
            Map.of("id", 2L, "name", "Jane", "operation_type", "I"),
            Map.of("id", 3L, "name", "Bob", "operation_type", "I"));
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput = new SCD1MergeInput(inputData, tableFilter, keyColumns);
    return Arguments.of(
        "all_inserts",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments allDeletesTestCase() {
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

    List<Map<String, Object>> inputData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "operation_type", "D"),
            Map.of("id", 2L, "name", "Jane", "operation_type", "D"),
            Map.of("id", 3L, "name", "Bob", "operation_type", "D"));

    List<Map<String, Object>> expectedData = Arrays.asList();

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput = new SCD1MergeInput(inputData, tableFilter, keyColumns);
    return Arguments.of(
        "all_deletes",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments emptySourceTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<Map<String, Object>> inputData = new ArrayList<>();

    List<Map<String, Object>> expectedData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput = new SCD1MergeInput(inputData, tableFilter, keyColumns);
    return Arguments.of(
        "empty_source",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments multipleOperationsTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<List<Map<String, Object>>> inputDataList = null;

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
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 3L, "name", "Robert"),
            Map.of("id", 4L, "name", "Alice"),
            Map.of("id", 5L, "name", "Jane Doe"));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    List<SCD1MergeInput> mergeInputs =
        inputDataList.stream()
            .map(inputData -> new SCD1MergeInput(inputData, tableFilter, keyColumns))
            .collect(Collectors.toList());

    return Arguments.of(
        "multiple_operations",
        schema,
        partitionSpec,
        initialData,
        mergeInputs,
        expectedData,
        null,
        null);
  }

  private static Arguments longHistoryChainTestCase() {
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
            .map(inputData -> new SCD1MergeInput(inputData, tableFilter, keyColumns))
            .collect(Collectors.toList());

    return Arguments.of(
        "long_history_chain",
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

  private static Arguments nullValuesTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "nullable_string", Types.StringType.get()),
            Types.NestedField.optional(3, "nullable_int", Types.IntegerType.get()));
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
    SCD1MergeInput mergeInput = new SCD1MergeInput(inputData, tableFilter, keyColumns);
    return Arguments.of(
        "null_values",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments extremeValuesTestCase() {
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
        Arrays.asList(Map.of("id", 1L, "long_string", longString, "big_number", bigNumber));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput = new SCD1MergeInput(inputData, tableFilter, keyColumns);
    return Arguments.of(
        "extreme_values",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments unicodeAndSpecialCharactersTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "special_string", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    String specialString = "!@#$%^&*()_+{ ¡™£¢∞§¶•ªº}[]|\\:;こんにちは\"'<你好，世界>,.?/~`";

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "special_string", "normal"));

    List<Map<String, Object>> inputData =
        Arrays.asList(Map.of("id", 1L, "special_string", specialString, "operation_type", "U"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(Map.of("id", 1L, "special_string", specialString));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput = new SCD1MergeInput(inputData, tableFilter, keyColumns);
    return Arguments.of(
        "special_characters",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments timeZoneHandlingTestCase() {
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

    List<Map<String, Object>> inputData =
        Arrays.asList(Map.of("id", 1L, "timestamp_with_tz", changedTime, "operation_type", "U"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id", 1L, "timestamp_with_tz", changedTime.withOffsetSameInstant(ZoneOffset.UTC)));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput = new SCD1MergeInput(inputData, tableFilter, keyColumns);
    return Arguments.of(
        "time_zone_handling",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments schemaEvolutionTestCase() {
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
            Map.of("id", 1L, "name", "John Doe", "email", "john.doe@example.com"),
            Map.of("id", 2L, "name", "Jane", "email", "jane@example.com"));

    List<String> keyColumns = Arrays.asList("id");
    String tableFilter = "id IS NOT NULL";
    SCD1MergeInput mergeInput = new SCD1MergeInput(inputData, tableFilter, keyColumns);
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

  private static Arguments multiColumnKeyTestCase() {
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

    List<Map<String, Object>> inputData = null;
    inputData =
        Arrays.asList(
            Map.of("id1", 1L, "id2", "A", "value", "Updated1", "operation_type", "U"),
            Map.of("id1", 2L, "id2", "B", "value", "New", "operation_type", "I"),
            Map.of("id1", 2L, "id2", "A", "value", "ToDelete", "operation_type", "D"));
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id1", 1L, "id2", "B", "value", "Initial2"),
            Map.of("id1", 1L, "id2", "A", "value", "Updated1"),
            Map.of("id1", 2L, "id2", "B", "value", "New"));

    List<String> keyColumns = Arrays.asList("id1", "id2");
    String tableFilter = "id1 IS NOT NULL";
    SCD1MergeInput mergeInput = new SCD1MergeInput(inputData, tableFilter, keyColumns);
    return Arguments.of(
        "multi_column_key",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(mergeInput),
        expectedData,
        null,
        null);
  }

  private static Arguments errorHandlingTestCase() {
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
    List<Map<String, Object>> inputData = null;
    inputData =
        Arrays.asList(
            Map.of("id", 1L, "value", 300.0, "operation_type", "U"),
            Map.of("id", 2L, "operation_type", "I"));
    mergeInputs.add(new SCD1MergeInput(inputData, tableFilter, keyColumns));
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
    mergeInputs.add(new SCD1MergeInput(inputData, tableFilter, keyColumns));
    errors.add(
        Pair.of(ValidationException.class, "Required fields cannot contain null values - value"));

    // Duplicate records
    inputData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe", "value", 300.0, "operation_type", "U"),
            Map.of("id", 1L, "name", "Bob", "value", 400.0, "operation_type", "D"));
    mergeInputs.add(new SCD1MergeInput(inputData, tableFilter, keyColumns));
    errors.add(
        Pair.of(
            ValidationException.class,
            "Merge operation matched a single row from target table data with multiple rows of the source data"));
    inputData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe", "value", 300.0, "operation_type", "U"),
            Map.of("id", 3L, "name", "Bob", "value", 400.0, "operation_type", "I"));
    mergeInputs.add(new SCD1MergeInput(inputData, tableFilter, keyColumns));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 2L, "name", "Jane", "value", 200.0),
            Map.of("id", 1L, "name", "John Doe", "value", 300.0),
            Map.of("id", 3L, "name", "Bob", "value", 400.0));

    return Arguments.of(
        "error_handling",
        schema,
        partitionSpec,
        initialData,
        mergeInputs,
        expectedData,
        null,
        errors);
  }

  @ParameterizedTest
  @MethodSource("providePartitionStrategies")
  void testDifferentPartitionStrategies(
      PartitionSpec partitionSpec, List<Pair<String, Long>> partitionLevelRecordCounts) {
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
    String changeSql = TestUtil.createChangeSql(inputData, schema);
    swiftLakeEngine
        .applyChangesAsSCD1(tableName)
        .tableFilterSql("id IS NOT NULL")
        .sourceSql(changeSql)
        .keyColumns(Arrays.asList("id"))
        .operationTypeColumn("operation_type", "D")
        .processSourceTables(false)
        .execute();

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
    return Stream.of(
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
    List<Map<String, Object>> inputData1 = null;
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
    List<Map<String, Object>> inputData2 = null;
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

  public static class SCD1MergeInput {
    List<Map<String, Object>> inputData;
    String tableFilter;
    List<String> keyColumns;

    public SCD1MergeInput(
        List<Map<String, Object>> inputData, String tableFilter, List<String> keyColumns) {
      this.inputData = inputData;
      this.tableFilter = tableFilter;
      this.keyColumns = keyColumns;
    }
  }
}
