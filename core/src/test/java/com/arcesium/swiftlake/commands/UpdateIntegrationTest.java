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
import com.arcesium.swiftlake.writer.TableBatchTransaction;
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
import org.apache.iceberg.SortOrder;
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
import org.junit.jupiter.params.provider.ValueSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class UpdateIntegrationTest {
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
  void testUpdate(
      String testName,
      Schema schema,
      PartitionSpec partitionSpec,
      List<Map<String, Object>> initialData,
      List<UpdateInput> updateInputList,
      List<Map<String, Object>> expectedData,
      List<Consumer<Table>> evolveSchema,
      List<Pair<Class<? extends Throwable>, String>> errors) {
    // Create table
    TableIdentifier tableId = TableIdentifier.of("test_db", "update_test_table_" + testName);
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    // Insert initial data
    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    for (int i = 0; i < updateInputList.size(); i++) {
      if (evolveSchema != null && evolveSchema.size() > i && evolveSchema.get(i) != null) {
        evolveSchema.get(i).accept(swiftLakeEngine.getTable(tableName));
        schema = swiftLakeEngine.getTable(tableName).schema();
      }
      UpdateInput updateInput = updateInputList.get(i);
      Runnable updateFunc =
          () ->
              swiftLakeEngine
                  .update(tableName)
                  .conditionSql(updateInput.conditionSql)
                  .updateSets(updateInput.inputData)
                  .execute();
      if (errors != null && errors.size() > i) {
        var throwableAssert = assertThatThrownBy(() -> updateFunc.run());
        var error = errors.get(i);
        if (error.getLeft() != null) {
          throwableAssert.isInstanceOf(error.getLeft());
        }
        if (error.getRight() != null) {
          throwableAssert.hasMessageContaining(error.getRight());
        }
      } else {
        updateFunc.run();
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
        allUpdatesTestCase(),
        nullValuesTestCase(),
        longHistoryChainTestCase(),
        extremeValuesTestCase(),
        unicodeAndSpecialCharactersTestCase(),
        timeZoneHandlingTestCase(),
        schemaEvolutionTestCase(),
        errorHandlingTestCase());
  }

  private static Arguments simpleSchemaTestCase() {
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
    UpdateInput updateInput = new UpdateInput(Map.of("name", "John Doe"), "id = 1");
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));
    return Arguments.of(
        "simple_schema",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(updateInput),
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
        Arrays.asList(
            createComplexRow(1L, "2025-01-01"),
            createComplexRow(2L, "2025-01-01"),
            createComplexRow(3L, "2025-01-01"));
    UpdateInput updateInput =
        new UpdateInput(Map.of("string_col", "updated_string_" + 1, "int_col", 101), "id = 1");
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            createExpectedComplexRow(1L, "2025-01-01", true),
            createExpectedComplexRow(2L, "2025-01-01", false),
            createExpectedComplexRow(3L, "2025-01-01", false));
    return Arguments.of(
        "complex_types",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(updateInput),
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
    List<UpdateInput> updateInputList = new ArrayList<>();
    updateInputList.add(new UpdateInput(Map.of("name", "John"), "id = 1"));
    updateInputList.add(new UpdateInput(Map.of("name", "Bob"), "id = 3"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));
    return Arguments.of(
        "no_changes",
        schema,
        partitionSpec,
        initialData,
        updateInputList,
        expectedData,
        null,
        null);
  }

  private static Arguments allUpdatesTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));
    List<Map<String, Object>> expectedData =
        Arrays.asList(Map.of("id", 1L, "name", "Bob"), Map.of("id", 2L, "name", "Bob"));
    return Arguments.of(
        "empty_source",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(new UpdateInput(Map.of("name", "Bob"), "id IS NOT NULL")),
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

    List<Map<String, Object>> inputDataList = new ArrayList<>();
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
        inputDataList.add(Map.of("value", "Update" + i));
      } else if (i < 20) {
        inputDataList.add(Map.of("value", "Update" + i));
      } else if (i < 50) {
        inputDataList.add(Map.of("value_2", "Update" + i));
      } else if (i < 55) {
        inputDataList.add(
            Map.of(
                "value_2",
                "Update" + i,
                "value_3",
                "Update" + i,
                "value_4",
                decimal28PrecisionValue,
                "value_5",
                epochDay.plusDays(i)));
      } else if (i < 60) {
        inputDataList.add(
            Map.of(
                "value_2",
                "Update" + i,
                "value_3",
                "Update" + i,
                "value_4",
                decimal38PrecisionValue,
                "value_5",
                epochDay.plusDays(i)));
      } else if (i < 70) {
        inputDataList.add(
            Map.of(
                "value_2",
                "Update" + i,
                "value_3",
                "Update" + i,
                "value_4",
                decimal38PrecisionValue,
                "value_5",
                epochDay.plusDays(i)));
      } else {
        inputDataList.add(
            Map.of(
                "value_2",
                "Update" + i,
                "value_3",
                "Update" + i,
                "value_5",
                decimal38PrecisionValue));

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
    List<UpdateInput> updateInputs =
        inputDataList.stream()
            .map(inputData -> new UpdateInput(inputData, "id=1"))
            .collect(Collectors.toList());

    return Arguments.of(
        "long_history_chain",
        schema,
        partitionSpec,
        initialData,
        updateInputs,
        expectedData,
        evolveSchemaList,
        null);
  }

  private static Map<String, Object> createComplexRow(Long id, String date) {
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
        Arrays.asList(
            Map.of("id", 1L, "nullable_string", "value", "nullable_int", 10),
            Map.of("id", 2L, "nullable_string", "value_2", "nullable_int", 20));

    List<UpdateInput> updateInputs =
        Arrays.asList(
            new UpdateInput(
                new HashMap<>() {
                  {
                    put("nullable_string", null);
                    put("nullable_int", null);
                  }
                },
                "id=1"),
            new UpdateInput(
                new HashMap<>() {
                  {
                    put("nullable_string", "new");
                    put("nullable_int", null);
                  }
                },
                "id=2"));

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
    return Arguments.of(
        "null_values", schema, partitionSpec, initialData, updateInputs, expectedData, null, null);
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
        Arrays.asList(
            Map.of("id", 1L, "long_string", "initial", "big_number", BigDecimal.ONE),
            Map.of("id", 2L, "long_string", "initial", "big_number", BigDecimal.ONE));
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "long_string", longString, "big_number", bigNumber),
            Map.of(
                "id",
                2L,
                "long_string",
                "initial",
                "big_number",
                BigDecimal.ONE.setScale(10, RoundingMode.HALF_UP)));

    UpdateInput updateInput =
        new UpdateInput(Map.of("long_string", longString, "big_number", bigNumber), "id=1");
    return Arguments.of(
        "extreme_values",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(updateInput),
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
    List<Map<String, Object>> expectedData =
        Arrays.asList(Map.of("id", 1L, "special_string", specialString));
    UpdateInput updateInput = new UpdateInput(Map.of("special_string", specialString), "id=1");
    return Arguments.of(
        "special_characters",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(updateInput),
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
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of(
                "id", 1L, "timestamp_with_tz", changedTime.withOffsetSameInstant(ZoneOffset.UTC)));
    UpdateInput updateInput = new UpdateInput(Map.of("timestamp_with_tz", changedTime), "id=1");
    return Arguments.of(
        "time_zone_handling",
        schema,
        partitionSpec,
        initialData,
        Arrays.asList(updateInput),
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

    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));

    List<UpdateInput> inputData =
        Arrays.asList(
            new UpdateInput(Map.of("name", "John Doe", "email", "john.doe@example.com"), "id = 1"),
            new UpdateInput(Map.of("email", "jane@example.com"), "id = 2"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John Doe", "email", "john.doe@example.com"),
            Map.of("id", 2L, "name", "Jane", "email", "jane@example.com"));
    return Arguments.of(
        "schema_evolution",
        initialSchema,
        partitionSpec,
        initialData,
        inputData,
        expectedData,
        Arrays.asList(evolveSchema),
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

    List<UpdateInput> updateInputs = new ArrayList<>();
    List<Pair<Class<? extends Throwable>, String>> errors = new ArrayList<>();

    // Missing required columns
    updateInputs.add(
        new UpdateInput(
            new HashMap<String, Object>() {
              {
                put("name", null);
                put("value", null);
              }
            },
            "id=1"));
    errors.add(
        Pair.of(
            ValidationException.class, "Required fields cannot contain null values - name,value"));

    // Null value for required columns
    updateInputs.add(
        new UpdateInput(
            new HashMap<String, Object>() {
              {
                put("name", "Bob");
                put("value", null);
              }
            },
            "id=2"));
    errors.add(
        Pair.of(ValidationException.class, "Required fields cannot contain null values - value"));

    // invalid columns
    updateInputs.add(
        new UpdateInput(
            new HashMap<String, Object>() {
              {
                put("name_2", "Bob");
                put("value_2", null);
              }
            },
            "id=2"));
    errors.add(Pair.of(ValidationException.class, "Could not find column name_2"));

    // Invalid condition
    updateInputs.add(
        new UpdateInput(
            new HashMap<String, Object>() {
              {
                put("name", "Bob");
                put("value", null);
              }
            },
            "id2 = 2"));
    errors.add(Pair.of(ValidationException.class, "Column does not exist id2"));

    updateInputs.add(
        new UpdateInput(
            new HashMap<String, Object>() {
              {
                put("name", "Bob");
                put("value", null);
              }
            },
            "id = value"));
    errors.add(Pair.of(ValidationException.class, "Invalid value id"));

    updateInputs.add(new UpdateInput(Map.of("name", "John Doe", "value", 300.0), "id=1"));
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 2L, "name", "Jane", "value", 200.0),
            Map.of("id", 1L, "name", "John Doe", "value", 300.0));
    return Arguments.of(
        "error_handling",
        schema,
        partitionSpec,
        initialData,
        updateInputs,
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
            "update_partition_strategy_test_" + UUID.randomUUID().toString().replace('-', '_'));
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

    swiftLakeEngine
        .update(tableName)
        .conditionSql("id = 1")
        .updateSets(
            Map.of(
                "date",
                LocalDate.parse("2024-02-02"),
                "timestamp",
                LocalDateTime.parse("2024-04-04T12:00:00"),
                "name",
                "John Doe"))
        .execute();

    swiftLakeEngine
        .insertInto(tableName)
        .sql(
            TestUtil.createSelectSql(
                Arrays.asList(
                    Map.of(
                        "id",
                        3L,
                        "date",
                        LocalDate.parse("2025-01-03"),
                        "timestamp",
                        LocalDateTime.parse("2025-02-02T06:06:06"),
                        "name",
                        "Bob")),
                schema))
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

    TableIdentifier tableId = TableIdentifier.of("test_db", "update_concurrent_operations_test");
    swiftLakeEngine
        .getCatalog()
        .createTable(tableId, schema, partitionSpec, Map.of("commit.retry.num-retries", "10"));
    String tableName = tableId.toString();

    List<Map<String, Object>> initialData =
        IntStream.range(0, 10)
            .mapToObj(i -> Map.of("id", (Object) (long) i, "name", "name_" + i))
            .collect(Collectors.toList());
    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    List<Map<String, Object>> expectedData = new ArrayList<>();

    // Perform multiple concurrent update operations
    for (int j = 0; j < 3; j++) {
      int index = j;
      IntStream.range(0, 10)
          .parallel()
          .forEach(
              i -> {
                swiftLakeEngine
                    .update(tableName)
                    .conditionSql("id = " + i)
                    .updateSets(Map.of("id", (long) i, "name", "name_" + i + index))
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
        TableIdentifier.of("test_db", "update_partition_evolution_data_movement_test");
    swiftLakeEngine.getCatalog().createTable(tableId, schema, initialSpec);
    String tableName = tableId.toString();

    // Initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "year", 2025, "month", 1, "value", 100.0),
            Map.of("id", 2L, "name", "Jane", "year", 2025, "month", 2, "value", 200.0),
            Map.of("id", 3L, "name", "Bob", "year", 2025, "month", 3, "value", 300.0));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Perform first update
    swiftLakeEngine
        .update(tableName)
        .conditionSql("id=1")
        .updateSets(Map.of("id", 1L, "name", "John Doe", "year", 2025, "month", 1, "value", 150.0))
        .execute();

    // Evolve partition spec to include month
    swiftLakeEngine.getTable(tableName).updateSpec().addField("month").commit();

    swiftLakeEngine
        .insertInto(tableName)
        .sql(
            TestUtil.createSelectSql(
                Arrays.asList(
                    Map.of("id", 4L, "name", "Alice", "year", 2025, "month", 4, "value", 400.0)),
                schema))
        .processSourceTables(false)
        .execute();

    // Perform second update with new partition spec
    swiftLakeEngine
        .update(tableName)
        .conditionSql("id=2")
        .updateSets(Map.of("name", "Jane Doe", "year", 2025, "month", 2, "value", 250.0))
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
  void testTableBatchTransaction() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.IntegerType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    TableIdentifier tableId = TableIdentifier.of("test_db", "update_batch_transaction_test");
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    // Insert initial data
    List<Map<String, Object>> initialData =
        IntStream.range(0, 100)
            .mapToObj(i -> Map.of("id", (Object) (long) i, "name", "name_" + i, "value", i))
            .collect(Collectors.toList());

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Update multiple records using tableBatchTransaction
    TableBatchTransaction transaction =
        TableBatchTransaction.builderFor(swiftLakeEngine, tableName).build();
    swiftLakeEngine
        .update(transaction)
        .conditionSql("value < 50")
        .updateSets(Map.of("name", "updated_batch"))
        .execute();

    assertThatThrownBy(
            () ->
                swiftLakeEngine
                    .update(transaction)
                    .conditionSql("value BETWEEN 50 AND 59")
                    .updateSets(Map.of("name", "updated_batch"))
                    .execute())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Found conflicting files that can contain records matching");

    // Recreate the table with partitioning
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
    swiftLakeEngine
        .getCatalog()
        .createTable(
            tableId, schema, PartitionSpec.builderFor(schema).truncate("value", 10).build());

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Capture snapshot ID before the update
    long beforeSnapshotId = swiftLakeEngine.getTable(tableName).currentSnapshot().snapshotId();

    TableBatchTransaction newTransaction =
        TableBatchTransaction.builderFor(swiftLakeEngine, tableName).build();
    swiftLakeEngine
        .update(newTransaction)
        .conditionSql("value < 50")
        .updateSets(Map.of("name", "updated_batch"))
        .execute();

    swiftLakeEngine
        .update(newTransaction)
        .conditionSql("value BETWEEN 50 AND 59")
        .updateSets(Map.of("name", "updated_batch"))
        .execute();

    Table table = swiftLakeEngine.getTable(tableName);
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(beforeSnapshotId);

    // Commit both updates in one commit
    newTransaction.commit();

    table = swiftLakeEngine.getTable(tableName);
    long afterSnapshotId = table.currentSnapshot().snapshotId();
    assertThat(afterSnapshotId).isNotEqualTo(beforeSnapshotId);

    // Verify the data was updated correctly
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    long updatedCount =
        actualData.stream().filter(m -> "updated_batch".equals(m.get("name"))).count();

    assertThat(updatedCount).isEqualTo(60); // 60 records should be updated

    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSkipDataSorting(boolean isPartitioned) {
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
    TableIdentifier tableId = TableIdentifier.of("test_db", "update_skip_sorting_test");
    swiftLakeEngine
        .getCatalog()
        .buildTable(tableId, schema)
        .withPartitionSpec(partitionSpec)
        .withSortOrder(sortOrder)
        .create();
    String tableName = tableId.toString();

    // Insert initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 3L, "name", "Charlie", "value", 300),
            Map.of("id", 2L, "name", "Bob", "value", 200),
            Map.of("id", 1L, "name", "Alice", "value", 100));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(
            "SELECT * FROM ("
                + TestUtil.createSelectSql(initialData, schema)
                + ") ORDER BY id DESC")
        .skipDataSorting(true)
        .processSourceTables(false)
        .execute();

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 3L, "name", "Charlie", "value", 300),
            Map.of("id", 2L, "name", "Bob", "value", 200),
            Map.of("id", 1L, "name", "Alice", "value", 100));

    List<Map<String, Object>> actualData =
        TestUtil.getRecordsFromTableInFileOrder(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyElementsOf(expectedData);

    // Update with skipDataSorting=true option
    swiftLakeEngine
        .update(tableName)
        .conditionSql("id = 1")
        .updateSets(Map.of("name", "Alice Updated", "value", 150))
        .skipDataSorting(true)
        .execute();

    expectedData =
        Arrays.asList(
            Map.of("id", 3L, "name", "Charlie", "value", 300),
            Map.of("id", 2L, "name", "Bob", "value", 200),
            Map.of("id", 1L, "name", "Alice Updated", "value", 150));

    actualData = TestUtil.getRecordsFromTableInFileOrder(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyElementsOf(expectedData);

    // Update with skipDataSorting=false option
    swiftLakeEngine
        .update(tableName)
        .conditionSql("id = 2")
        .updateSets(Map.of("name", "Bob Updated", "value", 250))
        .skipDataSorting(false)
        .execute();

    // Verify the update was successful
    expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "Alice Updated", "value", 150),
            Map.of("id", 2L, "name", "Bob Updated", "value", 250),
            Map.of("id", 3L, "name", "Charlie", "value", 300));

    actualData = TestUtil.getRecordsFromTableInFileOrder(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyElementsOf(expectedData);

    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testBranch(boolean isPartitioned) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.IntegerType.get()));
    PartitionSpec partitionSpec =
        isPartitioned
            ? PartitionSpec.builderFor(schema).identity("id").build()
            : PartitionSpec.unpartitioned();
    TableIdentifier tableId = TableIdentifier.of("test_db", "update_branch_test");
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    // Insert initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "name", "Alice", "value", 100),
            Map.of("id", 2L, "name", "Bob", "value", 200));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Create a new branch
    String branchName = "test-branch";
    swiftLakeEngine.getTable(tableName).manageSnapshots().createBranch(branchName).commit();

    // Update data on the new branch
    swiftLakeEngine
        .update(tableName)
        .conditionSql("id = 1")
        .updateSets(Map.of("name", "Alice on Branch", "value", 150))
        .branch(branchName)
        .execute();

    // Verify data on the branch
    List<Map<String, Object>> expectedBranchData =
        Arrays.asList(
            Map.of("id", 1L, "name", "Alice on Branch", "value", 150),
            Map.of("id", 2L, "name", "Bob", "value", 200));

    List<Map<String, Object>> actualBranchData =
        TestUtil.getRecordsFromTable(
            swiftLakeEngine, TestUtil.getTableNameForBranch(tableId, branchName));

    assertThat(actualBranchData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedBranchData);

    // Verify main branch data is unchanged
    List<Map<String, Object>> mainBranchData =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(mainBranchData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(initialData);

    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testSnapshotMetadata() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    TableIdentifier tableId = TableIdentifier.of("test_db", "update_snapshot_metadata_test");
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    // Insert initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(Map.of("id", 1L, "name", "Alice"), Map.of("id", 2L, "name", "Bob"));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Custom metadata for the update
    Map<String, String> snapshotMetadata =
        Map.of(
            "user", "test-user",
            "purpose", "testing");

    // Update with snapshot metadata
    swiftLakeEngine
        .update(tableName)
        .conditionSql("id = 1")
        .updateSets(Map.of("name", "Alice Updated"))
        .snapshotMetadata(snapshotMetadata)
        .execute();

    // Verify that the snapshot contains our custom metadata
    Table table = swiftLakeEngine.getTable(tableName);
    Map<String, String> actualMetadata = table.currentSnapshot().summary();

    assertThat(actualMetadata).containsAllEntriesOf(snapshotMetadata);

    // Verify the update was successful
    List<Map<String, Object>> expectedData =
        Arrays.asList(Map.of("id", 1L, "name", "Alice Updated"), Map.of("id", 2L, "name", "Bob"));

    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testUpdateWithDifferentIsolationLevels() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "year", Types.IntegerType.get()),
            Types.NestedField.required(3, "value", Types.StringType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("year").build();

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "update_isolation_test_" + UUID.randomUUID().toString().replace('-', '_'));
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
    Table tableStateAfterInitialInsertCopy = swiftLakeEngine.getTable(tableName);
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

    // Update ID=1 record in the 2023 partition
    swiftLakeEngine
        .update(tableName)
        .conditionSql("id = 1")
        .updateSets(Map.of("value", "updated_2023_value"))
        .execute();

    // Update should succeed since it's working on the most recent table state
    List<Map<String, Object>> dataAfterFirstUpdate =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(dataAfterFirstUpdate)
        .anyMatch(row -> row.get("id").equals(1L) && row.get("value").equals("updated_2023_value"));

    // Try to update the 2024 partition using the original table state
    // This should fail due to conflicting files, as we're using the default isolation level
    assertThatThrownBy(
            () -> {
              swiftLakeEngine
                  .update(tableStateAfterInitialInsert)
                  .conditionSql("year = 2024")
                  .updateSets(Map.of("value", "updated_2024_value"))
                  .execute();
            })
        .hasMessageContaining("Found conflicting files");

    // Try again with SNAPSHOT isolation level
    // This should succeed since SNAPSHOT isolation ignores new files
    swiftLakeEngine
        .update(tableStateAfterInitialInsertCopy)
        .conditionSql("year = 2024")
        .updateSets(Map.of("value", "updated_2024_value"))
        .isolationLevel(org.apache.iceberg.IsolationLevel.SNAPSHOT)
        .execute();

    // Refresh and verify the update succeeded
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId())
        .isNotEqualTo(tableStateAfterSecondInsert.currentSnapshot().snapshotId());

    // Try to update ID=2 again using an outdated table state
    // Even with SNAPSHOT isolation, this should fail because the record was already updated
    assertThatThrownBy(
            () -> {
              swiftLakeEngine
                  .update(tableStateAfterSecondInsert)
                  .conditionSql("id = 2")
                  .updateSets(Map.of("value", "updated_again_2024_value"))
                  .isolationLevel(org.apache.iceberg.IsolationLevel.SNAPSHOT)
                  .execute();
            })
        .hasMessageContaining("Missing required files to delete");

    // Attempting with SERIALIZABLE isolation on outdated state should also fail
    assertThatThrownBy(
            () -> {
              swiftLakeEngine
                  .update(tableStateAfterSecondInsertCopy)
                  .conditionSql("id = 2")
                  .updateSets(Map.of("value", "updated_again_2024_value"))
                  .isolationLevel(org.apache.iceberg.IsolationLevel.SERIALIZABLE)
                  .execute();
            })
        .hasMessageContaining("Found conflicting files");

    // Use SNAPSHOT isolation with current table state
    swiftLakeEngine
        .update(tableName)
        .conditionSql("id = 2")
        .updateSets(Map.of("value", "updated_again_2024_value"))
        .isolationLevel(org.apache.iceberg.IsolationLevel.SNAPSHOT)
        .execute();

    // Verify we have latest data
    table.refresh();
    long updatedSnapshotId = table.currentSnapshot().snapshotId();

    // Verify the data after SNAPSHOT isolation update
    List<Map<String, Object>> dataAfterSnapshotUpdate =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(dataAfterSnapshotUpdate)
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
              assertThat(row.get("value")).isEqualTo("updated_again_2024_value");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(3L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("value")).isEqualTo("second_2024_value");
            });

    // Test SERIALIZABLE isolation level with current table state
    swiftLakeEngine
        .update(tableName)
        .conditionSql("id = 3")
        .updateSets(Map.of("value", "serializable_update_value"))
        .isolationLevel(org.apache.iceberg.IsolationLevel.SERIALIZABLE)
        .execute();

    // The update should create a new snapshot
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(updatedSnapshotId);

    // Insert another record to check how updates work with recent changes
    List<Map<String, Object>> thirdInsertData =
        Arrays.asList(Map.of("id", 4L, "year", 2024, "value", "third_2024_value"));
    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(thirdInsertData, schema))
        .processSourceTables(false)
        .execute();

    // Capture table state before final update
    table.refresh();
    Table tableStateBeforeFinalUpdate = swiftLakeEngine.getTable(tableName);

    // Update ID=4 record with SERIALIZABLE isolation - should succeed with latest state
    swiftLakeEngine
        .update(tableName)
        .conditionSql("id = 4")
        .updateSets(Map.of("value", "updated_third_value"))
        .isolationLevel(org.apache.iceberg.IsolationLevel.SERIALIZABLE)
        .execute();

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
              assertThat(row.get("value")).isEqualTo("updated_again_2024_value");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(3L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("value")).isEqualTo("serializable_update_value");
            })
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(4L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("value")).isEqualTo("updated_third_value");
            });

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  public static class UpdateInput {
    Map<String, Object> inputData;
    String conditionSql;

    public UpdateInput(Map<String, Object> inputData, String conditionSql) {
      this.inputData = inputData;
      this.conditionSql = conditionSql;
    }
  }
}
