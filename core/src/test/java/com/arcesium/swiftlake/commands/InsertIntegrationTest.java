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
import static org.assertj.core.api.Assertions.tuple;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.TestUtil;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import com.arcesium.swiftlake.writer.TableBatchTransaction;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
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
public class InsertIntegrationTest {
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
  void testInsert(
      String testName,
      Schema schema,
      PartitionSpec partitionSpec,
      List<List<Map<String, Object>>> inputDataList,
      List<Map<String, Object>> expectedData,
      List<Consumer<Table>> evolveSchema,
      List<Pair<Class<? extends Throwable>, String>> errors) {
    // Create table
    TableIdentifier tableId = TableIdentifier.of("test_db", "insert_test_table_" + testName);
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    for (int i = 0; i < inputDataList.size(); i++) {
      if (evolveSchema != null && evolveSchema.size() > i && evolveSchema.get(i) != null) {
        evolveSchema.get(i).accept(swiftLakeEngine.getTable(tableName));
        schema = swiftLakeEngine.getTable(tableName).schema();
      }
      List<Map<String, Object>> inputData = inputDataList.get(i);
      String inputSql = TestUtil.createSelectSql(inputData, schema);
      Runnable insertFunc =
          () -> {
            swiftLakeEngine
                .insertInto(tableName)
                .sql(inputSql)
                .processSourceTables(false)
                .execute();
          };
      if (errors != null && errors.size() > i && errors.get(i) != null) {
        var throwableAssert = assertThatThrownBy(() -> insertFunc.run());
        var error = errors.get(i);
        if (error.getLeft() != null) {
          throwableAssert.isInstanceOf(error.getLeft());
        }
        if (error.getRight() != null) {
          throwableAssert.hasMessageContaining(error.getRight());
        }
      } else {
        insertFunc.run();
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
        emptySourceTestCase(),
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
    List<List<Map<String, Object>>> inputDataList = new ArrayList<>();
    inputDataList.add(
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane")));
    inputDataList.add(Arrays.asList(Map.of("id", 3L, "name", "Bob")));
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));
    return Arguments.of(
        "simple_schema", schema, partitionSpec, inputDataList, expectedData, null, null);
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
    List<List<Map<String, Object>>> inputDataList = new ArrayList<>();
    inputDataList.add(
        Arrays.asList(createComplexRow(1L, "2025-01-01"), createComplexRow(2L, "2025-01-01")));
    inputDataList.add(Arrays.asList(createComplexRow(3L, "2025-01-01")));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            createComplexRow(1L, "2025-01-01"),
            createComplexRow(2L, "2025-01-01"),
            createComplexRow(3L, "2025-01-01"));
    return Arguments.of(
        "complex_types", schema, partitionSpec, inputDataList, expectedData, null, null);
  }

  private static Arguments emptySourceTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    return Arguments.of(
        "empty_source", schema, partitionSpec, Arrays.asList(), Arrays.asList(), null, null);
  }

  private static Arguments longHistoryChainTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    List<List<Map<String, Object>>> inputDataList = new ArrayList<>();
    List<Consumer<Table>> evolveSchemaList = new ArrayList<>();
    List<Map<String, Object>> expectedData = new ArrayList<>();
    List<Map<String, Object>> initialData = Arrays.asList(Map.of("id", 1L, "value", "Initial"));
    inputDataList.add(initialData);
    evolveSchemaList.add(null);
    expectedData.add(
        new HashMap<>() {
          {
            put("id", 1L);
            put("value_2", "Initial");
            put("value_3", null);
            put("value_5", null);
          }
        });
    LocalDate epochDay = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC).toLocalDate();
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
        inputDataList.add(Arrays.asList(Map.of("id", 1, "value", "Update" + i)));
        expectedData.add(
            new HashMap<>() {
              {
                put("id", 1L);
                put("value_2", expectedStringValue);
                put("value_3", null);
                put("value_5", null);
              }
            });
      } else if (i < 20) {
        inputDataList.add(Arrays.asList(Map.of("id", 1L, "value", "Update" + i)));
        expectedData.add(
            new HashMap<>() {
              {
                put("id", 1L);
                put("value_2", expectedStringValue);
                put("value_3", null);
                put("value_5", null);
              }
            });
      } else if (i < 50) {
        inputDataList.add(Arrays.asList(Map.of("id", 1L, "value_2", "Update" + i)));
        expectedData.add(
            new HashMap<>() {
              {
                put("id", 1L);
                put("value_2", expectedStringValue);
                put("value_3", null);
                put("value_5", null);
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
                    epochDay.plusDays(i))));
        expectedData.add(
            new HashMap<>() {
              {
                put("id", 1L);
                put("value_2", expectedStringValue);
                put("value_3", null);
                put("value_5", decimal28PrecisionValue);
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
                    epochDay.plusDays(i))));
        expectedData.add(
            new HashMap<>() {
              {
                put("id", 1L);
                put("value_2", expectedStringValue);
                put("value_3", null);
                put("value_5", decimal38PrecisionValue);
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
                    epochDay.plusDays(i))));
        expectedData.add(
            new HashMap<>() {
              {
                put("id", 1L);
                put("value_2", expectedStringValue);
                put("value_3", expectedStringValue);
                put("value_5", decimal38PrecisionValue);
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
                    decimal38PrecisionValue)));

        var index = i;
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

    return Arguments.of(
        "long_history_chain",
        schema,
        partitionSpec,
        inputDataList,
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

  private static Arguments nullValuesTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "value", Types.DoubleType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    List<List<Map<String, Object>>> inputDataList = new ArrayList<>();

    inputDataList.add(Arrays.asList(Map.of("id", 1L, "name", "John", "value", 100.0)));
    inputDataList.add(
        Arrays.asList(
            new HashMap<>() {
              {
                put("id", null);
                put("name", null);
                put("value", null);
              }
            },
            new HashMap<>() {
              {
                put("id", 2L);
                put("name", null);
                put("value", null);
              }
            }));

    List<Map<String, Object>> expectedData =
        inputDataList.stream().flatMap(Collection::stream).collect(Collectors.toList());
    return Arguments.of(
        "null_values", schema, partitionSpec, inputDataList, expectedData, null, null);
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
    List<List<Map<String, Object>>> inputDataList = new ArrayList<>();
    inputDataList.add(
        Arrays.asList(Map.of("id", 1L, "long_string", "initial", "big_number", BigDecimal.ONE)));
    inputDataList.add(
        Arrays.asList(Map.of("id", 2L, "long_string", longString, "big_number", bigNumber)));
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "long_string", "initial", "big_number", BigDecimal.ONE.setScale(10)),
            Map.of("id", 2L, "long_string", longString, "big_number", bigNumber));

    return Arguments.of(
        "extreme_values", schema, partitionSpec, inputDataList, expectedData, null, null);
  }

  private static Arguments unicodeAndSpecialCharactersTestCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "special_string", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    String specialString = "!@#$%^&*()_+{ ¡™£¢∞§¶•ªº}[]|\\:;こんにちは\"'<你好，世界>,.?/~`";
    List<List<Map<String, Object>>> inputDataList = new ArrayList<>();
    inputDataList.add(Arrays.asList(Map.of("id", 1L, "special_string", "normal")));
    inputDataList.add(Arrays.asList(Map.of("id", 2L, "special_string", specialString)));
    List<Map<String, Object>> expectedData =
        inputDataList.stream().flatMap(Collection::stream).collect(Collectors.toList());
    return Arguments.of(
        "special_characters", schema, partitionSpec, inputDataList, expectedData, null, null);
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
    List<List<Map<String, Object>>> inputDataList = new ArrayList<>();
    inputDataList.add(Arrays.asList(Map.of("id", 1L, "timestamp_with_tz", utcTime)));
    inputDataList.add(Arrays.asList(Map.of("id", 2L, "timestamp_with_tz", changedTime)));
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "timestamp_with_tz", utcTime),
            Map.of(
                "id", 2L, "timestamp_with_tz", changedTime.withOffsetSameInstant(ZoneOffset.UTC)));

    return Arguments.of(
        "time_zone_handling", schema, partitionSpec, inputDataList, expectedData, null, null);
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
    List<List<Map<String, Object>>> inputDataList = new ArrayList<>();
    inputDataList.add(Arrays.asList(Map.of("id", 1L, "name", "John")));
    inputDataList.add(
        Arrays.asList(
            Map.of("id", 2L, "name", "John Doe", "email", "john.doe@example.com"),
            Map.of("id", 3L, "name", "Jane", "email", "jane@example.com")));
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            new HashMap<>() {
              {
                put("id", 1L);
                put("name", "John");
                put("email", null);
              }
            },
            Map.of("id", 2L, "name", "John Doe", "email", "john.doe@example.com"),
            Map.of("id", 3L, "name", "Jane", "email", "jane@example.com"));

    return Arguments.of(
        "schema_evolution",
        initialSchema,
        partitionSpec,
        inputDataList,
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
    List<List<Map<String, Object>>> inputDataList = new ArrayList<>();
    List<Pair<Class<? extends Throwable>, String>> errors = new ArrayList<>();
    // Initial data
    inputDataList.add(
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "value", 100.0),
            Map.of("id", 2L, "name", "Jane", "value", 200.0)));
    errors.add(null);
    // Missing required columns
    inputDataList.add(Arrays.asList(Map.of("id", 3L, "value", 300.0), Map.of("id", 4L)));
    errors.add(
        Pair.of(
            ValidationException.class, "Required fields cannot contain null values - name,value"));

    // Null value for required columns
    inputDataList.add(
        Arrays.asList(
            new HashMap<String, Object>() {
              {
                put("id", 3L);
                put("name", "Bob");
                put("value", null);
                put("operation_type", "I");
              }
            }));
    errors.add(
        Pair.of(ValidationException.class, "Required fields cannot contain null values - value"));

    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John", "value", 100.0),
            Map.of("id", 2L, "name", "Jane", "value", 200.0));

    return Arguments.of(
        "error_handling", schema, partitionSpec, inputDataList, expectedData, null, errors);
  }

  @ParameterizedTest
  @MethodSource("providePartitionStrategies")
  void testDifferentPartitionStrategies(
      PartitionSpec partitionSpec, List<Pair<String, Long>> partitionLevelRecordCounts) {
    Schema schema = getSchemaForPartitionStrategiesTest();

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "insert_partition_strategy_test_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine.getCatalog().createTable(tableId, schema, partitionSpec);
    String tableName = tableId.toString();

    List<Map<String, Object>> inputData =
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
        .sql(TestUtil.createSelectSql(inputData, schema))
        .processSourceTables(false)
        .execute();

    inputData =
        Arrays.asList(
            Map.of(
                "id",
                3L,
                "date",
                LocalDate.parse("2024-02-02"),
                "timestamp",
                LocalDateTime.parse("2024-04-04T12:00:00"),
                "name",
                "John Doe"),
            Map.of(
                "id",
                4L,
                "date",
                LocalDate.parse("2025-01-03"),
                "timestamp",
                LocalDateTime.parse("2025-02-02T06:06:06"),
                "name",
                "Bob"));
    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(inputData, schema))
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
                "Jane"),
            Map.of(
                "id",
                3L,
                "date",
                LocalDate.parse("2024-02-02"),
                "timestamp",
                LocalDateTime.parse("2024-04-04T12:00:00"),
                "name",
                "John Doe"),
            Map.of(
                "id",
                4L,
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
        Arguments.of(PartitionSpec.unpartitioned(), Arrays.asList(Pair.of("", 4L))),
        Arguments.of(
            PartitionSpec.builderFor(schema).identity("id").build(),
            Arrays.asList(
                Pair.of("id=1", 1L),
                Pair.of("id=2", 1L),
                Pair.of("id=3", 1L),
                Pair.of("id=4", 1L))),
        Arguments.of(
            PartitionSpec.builderFor(schema).year("date").build(),
            Arrays.asList(Pair.of("date_year=2024", 1L), Pair.of("date_year=2025", 3L))),
        Arguments.of(
            PartitionSpec.builderFor(schema).identity("id").year("date").build(),
            Arrays.asList(
                Pair.of("id=1/date_year=2025", 1L),
                Pair.of("id=4/date_year=2025", 1L),
                Pair.of("id=2/date_year=2025", 1L),
                Pair.of("id=3/date_year=2024", 1L))),
        Arguments.of(
            PartitionSpec.builderFor(schema).month("date").bucket("id", 5).build(),
            Arrays.asList(
                Pair.of("date_month=2025-01/id_bucket=1", 1L),
                    Pair.of("date_month=2024-02/id_bucket=0", 1L),
                Pair.of("date_month=2025-01/id_bucket=0", 1L),
                    Pair.of("date_month=2025-01/id_bucket=2", 1L))),
        Arguments.of(
            PartitionSpec.builderFor(schema).month("date").truncate("name", 3).build(),
            Arrays.asList(
                Pair.of("date_month=2025-01/name_trunc=Joh", 1L),
                Pair.of("date_month=2024-02/name_trunc=Joh", 1L),
                Pair.of("date_month=2025-01/name_trunc=Jan", 1L),
                Pair.of("date_month=2025-01/name_trunc=Bob", 1L))),
        Arguments.of(
            PartitionSpec.builderFor(schema).hour("timestamp").identity("date").build(),
            Arrays.asList(
                Pair.of("timestamp_hour=2025-01-01-02/date=2025-01-01", 1L),
                Pair.of("timestamp_hour=2025-01-01-02/date=2025-01-02", 1L),
                Pair.of("timestamp_hour=2024-04-04-12/date=2024-02-02", 1L),
                Pair.of("timestamp_hour=2025-02-02-06/date=2025-01-03", 1L))),
        Arguments.of(
            PartitionSpec.builderFor(schema).day("timestamp").truncate("id", 2).build(),
            Arrays.asList(
                Pair.of("timestamp_day=2025-01-01/id_trunc=0", 1L),
                Pair.of("timestamp_day=2024-04-04/id_trunc=2", 1L),
                Pair.of("timestamp_day=2025-01-01/id_trunc=2", 1L),
                Pair.of("timestamp_day=2025-02-02/id_trunc=4", 1L))),
        Arguments.of(
            PartitionSpec.builderFor(schema).month("timestamp").month("date").build(),
            Arrays.asList(
                Pair.of("timestamp_month=2025-01/date_month=2025-01", 2L),
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
    TableIdentifier tableId = TableIdentifier.of("test_db", "insert_concurrent_operations_test");
    swiftLakeEngine
        .getCatalog()
        .createTable(tableId, schema, partitionSpec, Map.of("commit.retry.num-retries", "10"));
    String tableName = tableId.toString();
    List<Map<String, Object>> expectedData = new ArrayList<>();

    // Perform multiple concurrent insert operations
    for (int j = 0; j < 3; j++) {
      int index = j;
      IntStream.range(0, 10)
          .parallel()
          .forEach(
              i -> {
                List<Map<String, Object>> data =
                    Arrays.asList(Map.of("id", (long) i, "name", "name_" + i + index));
                swiftLakeEngine
                    .insertInto(tableName)
                    .sql(TestUtil.createSelectSql(data, schema))
                    .processSourceTables(false)
                    .execute();
              });
      expectedData.addAll(
          IntStream.range(0, 10)
              .mapToObj(
                  i ->
                      new HashMap<String, Object>() {
                        {
                          put("id", (long) i);
                          put("name", "name_" + i + index);
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

  @Test
  void testTableBatchTransactionWithThreads() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "department", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.StringType.get()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "batch_thread_table_" + UUID.randomUUID().toString().replace('-', '_'));
    Table table =
        swiftLakeEngine
            .getCatalog()
            .createTable(
                tableId, schema, PartitionSpec.builderFor(schema).identity("department").build());
    String tableName = tableId.toString();

    // Create batch transaction
    TableBatchTransaction batchTransaction =
        TableBatchTransaction.builderFor(swiftLakeEngine, table).build();

    // Prepare data for different departments (simulating different partitions)
    List<String> departments = Arrays.asList("HR", "IT", "Finance", "Marketing", "Sales");
    List<Map<String, Object>> allExpectedData = new ArrayList<>();

    // Use a CountDownLatch to wait for all threads to complete
    CountDownLatch latch = new CountDownLatch(departments.size());

    // Process each department in a separate thread
    for (int i = 0; i < departments.size(); i++) {
      final String department = departments.get(i);
      final int startId = i * 10; // Different ID range for each department

      // Create data for this department
      List<Map<String, Object>> departmentData = new ArrayList<>();
      for (int j = 0; j < 5; j++) {
        long id = startId + j;
        Map<String, Object> row =
            Map.of(
                "id", id,
                "department", department,
                "value", department + "-" + j);
        departmentData.add(row);
        allExpectedData.add(row);
      }

      // Process this department in a separate thread
      Thread thread =
          new Thread(
              () -> {
                try {
                  // Simulate some processing time
                  Thread.sleep(50);
                  swiftLakeEngine
                      .insertInto(batchTransaction)
                      .sql(TestUtil.createSelectSql(departmentData, schema))
                      .processSourceTables(false)
                      .execute();
                } catch (Exception e) {
                  e.printStackTrace();
                } finally {
                  latch.countDown();
                }
              });
      thread.start();
    }

    // Wait for all threads to complete
    latch.await(30, TimeUnit.SECONDS);

    // Commit all operations in a single transaction
    batchTransaction.commit();

    // Verify all data was inserted correctly
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(allExpectedData);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testProcessSourceTables() throws IOException {
    // Create source table with data
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    TableIdentifier sourceTableId =
        TableIdentifier.of(
            "test_db", "source_table_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine.getCatalog().createTable(sourceTableId, schema, PartitionSpec.unpartitioned());
    String sourceTableName = sourceTableId.toString();

    // Insert data into source table
    List<Map<String, Object>> sourceData =
        Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 2L, "name", "Jane"));
    swiftLakeEngine
        .insertInto(sourceTableName)
        .sql(TestUtil.createSelectSql(sourceData, schema))
        .processSourceTables(false)
        .execute();

    // Create target table
    TableIdentifier targetTableId =
        TableIdentifier.of(
            "test_db", "target_table_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine.getCatalog().createTable(targetTableId, schema, PartitionSpec.unpartitioned());
    String targetTableName = targetTableId.toString();

    // Test processSourceTables=true
    swiftLakeEngine
        .insertInto(targetTableName)
        .sql("SELECT * FROM " + sourceTableName)
        .processSourceTables(true)
        .execute();

    // Verify results
    List<Map<String, Object>> actualData =
        TestUtil.getRecordsFromTable(swiftLakeEngine, targetTableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(sourceData);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, sourceTableName);
    TestUtil.dropIcebergTable(swiftLakeEngine, targetTableName);
  }

  @Test
  void testExecuteSqlOnceOnly() {
    // Create table
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "exec_once_table_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine
        .getCatalog()
        .createTable(tableId, schema, PartitionSpec.builderFor(schema).identity("id").build());
    String tableName = tableId.toString();

    // Prepare data
    List<Map<String, Object>> testData =
        Arrays.asList(
            Map.of("id", 1L, "name", "John"),
            Map.of("id", 2L, "name", "Jane"),
            Map.of("id", 3L, "name", "Bob"));

    // Insert with executeSqlOnceOnly=true
    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(testData, schema))
        .executeSqlOnceOnly(true)
        .processSourceTables(false)
        .execute();

    // Verify results
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(testData);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testMybatisStatement() {
    // Create table
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "mybatis_table_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    // Create MyBatis session factory with a test mapper
    SwiftLakeSqlSessionFactory sqlSessionFactory =
        (SwiftLakeSqlSessionFactory)
            swiftLakeEngine.createSqlSessionFactory("mybatis/mybatis-config.xml");

    // Execute insert with MyBatis statement
    Map<String, Object> params = new HashMap<>();
    params.put("limit", 3);

    // #{} parameters are not supported in write operations
    assertThatThrownBy(
            () ->
                swiftLakeEngine
                    .insertInto(tableName)
                    .mybatisStatement("TestMapper.getTestData", params)
                    .sqlSessionFactory(sqlSessionFactory)
                    .processSourceTables(false)
                    .execute())
        .hasCauseInstanceOf(SQLException.class)
        .cause()
        .hasMessageContaining("Invalid Input Error: Parameter count mismatch");

    swiftLakeEngine
        .insertInto(tableName)
        .mybatisStatement("TestMapper.getTestData2", params)
        .sqlSessionFactory(sqlSessionFactory)
        .processSourceTables(false)
        .execute();

    // Verify results
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData).hasSize(3);

    // Test without params
    swiftLakeEngine
        .insertInto(tableName)
        .mybatisStatement("TestMapper.getTestData3")
        .sqlSessionFactory(sqlSessionFactory)
        .processSourceTables(false)
        .execute();

    actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(actualData).hasSize(6);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testOverwriteWithFilter() {
    // Create table
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "year", Types.IntegerType.get()),
            Types.NestedField.required(3, "value", Types.StringType.get()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "overwrite_filter_table_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine
        .getCatalog()
        .createTable(tableId, schema, PartitionSpec.builderFor(schema).identity("year").build());
    String tableName = tableId.toString();

    // Insert initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "year", 2020, "value", "A"),
            Map.of("id", 2L, "year", 2020, "value", "B"),
            Map.of("id", 3L, "year", 2021, "value", "C"),
            Map.of("id", 4L, "year", 2021, "value", "D"),
            Map.of("id", 5L, "year", 2022, "value", "E"));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Test overwriteByFilter
    List<Map<String, Object>> newData =
        Arrays.asList(
            Map.of("id", 6L, "year", 2020, "value", "X"),
            Map.of("id", 7L, "year", 2020, "value", "Y"));

    swiftLakeEngine
        .insertOverwrite(tableName)
        .overwriteByFilter(Expressions.equal("year", 2020))
        .sql(TestUtil.createSelectSql(newData, schema))
        .processSourceTables(false)
        .execute();

    // Verify only 2020 data is overwritten
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 6L, "year", 2020, "value", "X"),
            Map.of("id", 7L, "year", 2020, "value", "Y"),
            Map.of("id", 3L, "year", 2021, "value", "C"),
            Map.of("id", 4L, "year", 2021, "value", "D"),
            Map.of("id", 5L, "year", 2022, "value", "E"));

    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testOverwriteWithFilterSql() {
    // Create table
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "year", Types.IntegerType.get()),
            Types.NestedField.required(3, "value", Types.StringType.get()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "overwrite_filter_sql_table_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine
        .getCatalog()
        .createTable(tableId, schema, PartitionSpec.builderFor(schema).identity("year").build());
    String tableName = tableId.toString();

    // Insert initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "year", 2020, "value", "A"),
            Map.of("id", 2L, "year", 2020, "value", "B"),
            Map.of("id", 3L, "year", 2021, "value", "C"),
            Map.of("id", 4L, "year", 2021, "value", "D"),
            Map.of("id", 5L, "year", 2022, "value", "E"));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Test overwriteByFilterSql
    List<Map<String, Object>> newData = Arrays.asList(Map.of("id", 8L, "year", 2021, "value", "Z"));

    swiftLakeEngine
        .insertOverwrite(tableName)
        .overwriteByFilterSql("year = 2021")
        .sql(TestUtil.createSelectSql(newData, schema))
        .processSourceTables(false)
        .execute();

    // Verify only 2021 data is overwritten
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "year", 2020, "value", "A"),
            Map.of("id", 2L, "year", 2020, "value", "B"),
            Map.of("id", 8L, "year", 2021, "value", "Z"),
            Map.of("id", 5L, "year", 2022, "value", "E"));

    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testOverwriteWithFilterColumns() {
    // Create table
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "year", Types.IntegerType.get()),
            Types.NestedField.required(3, "value", Types.StringType.get()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "overwrite_filter_columns_table_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine
        .getCatalog()
        .createTable(tableId, schema, PartitionSpec.builderFor(schema).identity("year").build());
    String tableName = tableId.toString();

    // Insert initial data
    List<Map<String, Object>> initialData =
        Arrays.asList(
            Map.of("id", 1L, "year", 2020, "value", "A"),
            Map.of("id", 2L, "year", 2020, "value", "B"),
            Map.of("id", 3L, "year", 2021, "value", "C"),
            Map.of("id", 4L, "year", 2021, "value", "D"),
            Map.of("id", 5L, "year", 2022, "value", "E"));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(initialData, schema))
        .processSourceTables(false)
        .execute();

    // Test overwriteByFilterColumns
    List<Map<String, Object>> newData = Arrays.asList(Map.of("id", 9L, "year", 2022, "value", "X"));

    swiftLakeEngine
        .insertOverwrite(tableName)
        .overwriteByFilterColumns(Arrays.asList("year"))
        .sql(TestUtil.createSelectSql(newData, schema))
        .processSourceTables(false)
        .execute();

    // Verify only 2022 data is overwritten (since that's the year in newData)
    List<Map<String, Object>> actualData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    List<Map<String, Object>> expectedData =
        Arrays.asList(
            Map.of("id", 1L, "year", 2020, "value", "A"),
            Map.of("id", 2L, "year", 2020, "value", "B"),
            Map.of("id", 3L, "year", 2021, "value", "C"),
            Map.of("id", 4L, "year", 2021, "value", "D"),
            Map.of("id", 9L, "year", 2022, "value", "X"));

    assertThat(actualData)
        .usingRecursiveFieldByFieldElementComparator()
        .containsExactlyInAnyOrderElementsOf(expectedData);

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  public void testOverwriteWithDifferentIsolationLevels() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "year", Types.IntegerType.get()),
            Types.NestedField.required(3, "value", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("year").build();
    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "isolation_test_" + UUID.randomUUID().toString().replace('-', '_'));
    Table table = swiftLakeEngine.getCatalog().createTable(tableId, schema, spec);
    String tableName = tableId.toString();

    List<String> columns = Arrays.asList("id", "year", "value");

    String initialInsertSql = "SELECT 1 AS id, 2024 AS year, 'initial_value' AS value";

    swiftLakeEngine
        .insertInto(tableName)
        .sql(initialInsertSql)
        .columns(columns)
        .processSourceTables(false)
        .execute();

    table.refresh();
    Table tableStateAfterFirstInsert = swiftLakeEngine.getTable(tableName);
    long snapshotIdAfterFirstInsert = tableStateAfterFirstInsert.currentSnapshot().snapshotId();

    String secondInsertSql = "SELECT 2 AS id, 2024 AS year, 'second_value' AS value";

    swiftLakeEngine
        .insertInto(tableName)
        .sql(secondInsertSql)
        .columns(columns)
        .processSourceTables(false)
        .execute();

    Table tableStateAfterSecondInsert = swiftLakeEngine.getTable(tableName);
    Table tableStateAfterSecondInsertCopy = swiftLakeEngine.getTable(tableName);
    assertThat(tableStateAfterSecondInsert.currentSnapshot().snapshotId())
        .isNotEqualTo(snapshotIdAfterFirstInsert);

    // attempting to overwrite with default isolation level should fail due to conflicting files
    String overwriteSql = "SELECT 3 AS id, 2024 AS year, 'overwrite_value' AS value";

    assertThatThrownBy(
            () -> {
              swiftLakeEngine
                  .insertOverwrite(table)
                  .overwriteByFilterSql("year = 2024")
                  .sql(overwriteSql)
                  .columns(columns)
                  .processSourceTables(false)
                  .execute();
            })
        .hasMessageContaining("Found conflicting files that can contain records matching");

    table.refresh();
    assertThat(tableStateAfterFirstInsert.currentSnapshot().snapshotId())
        .isEqualTo(snapshotIdAfterFirstInsert);

    // use SNAPSHOT isolation with the table at first snapshot state
    swiftLakeEngine
        .insertOverwrite(tableStateAfterFirstInsert)
        .overwriteByFilterSql("year = 2024")
        .sql(overwriteSql)
        .columns(columns)
        .isolationLevel(IsolationLevel.SNAPSHOT)
        .processSourceTables(false)
        .execute();

    // the operation should succeed and table should have a new snapshot
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId())
        .isNotEqualTo(tableStateAfterSecondInsert.currentSnapshot().snapshotId());

    // attempting to overwrite with SNAPSHOT isolation on outdated state should fail
    // due to conflicting deleted files
    assertThatThrownBy(
            () -> {
              Insert.overwrite(swiftLakeEngine, tableStateAfterSecondInsert)
                  .overwriteByFilterSql("year = 2024")
                  .sql(overwriteSql)
                  .columns(columns)
                  .isolationLevel(IsolationLevel.SNAPSHOT)
                  .processSourceTables(false)
                  .execute();
            })
        .hasMessageContaining("Found conflicting deleted files");

    // attempting to overwrite with SERIALIZABLE isolation on outdated state should fail
    assertThatThrownBy(
            () -> {
              Insert.overwrite(swiftLakeEngine, tableStateAfterSecondInsertCopy)
                  .overwriteByFilterSql("year = 2024")
                  .sql(overwriteSql)
                  .columns(columns)
                  .isolationLevel(IsolationLevel.SERIALIZABLE)
                  .processSourceTables(false)
                  .execute();
            })
        .hasMessageContaining("Found conflicting files");

    // use SNAPSHOT isolation with current table state
    Insert.overwrite(swiftLakeEngine, tableName)
        .overwriteByFilterSql("year = 2024")
        .sql(overwriteSql)
        .columns(columns)
        .isolationLevel(IsolationLevel.SNAPSHOT)
        .processSourceTables(false)
        .execute();

    // verify we have latest data by checking snapshot ID
    table.refresh();
    long updatedSnapshotId = table.currentSnapshot().snapshotId();

    // Read and verify the data after SNAPSHOT isolation overwrite
    List<Map<String, Object>> dataAfterSnapshotOverwrite =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(dataAfterSnapshotOverwrite)
        .hasSize(1)
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(3L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("value")).isEqualTo("overwrite_value");
            });

    // use SERIALIZABLE isolation with current table state
    String finalOverwriteSql = "SELECT 4 AS id, 2024 AS year, 'final_value' AS value";
    Insert.overwrite(swiftLakeEngine, tableName)
        .overwriteByFilterSql("year = 2024")
        .sql(finalOverwriteSql)
        .columns(columns)
        .isolationLevel(IsolationLevel.SERIALIZABLE)
        .processSourceTables(false)
        .execute();

    // the operation should create a new snapshot
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(updatedSnapshotId);

    // Verify final data after all operations
    List<Map<String, Object>> finalData = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(finalData)
        .hasSize(1)
        .anySatisfy(
            row -> {
              assertThat(row.get("id")).isEqualTo(4L);
              assertThat(row.get("year")).isEqualTo(2024);
              assertThat(row.get("value")).isEqualTo("final_value");
            });

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testInsertWithColumnSubset() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(4, "salary", Types.DoubleType.get()),
            Types.NestedField.optional(5, "department", Types.StringType.get()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "columns_test_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    String sql =
        "SELECT 1 AS id, 'John Doe' AS name, 30 AS age, 75000.00 AS salary, 'Engineering' AS department";

    // First insert with all columns
    swiftLakeEngine.insertInto(tableName).sql(sql).processSourceTables(false).execute();

    // Second insert with only id and name columns
    swiftLakeEngine
        .insertInto(tableName)
        .sql(sql)
        .columns(Arrays.asList("id", "name"))
        .processSourceTables(false)
        .execute();

    // Third insert with only id, age and department columns
    swiftLakeEngine
        .insertInto(tableName)
        .sql(sql)
        .columns(Arrays.asList("id", "age", "department"))
        .processSourceTables(false)
        .execute();

    // Trying to insert with missing required column should fail
    assertThatThrownBy(
            () -> {
              swiftLakeEngine
                  .insertInto(tableName)
                  .sql(sql)
                  .columns(Arrays.asList("name")) // Missing 'id' which is required
                  .processSourceTables(false)
                  .execute();
            })
        .isInstanceOf(com.arcesium.swiftlake.common.ValidationException.class)
        .hasMessageContaining("Required fields cannot contain null values")
        .hasMessageContaining("id");

    // Verify that unspecified columns have null values
    List<Map<String, Object>> results = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    // Should have 3 records
    assertThat(results).hasSize(3);

    // First record should have all values
    assertThat(results)
        .anySatisfy(
            record -> {
              assertThat(record.get("id")).isEqualTo(1L);
              assertThat(record.get("name")).isEqualTo("John Doe");
              assertThat(record.get("age")).isEqualTo(30);
              assertThat(record.get("salary")).isEqualTo(75000.00);
              assertThat(record.get("department")).isEqualTo("Engineering");
            });

    // Second record should have only id and name, with nulls for the rest
    assertThat(results)
        .anySatisfy(
            record -> {
              assertThat(record.get("id")).isEqualTo(1L);
              assertThat(record.get("name")).isEqualTo("John Doe");
              assertThat(record.get("age")).isNull();
              assertThat(record.get("salary")).isNull();
              assertThat(record.get("department")).isNull();
            });

    // Third record should have only id, age and department, with nulls for the rest
    assertThat(results)
        .anySatisfy(
            record -> {
              assertThat(record.get("id")).isEqualTo(1L);
              assertThat(record.get("name")).isNull();
              assertThat(record.get("age")).isEqualTo(30);
              assertThat(record.get("salary")).isNull();
              assertThat(record.get("department")).isEqualTo("Engineering");
            });

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testInsertWithColumnSubsetAndPartitioning() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "year", Types.IntegerType.get()),
            Types.NestedField.optional(3, "name", Types.StringType.get()),
            Types.NestedField.optional(4, "value", Types.DoubleType.get()),
            Types.NestedField.optional(5, "status", Types.StringType.get()));

    // Partition by year
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("year").build();

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db",
            "columns_partitioned_test_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine.getCatalog().createTable(tableId, schema, spec);
    String tableName = tableId.toString();

    String sql =
        "SELECT * FROM (VALUES"
            + "(1, 2020, 'Record 2020-1', 10.5, 'Active'),"
            + "(2, 2021, 'Record 2021-1', 20.5, 'Inactive'),"
            + "(3, 2022, 'Record 2022-1', 30.5, 'Active')"
            + ") AS t(id, year, name, value, status)";

    // Insert with different column subsets
    // First batch - all columns
    swiftLakeEngine.insertInto(tableName).sql(sql).processSourceTables(false).execute();

    // Second batch - only required columns and name
    swiftLakeEngine
        .insertInto(tableName)
        .sql(sql)
        .columns(Arrays.asList("id", "year", "name"))
        .processSourceTables(false)
        .execute();

    // Third batch - required columns and status
    swiftLakeEngine
        .insertInto(tableName)
        .sql(sql)
        .columns(Arrays.asList("id", "year", "status"))
        .processSourceTables(false)
        .execute();

    // Trying to insert with missing required column should fail
    assertThatThrownBy(
            () -> {
              swiftLakeEngine
                  .insertInto(tableName)
                  .sql(sql)
                  .columns(Arrays.asList("id")) // Missing 'year' which is required
                  .processSourceTables(false)
                  .execute();
            })
        .isInstanceOf(com.arcesium.swiftlake.common.ValidationException.class)
        .hasMessageContaining("Required fields cannot contain null values")
        .hasMessageContaining("year");

    // Verify that unspecified columns have null values and partitioning works properly
    List<Map<String, Object>> results = TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);

    // Should have 9 records total (3 batches × 3 records)
    assertThat(results).hasSize(9);

    // Check first batch (all columns)
    assertThat(results)
        .filteredOn(r -> ((Long) r.get("id") == 1L) && r.get("value") != null)
        .hasSize(1)
        .first()
        .satisfies(
            record -> {
              assertThat(record.get("id")).isEqualTo(1L);
              assertThat(record.get("year")).isEqualTo(2020);
              assertThat(record.get("name")).isEqualTo("Record 2020-1");
              assertThat(record.get("value")).isEqualTo(10.5);
              assertThat(record.get("status")).isEqualTo("Active");
            });

    // Check second batch (only id, year, name)
    assertThat(results)
        .filteredOn(
            r -> ((Long) r.get("id") == 2L) && r.get("name") != null && r.get("value") == null)
        .hasSize(1)
        .first()
        .satisfies(
            record -> {
              assertThat(record.get("id")).isEqualTo(2L);
              assertThat(record.get("year")).isEqualTo(2021);
              assertThat(record.get("name")).isEqualTo("Record 2021-1");
              assertThat(record.get("value")).isNull();
              assertThat(record.get("status")).isNull();
            });

    // Check third batch (only id, year, status)
    assertThat(results)
        .filteredOn(
            r -> ((Long) r.get("id") == 3L) && r.get("status") != null && r.get("name") == null)
        .hasSize(1)
        .first()
        .satisfies(
            record -> {
              assertThat(record.get("id")).isEqualTo(3L);
              assertThat(record.get("year")).isEqualTo(2022);
              assertThat(record.get("name")).isNull();
              assertThat(record.get("value")).isNull();
              assertThat(record.get("status")).isEqualTo("Active");
            });

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSkipDataSorting(boolean isPartitioned) throws Exception {
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
            "test_db", "skip_data_sorting_test_" + UUID.randomUUID().toString().replace('-', '_'));
    swiftLakeEngine
        .getCatalog()
        .buildTable(tableId, schema)
        .withPartitionSpec(partitionSpec)
        .withSortOrder(sortOrder)
        .create();
    String tableName = tableId.toString();

    List<Map<String, Object>> testRecords = new ArrayList<>();
    for (int i = 100; i > 0; i--) {
      Map<String, Object> record = new HashMap<>();
      record.put("id", i);
      record.put("sort_key", i);
      record.put("value", "value_" + String.format("%03d", i));
      testRecords.add(record);
    }

    // Create SQL for inserting the test data
    StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM (VALUES ");
    String valuesSql =
        testRecords.stream()
            .map(
                r ->
                    String.format("(%d, %d, '%s')", r.get("id"), r.get("sort_key"), r.get("value")))
            .collect(Collectors.joining(","));
    sqlBuilder.append(valuesSql);
    sqlBuilder.append(") AS t(id, sort_key, value)");

    // Create a sorted copy of the test records for comparison
    List<Map<String, Object>> expectedSortedRecords = new ArrayList<>(testRecords);
    expectedSortedRecords.sort(Comparator.comparing(m -> (Integer) m.get("sort_key")));

    // Insert data with skipDataSorting=true
    swiftLakeEngine
        .insertInto(tableName)
        .sql(sqlBuilder.toString())
        .skipDataSorting(true)
        .processSourceTables(false)
        .execute();

    // Read the data in file order and compare with our expectations
    List<Map<String, Object>> actualRecordsWithSkipSorting =
        TestUtil.getRecordsFromTableInFileOrder(swiftLakeEngine, tableName);
    boolean isUnsortedWithSkip =
        !TestUtil.compareDataInOrder(actualRecordsWithSkipSorting, expectedSortedRecords);
    assertThat(isUnsortedWithSkip).isTrue();

    // Drop and recreate table for the second test
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
    swiftLakeEngine
        .getCatalog()
        .buildTable(tableId, schema)
        .withPartitionSpec(partitionSpec)
        .withSortOrder(sortOrder)
        .create();

    // Insert data with skipDataSorting=false
    swiftLakeEngine
        .insertInto(tableName)
        .sql(sqlBuilder.toString())
        .skipDataSorting(false)
        .processSourceTables(false)
        .execute();

    // Read the data in file order and compare with our expectations
    List<Map<String, Object>> actualRecordsWithoutSkipSorting =
        TestUtil.getRecordsFromTableInFileOrder(swiftLakeEngine, tableName);
    boolean isSortedWithoutSkip =
        TestUtil.compareDataInOrder(actualRecordsWithoutSkipSorting, expectedSortedRecords);

    // First, check that the records match regardless of order
    assertThat(actualRecordsWithoutSkipSorting)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("file_name", "file_row_number")
        .containsExactlyInAnyOrderElementsOf(testRecords)
        .as("All records should be present in the table");

    // With skipDataSorting=false, the data should match exactly the sorted order
    assertThat(isSortedWithoutSkip).isTrue();

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testBranchOperations() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "branch_test_" + UUID.randomUUID().toString().replace('-', '_'));
    Table table =
        swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    // Insert initial data into the main branch
    swiftLakeEngine
        .insertInto(tableName)
        .sql("SELECT 1 AS id, 'main-value-1' AS value")
        .processSourceTables(false)
        .execute();

    // Creating a new branch and inserting data specifically into that branch
    String testBranch = "test-branch";
    table.manageSnapshots().createBranch(testBranch).commit();

    swiftLakeEngine
        .insertInto(tableName)
        .sql("SELECT 2 AS id, 'branch-value-1' AS value")
        .branch(testBranch)
        .processSourceTables(false)
        .execute();

    // Insert more data into main branch
    swiftLakeEngine
        .insertInto(tableName)
        .sql("SELECT 3 AS id, 'main-value-2' AS value")
        .processSourceTables(false)
        .execute();

    // Verify data in the main branch
    List<Map<String, Object>> mainResults =
        TestUtil.getRecordsFromTable(swiftLakeEngine, tableName);
    assertThat(mainResults)
        .hasSize(2)
        .extracting("id", "value")
        .containsExactlyInAnyOrder(tuple(1L, "main-value-1"), tuple(3L, "main-value-2"));

    // Verify data in the test branch
    List<Map<String, Object>> branchResults =
        TestUtil.getRecordsFromTable(
            swiftLakeEngine, TestUtil.getTableNameForBranch(tableId, testBranch));
    assertThat(branchResults)
        .hasSize(2)
        .extracting("id", "value")
        .containsExactlyInAnyOrder(tuple(1L, "main-value-1"), tuple(2L, "branch-value-1"));

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testSnapshotMetadata() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()));

    TableIdentifier tableId =
        TableIdentifier.of(
            "test_db", "snapshot_metadata_test_" + UUID.randomUUID().toString().replace('-', '_'));
    Table table =
        swiftLakeEngine.getCatalog().createTable(tableId, schema, PartitionSpec.unpartitioned());
    String tableName = tableId.toString();

    // Insert data with snapshot metadata
    Map<String, String> metadata = new HashMap<>();
    metadata.put("user", "test-user");
    metadata.put("source", "test-system");
    metadata.put("environment", "test");
    metadata.put("description", "This is a test snapshot with metadata");

    swiftLakeEngine
        .insertInto(tableName)
        .sql("SELECT 1 AS id, 'test-value' AS value")
        .snapshotMetadata(metadata)
        .processSourceTables(false)
        .execute();

    // Verify the snapshot contains our custom metadata
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();

    Map<String, String> snapshotMetadata = snapshot.summary();
    assertThat(snapshotMetadata)
        .containsEntry("user", "test-user")
        .containsEntry("source", "test-system")
        .containsEntry("environment", "test")
        .containsEntry("description", "This is a test snapshot with metadata");

    // Insert more data with different metadata
    Map<String, String> metadata2 = new HashMap<>();
    metadata2.put("user", "another-user");
    metadata2.put("date", LocalDate.now().toString());

    swiftLakeEngine
        .insertInto(tableName)
        .sql("SELECT 2 AS id, 'updated-value' AS value")
        .snapshotMetadata(metadata2)
        .processSourceTables(false)
        .execute();

    // Verify the new snapshot has the updated metadata
    table.refresh();
    Snapshot newSnapshot = table.currentSnapshot();
    assertThat(newSnapshot.snapshotId()).isNotEqualTo(snapshot.snapshotId());

    Map<String, String> newMetadata = newSnapshot.summary();
    assertThat(newMetadata).containsEntry("user", "another-user").containsKey("date");

    // Clean up
    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }
}
