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
package com.arcesium.swiftlake.writer;

import static org.assertj.core.api.Assertions.assertThat;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.TestUtil;
import com.arcesium.swiftlake.commands.WriteUtil;
import com.arcesium.swiftlake.metrics.CommitMetrics;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PartitioningIntegrationTest {
  private static SwiftLakeEngine swiftLakeEngine;
  private static final String SCHEMA_NAME = "partitioning_test_schema";
  @TempDir private static Path basePath;
  private static String longString;

  @BeforeAll
  public static void setup() {
    swiftLakeEngine = TestUtil.createSwiftLakeEngine(basePath.toString());
    longString = RandomStringUtils.secure().next(2000);
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

  // Test cases for Boolean partitioning
  static Stream<Arguments> provideBooleanTestCases() {
    return Stream.of(
        Arguments.of(true, "boolean_partition = true"),
        Arguments.of(false, "boolean_partition = false"),
        Arguments.of(null, "boolean_partition IS NULL"));
  }

  @ParameterizedTest(name = "Boolean partitioning: {0}")
  @MethodSource("provideBooleanTestCases")
  @DisplayName("Test Boolean partitioning")
  void testBooleanPartitioning(Boolean value, String whereClause) {
    String tableName = SCHEMA_NAME + ".boolean_part_table";

    // Create and populate test table
    Table table = createPartitionedTable(tableName, "boolean_partition", Types.BooleanType.get());
    insertBooleanTestData(table);

    // Query with partition filter
    List<Map<String, Object>> results =
        executeQuery("SELECT * FROM " + tableName + " WHERE " + whereClause);

    // Assert correct record is returned
    assertThat(results).hasSize(1);
    if (value != null) {
      assertThat(results.get(0).get("boolean_partition")).isEqualTo(value);
    } else {
      assertThat(results.get(0).get("boolean_partition")).isNull();
    }
    assertThat(results.get(0).get("id")).isNotNull();
  }

  // Test cases for Integer partitioning
  static Stream<Arguments> provideIntegerTestCases() {
    return Stream.of(
        Arguments.of(0, "integer_partition = 0"),
        Arguments.of(0, "integer_partition = 0::INTEGER"),
        Arguments.of(1, "integer_partition = 1::INT"),
        Arguments.of(Integer.MAX_VALUE, "integer_partition = " + Integer.MAX_VALUE + "::INT32"),
        Arguments.of(
            Integer.MIN_VALUE, "integer_partition = CAST(" + Integer.MIN_VALUE + " AS INT4)"),
        Arguments.of(null, "integer_partition IS NULL"));
  }

  @ParameterizedTest(name = "Integer partitioning: {0}")
  @MethodSource("provideIntegerTestCases")
  @DisplayName("Test Integer partitioning")
  void testIntegerPartitioning(Integer value, String whereClause) {
    String tableName = SCHEMA_NAME + ".integer_part_table";

    // Create and populate test table
    Table table = createPartitionedTable(tableName, "integer_partition", Types.IntegerType.get());
    insertIntegerTestData(table);

    // Query with partition filter
    List<Map<String, Object>> results =
        executeQuery("SELECT * FROM " + tableName + " WHERE " + whereClause);

    // Assert correct record is returned
    assertThat(results).hasSize(1);
    if (value != null) {
      assertThat(results.get(0).get("integer_partition")).isEqualTo(value);
    } else {
      assertThat(results.get(0).get("integer_partition")).isNull();
    }
    assertThat(results.get(0).get("id")).isNotNull();
  }

  // Test cases for Long partitioning
  static Stream<Arguments> provideLongTestCases() {
    return Stream.of(
        Arguments.of(0L, "long_partition = 0"),
        Arguments.of(0L, "long_partition = 0::LONG"),
        Arguments.of(1L, "long_partition = 1::BIGINT"),
        Arguments.of(Long.MAX_VALUE, "long_partition = " + Long.MAX_VALUE + "::INT64"),
        Arguments.of(Long.MIN_VALUE, "long_partition = CAST(" + Long.MIN_VALUE + " AS INT8)"),
        Arguments.of(null, "long_partition IS NULL"));
  }

  @ParameterizedTest(name = "Long partitioning: {0}")
  @MethodSource("provideLongTestCases")
  @DisplayName("Test Long partitioning")
  void testLongPartitioning(Long value, String whereClause) {
    String tableName = SCHEMA_NAME + ".long_part_table";

    // Create and populate test table
    Table table = createPartitionedTable(tableName, "long_partition", Types.LongType.get());
    insertLongTestData(table);

    // Query with partition filter
    List<Map<String, Object>> results =
        executeQuery("SELECT * FROM " + tableName + " WHERE " + whereClause);

    // Assert correct record is returned
    assertThat(results).hasSize(1);
    if (value != null) {
      assertThat(results.get(0).get("long_partition")).isEqualTo(value);
    } else {
      assertThat(results.get(0).get("long_partition")).isNull();
    }
    assertThat(results.get(0).get("id")).isNotNull();
  }

  // Test cases for Float partitioning
  static Stream<Arguments> provideFloatTestCases() {
    return Stream.of(
        Arguments.of(0.0f, "float_partition = 0.0"),
        Arguments.of(1.5f, "float_partition = 1.5::FLOAT"),
        Arguments.of(-1.5f, "float_partition = -1.5::FLOAT4"),
        Arguments.of(-1.5f, "float_partition = CAST (-1.5 AS REAL)"),
        Arguments.of(Float.MAX_VALUE, "float_partition = " + Float.MAX_VALUE + "::FLOAT"),
        Arguments.of(Float.MIN_VALUE, "float_partition = " + Float.MIN_VALUE + "::FLOAT"),
        Arguments.of(Float.POSITIVE_INFINITY, "float_partition = 'inf'"),
        Arguments.of(Float.NEGATIVE_INFINITY, "float_partition = '-inf'"),
        Arguments.of(Float.NaN, "float_partition = 'nan'"),
        Arguments.of(1.0e38f, "float_partition = 1.0e38::FLOAT"),
        Arguments.of(1.0e-38f, "float_partition = 1.0e-38::FLOAT"),
        Arguments.of(null, "float_partition IS NULL"));
  }

  @ParameterizedTest(name = "Float partitioning: {0}")
  @MethodSource("provideFloatTestCases")
  @DisplayName("Test Float partitioning")
  void testFloatPartitioning(Float value, String whereClause) {
    String tableName = SCHEMA_NAME + ".float_part_table";

    // Create and populate test table
    Table table = createPartitionedTable(tableName, "float_partition", Types.FloatType.get());
    insertFloatTestData(table);

    // Query with partition filter
    List<Map<String, Object>> results =
        executeQuery("SELECT * FROM " + tableName + " WHERE " + whereClause);

    // Assert correct record is returned
    assertThat(results).hasSize(1);
    if (value != null) {
      assertThat((Float) results.get(0).get("float_partition")).isEqualTo(value);
    } else {
      assertThat(results.get(0).get("float_partition")).isNull();
    }
    assertThat(results.get(0).get("id")).isNotNull();
  }

  // Test cases for Double partitioning
  static Stream<Arguments> provideDoubleTestCases() {
    return Stream.of(
        Arguments.of(0.0, "double_partition = 0.0"),
        Arguments.of(1.5, "double_partition = 1.5::FLOAT8"),
        Arguments.of(-1.5, "double_partition = -1.5::DOUBLE"),
        Arguments.of(Double.MAX_VALUE, "double_partition = " + Double.MAX_VALUE),
        Arguments.of(Double.MIN_VALUE, "double_partition = " + Double.MIN_VALUE),
        Arguments.of(Double.POSITIVE_INFINITY, "double_partition = 'inf'"),
        Arguments.of(Double.NEGATIVE_INFINITY, "double_partition = '-inf'"),
        Arguments.of(Double.NaN, "double_partition = 'nan'"),
        Arguments.of(1.0e308, "double_partition = 1.0e308"),
        Arguments.of(1.0e-308, "double_partition = 1.0e-308"),
        Arguments.of(null, "double_partition IS NULL"));
  }

  @ParameterizedTest(name = "Double partitioning: {0}")
  @MethodSource("provideDoubleTestCases")
  @DisplayName("Test Double partitioning")
  void testDoublePartitioning(Double value, String whereClause) {
    String tableName = SCHEMA_NAME + ".double_part_table";

    // Create and populate test table
    Table table = createPartitionedTable(tableName, "double_partition", Types.DoubleType.get());
    insertDoubleTestData(table);

    // Query with partition filter
    List<Map<String, Object>> results =
        executeQuery("SELECT * FROM " + tableName + " WHERE " + whereClause);

    // Assert correct record is returned
    assertThat(results).hasSize(1);
    if (value != null) {
      assertThat((Double) results.get(0).get("double_partition")).isEqualTo(value);
    } else {
      assertThat(results.get(0).get("double_partition")).isNull();
    }
    assertThat(results.get(0).get("id")).isNotNull();
  }

  // Test cases for Decimal partitioning
  static Stream<Arguments> provideDecimalTestCases() {
    return Stream.of(
        Arguments.of(BigDecimal.ZERO.setScale(2), "decimal_partition = 0.00"),
        Arguments.of(new BigDecimal("1.50"), "decimal_partition = 1.50"),
        Arguments.of(new BigDecimal("-1.50"), "decimal_partition = -1.50"),
        Arguments.of(new BigDecimal("9999.99"), "decimal_partition = 9999.99"),
        Arguments.of(new BigDecimal("-9999.99"), "decimal_partition = -9999.99"),
        Arguments.of(new BigDecimal("0.01"), "decimal_partition = 0.01"),
        Arguments.of(null, "decimal_partition IS NULL"));
  }

  @ParameterizedTest(name = "Decimal partitioning: {0}")
  @MethodSource("provideDecimalTestCases")
  @DisplayName("Test Decimal partitioning")
  void testDecimalPartitioning(BigDecimal value, String whereClause) {
    String tableName = SCHEMA_NAME + ".decimal_part_table";

    // Create and populate test table
    Table table =
        createPartitionedTable(tableName, "decimal_partition", Types.DecimalType.of(6, 2));
    insertDecimalTestData(table);

    // Query with partition filter
    List<Map<String, Object>> results =
        executeQuery("SELECT * FROM " + tableName + " WHERE " + whereClause);

    // Assert correct record is returned
    assertThat(results).hasSize(1);
    if (value != null) {
      assertThat(results.get(0).get("decimal_partition")).isEqualTo(value);
    } else {
      assertThat(results.get(0).get("decimal_partition")).isNull();
    }
    assertThat(results.get(0).get("id")).isNotNull();
  }

  // Test cases for String partitioning
  static Stream<Arguments> provideStringTestCases() {
    return Stream.of(
        Arguments.of("regular", "string_partition = 'regular'"),
        Arguments.of("", "string_partition = ''"),
        Arguments.of("null", "string_partition = 'null'"),
        Arguments.of("NULL", "string_partition = 'NULL'"),
        Arguments.of("Special: !@#$%^&*()_+", "string_partition = 'Special: !@#$%^&*()_+'"),
        Arguments.of(longString, "string_partition = '" + longString.replace("'", "''") + "'"),
        Arguments.of(null, "string_partition IS NULL"));
  }

  @ParameterizedTest(name = "String partitioning: {0}")
  @MethodSource("provideStringTestCases")
  @DisplayName("Test String partitioning")
  void testStringPartitioning(String value, String whereClause) {
    String tableName = SCHEMA_NAME + ".string_part_table";

    // Create and populate test table
    Table table =
        createPartitionedTable(
            tableName,
            "string_partition",
            Types.StringType.get(),
            Map.of("write.location-provider.impl", UUIDBasedLocationProvider.class.getName()));
    insertStringTestData(table);

    // Query with partition filter
    List<Map<String, Object>> results =
        executeQuery("SELECT * FROM " + tableName + " WHERE " + whereClause);

    // Assert correct record is returned
    assertThat(results).hasSize(1);
    if (value != null) {
      assertThat(results.get(0).get("string_partition")).isEqualTo(value);
    } else {
      assertThat(results.get(0).get("string_partition")).isNull();
    }
    assertThat(results.get(0).get("id")).isNotNull();
  }

  // Test cases for Date partitioning
  static Stream<Arguments> provideDateTestCases() {
    return Stream.of(
        Arguments.of(LocalDate.of(2025, 1, 1), "date_partition = DATE '2025-01-01'"),
        Arguments.of(LocalDate.of(1970, 1, 1), "date_partition = DATE '1970-01-01'"),
        Arguments.of(LocalDate.of(2100, 12, 31), "date_partition = DATE '2100-12-31'"),
        Arguments.of(LocalDate.of(1900, 1, 1), "date_partition = DATE '1900-01-01'"),
        Arguments.of(null, "date_partition IS NULL"));
  }

  @ParameterizedTest(name = "Date partitioning: {0}")
  @MethodSource("provideDateTestCases")
  @DisplayName("Test Date partitioning")
  void testDatePartitioning(LocalDate value, String whereClause) {
    String tableName = SCHEMA_NAME + ".date_part_table";

    // Create and populate test table
    Table table = createPartitionedTable(tableName, "date_partition", Types.DateType.get());
    insertDateTestData(table);

    // Query with partition filter
    List<Map<String, Object>> results =
        executeQuery("SELECT * FROM " + tableName + " WHERE " + whereClause);

    // Assert correct record is returned
    assertThat(results).hasSize(1);
    if (value != null) {
      assertThat(results.get(0).get("date_partition")).isEqualTo(value);
    } else {
      assertThat(results.get(0).get("date_partition")).isNull();
    }
    assertThat(results.get(0).get("id")).isNotNull();
  }

  // Test cases for Time partitioning
  static Stream<Arguments> provideTimeTestCases() {
    return Stream.of(
        Arguments.of(LocalTime.of(0, 0, 0, 0), "time_partition = TIME '00:00:00'"),
        Arguments.of(
            LocalTime.of(12, 30, 45, 123456000), "time_partition = TIME '12:30:45.123456'"),
        Arguments.of(
            LocalTime.of(23, 59, 59, 999999000), "time_partition = TIME '23:59:59.999999'"),
        Arguments.of(LocalTime.of(6, 0, 0, 500000000), "time_partition = TIME '06:00:00.5'"),
        Arguments.of(LocalTime.of(18, 0, 0, 1000), "time_partition = TIME '18:00:00.000001'"),
        Arguments.of(null, "time_partition IS NULL"));
  }

  @ParameterizedTest(name = "Time partitioning: {0}")
  @MethodSource("provideTimeTestCases")
  @DisplayName("Test Time partitioning")
  void testTimePartitioning(LocalTime value, String whereClause) {
    String tableName = SCHEMA_NAME + ".time_part_table";

    // Create and populate test table
    Table table = createPartitionedTable(tableName, "time_partition", Types.TimeType.get());
    insertTimeTestData(table);

    // Query with partition filter
    List<Map<String, Object>> results =
        executeQuery("SELECT * FROM " + tableName + " WHERE " + whereClause);

    // Assert correct record is returned
    assertThat(results).hasSize(1);
    if (value != null) {
      assertThat(results.get(0).get("time_partition")).isEqualTo(value);
    } else {
      assertThat(results.get(0).get("time_partition")).isNull();
    }
    assertThat(results.get(0).get("id")).isNotNull();
  }

  // Test cases for Timestamp partitioning
  static Stream<Arguments> provideTimestampTestCases() {
    return Stream.of(
        Arguments.of(
            LocalDateTime.of(2025, 1, 1, 0, 0, 0, 0),
            "timestamp_partition = TIMESTAMP '2025-01-01T00:00:00'"),
        Arguments.of(
            LocalDateTime.of(2025, 1, 1, 12, 30, 45, 123456000),
            "timestamp_partition = TIMESTAMP '2025-01-01T12:30:45.123456'"),
        Arguments.of(
            LocalDateTime.of(1970, 1, 1, 0, 0, 0, 500000000),
            "timestamp_partition = TIMESTAMP '1970-01-01T00:00:00.5'"),
        Arguments.of(
            LocalDateTime.of(2100, 12, 31, 23, 59, 59, 999999000),
            "timestamp_partition = TIMESTAMP '2100-12-31T23:59:59.999999'"),
        Arguments.of(null, "timestamp_partition IS NULL"));
  }

  @ParameterizedTest(name = "Timestamp partitioning: {0}")
  @MethodSource("provideTimestampTestCases")
  @DisplayName("Test Timestamp partitioning")
  void testTimestampPartitioning(LocalDateTime value, String whereClause) {
    String tableName = SCHEMA_NAME + ".timestamp_part_table";

    // Create and populate test table
    Table table =
        createPartitionedTable(tableName, "timestamp_partition", Types.TimestampType.withoutZone());
    insertTimestampTestData(table);

    // Query with partition filter
    List<Map<String, Object>> results =
        executeQuery("SELECT * FROM " + tableName + " WHERE " + whereClause);

    // Assert correct record is returned
    assertThat(results).hasSize(1);
    if (value != null) {
      assertThat(results.get(0).get("timestamp_partition")).isEqualTo(value);
    } else {
      assertThat(results.get(0).get("timestamp_partition")).isNull();
    }
    assertThat(results.get(0).get("id")).isNotNull();
  }

  // Test cases for TimestampTZ partitioning
  static Stream<Arguments> provideTimestampTZTestCases() {
    return Stream.of(
        Arguments.of(
            OffsetDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
            "timestamptz_partition = TIMESTAMPTZ '2025-01-01T00:00:00+00:00'"),
        Arguments.of(
            OffsetDateTime.of(2025, 1, 1, 12, 30, 45, 123456000, ZoneOffset.ofHours(5)),
            "timestamptz_partition = TIMESTAMPTZ '2025-01-01T12:30:45.123456+05:00'"),
        Arguments.of(
            OffsetDateTime.of(2025, 1, 1, 12, 30, 45, 500000000, ZoneOffset.ofHours(-5)),
            "timestamptz_partition = TIMESTAMPTZ '2025-01-01T12:30:45.5-05:00'"),
        Arguments.of(
            OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 999999000, ZoneOffset.UTC),
            "timestamptz_partition = TIMESTAMPTZ '1970-01-01T00:00:00.999999+00:00'"),
        Arguments.of(null, "timestamptz_partition IS NULL"));
  }

  @ParameterizedTest(name = "TimestampTZ partitioning: {0}")
  @MethodSource("provideTimestampTZTestCases")
  @DisplayName("Test TimestampTZ partitioning")
  void testTimestampTZPartitioning(OffsetDateTime value, String whereClause) {
    String tableName = SCHEMA_NAME + ".timestamptz_part_table";

    // Create and populate test table
    Table table =
        createPartitionedTable(tableName, "timestamptz_partition", Types.TimestampType.withZone());
    insertTimestampTZTestData(table);

    // Query with partition filter
    List<Map<String, Object>> results =
        executeQuery("SELECT * FROM " + tableName + " WHERE " + whereClause);

    // Assert correct record is returned
    assertThat(results).hasSize(1);
    if (value != null) {
      // Compare with instant to handle time zone conversions
      OffsetDateTime actual = (OffsetDateTime) results.get(0).get("timestamptz_partition");
      assertThat(actual.toInstant()).isEqualTo(value.toInstant());
    } else {
      assertThat(results.get(0).get("timestamptz_partition")).isNull();
    }
    assertThat(results.get(0).get("id")).isNotNull();
  }

  private Table createPartitionedTable(
      String tableName, String partitionColumn, Type partitionType) {
    return createPartitionedTable(tableName, partitionColumn, partitionType, null);
  }

  private Table createPartitionedTable(
      String tableName,
      String partitionColumn,
      Type partitionType,
      Map<String, String> properties) {
    // Check if table exists and drop it
    TableIdentifier tableId = TableIdentifier.parse(tableName);
    if (swiftLakeEngine.getCatalog().tableExists(tableId)) {
      swiftLakeEngine.getCatalog().dropTable(tableId);
    }

    // Create schema with ID column and partition column
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, partitionColumn, partitionType));
    // Create partitioned table
    return swiftLakeEngine
        .getCatalog()
        .createTable(
            tableId,
            schema,
            PartitionSpec.builderFor(schema).identity(partitionColumn).build(),
            properties);
  }

  private List<Map<String, Object>> executeQuery(String sql) {
    return TestUtil.executeQuery(swiftLakeEngine, sql);
  }

  private void insertBooleanTestData(Table table) {
    List<Map<String, Object>> data = new ArrayList<>();
    long id = 1;

    // Add test data for each test case
    List<Boolean> values = Arrays.asList(true, false, null);
    for (Boolean value : values) {
      Map<String, Object> record = new HashMap<>();
      record.put("id", id++);
      record.put("boolean_partition", value);
      data.add(record);
    }

    // Insert data
    CommitMetrics commitMetrics = insertTestData(table, data);
    assertThat(commitMetrics).isNotNull();
    assertThat(commitMetrics.getAddedFilesCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getAddedRecordsCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getPartitionCommitMetrics()).hasSize(values.size());
    assertThat(
            commitMetrics.getPartitionCommitMetrics().stream()
                .map(p -> p.getPartitionData().getPartitionValues().get(0).getValue())
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(values);
  }

  private void insertIntegerTestData(Table table) {
    List<Map<String, Object>> data = new ArrayList<>();
    long id = 1;

    // Add test data for each test case
    List<Integer> values = Arrays.asList(0, 1, Integer.MAX_VALUE, Integer.MIN_VALUE, null);
    for (Integer value : values) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", id++);
      row.put("integer_partition", value);
      data.add(row);
    }

    // Insert data
    CommitMetrics commitMetrics = insertTestData(table, data);
    assertThat(commitMetrics).isNotNull();
    assertThat(commitMetrics.getAddedFilesCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getAddedRecordsCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getPartitionCommitMetrics()).hasSize(values.size());
    assertThat(
            commitMetrics.getPartitionCommitMetrics().stream()
                .map(p -> p.getPartitionData().getPartitionValues().get(0).getValue())
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(values);
  }

  private void insertLongTestData(Table table) {
    List<Map<String, Object>> data = new ArrayList<>();
    long id = 1;

    // Add test data for each test case
    List<Long> values = Arrays.asList(0L, 1L, Long.MAX_VALUE, Long.MIN_VALUE, null);
    for (Long value : values) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", id++);
      row.put("long_partition", value);
      data.add(row);
    }

    // Insert data
    CommitMetrics commitMetrics = insertTestData(table, data);
    assertThat(commitMetrics).isNotNull();
    assertThat(commitMetrics.getAddedFilesCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getAddedRecordsCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getPartitionCommitMetrics()).hasSize(values.size());
    assertThat(
            commitMetrics.getPartitionCommitMetrics().stream()
                .map(p -> p.getPartitionData().getPartitionValues().get(0).getValue())
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(values);
  }

  private void insertFloatTestData(Table table) {
    List<Map<String, Object>> data = new ArrayList<>();
    long id = 1;

    // Add test data for each test case
    List<Float> values =
        Arrays.asList(
            0.0f,
            1.5f,
            -1.5f,
            Float.MAX_VALUE,
            Float.MIN_VALUE,
            Float.POSITIVE_INFINITY,
            Float.NEGATIVE_INFINITY,
            Float.NaN,
            1.0e38f,
            1.0e-38f,
            null);
    for (Float value : values) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", id++);
      row.put("float_partition", value);
      data.add(row);
    }

    // Insert data
    CommitMetrics commitMetrics = insertTestData(table, data);
    assertThat(commitMetrics).isNotNull();
    assertThat(commitMetrics.getAddedFilesCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getAddedRecordsCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getPartitionCommitMetrics()).hasSize(values.size());
    assertThat(
            commitMetrics.getPartitionCommitMetrics().stream()
                .map(p -> p.getPartitionData().getPartitionValues().get(0).getValue())
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(values);
  }

  private void insertDoubleTestData(Table table) {
    List<Map<String, Object>> data = new ArrayList<>();
    long id = 1;

    // Add test data for each test case
    List<Double> values =
        Arrays.asList(
            0.0,
            1.5,
            -1.5,
            Double.MAX_VALUE,
            Double.MIN_VALUE,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY,
            Double.NaN,
            1.0e308,
            1.0e-308,
            null);
    for (Double value : values) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", id++);
      row.put("double_partition", value);
      data.add(row);
    }

    // Insert data
    CommitMetrics commitMetrics = insertTestData(table, data);
    assertThat(commitMetrics).isNotNull();
    assertThat(commitMetrics.getAddedFilesCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getAddedRecordsCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getPartitionCommitMetrics()).hasSize(values.size());
    assertThat(
            commitMetrics.getPartitionCommitMetrics().stream()
                .map(p -> p.getPartitionData().getPartitionValues().get(0).getValue())
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(values);
  }

  private void insertDecimalTestData(Table table) {
    List<Map<String, Object>> data = new ArrayList<>();
    long id = 1;

    // Add test data for each test case
    List<BigDecimal> values =
        Arrays.asList(
            BigDecimal.ZERO.setScale(2),
            new BigDecimal("1.50"),
            new BigDecimal("-1.50"),
            new BigDecimal("9999.99"),
            new BigDecimal("-9999.99"),
            new BigDecimal("0.01"),
            null);

    for (BigDecimal value : values) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", id++);
      row.put("decimal_partition", value);
      data.add(row);
    }

    // Insert data
    CommitMetrics commitMetrics = insertTestData(table, data);
    assertThat(commitMetrics).isNotNull();
    assertThat(commitMetrics.getAddedFilesCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getAddedRecordsCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getPartitionCommitMetrics()).hasSize(values.size());
    assertThat(
            commitMetrics.getPartitionCommitMetrics().stream()
                .map(p -> p.getPartitionData().getPartitionValues().get(0).getValue())
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(values);
  }

  private void insertStringTestData(Table table) {
    List<Map<String, Object>> data = new ArrayList<>();
    long id = 1;

    // Add test data for each test case
    List<String> values =
        Arrays.asList("regular", "", "null", "NULL", "Special: !@#$%^&*()_+", longString, null);

    for (String value : values) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", id++);
      row.put("string_partition", value);
      data.add(row);
    }

    // Insert data
    CommitMetrics commitMetrics = insertTestData(table, data);
    assertThat(commitMetrics).isNotNull();
    assertThat(commitMetrics.getAddedFilesCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getAddedRecordsCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getPartitionCommitMetrics()).hasSize(values.size());
    assertThat(
            commitMetrics.getPartitionCommitMetrics().stream()
                .map(p -> p.getPartitionData().getPartitionValues().get(0).getValue())
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(values);
  }

  private void insertDateTestData(Table table) {
    List<Map<String, Object>> data = new ArrayList<>();
    long id = 1;

    // Add test data for each test case
    List<LocalDate> values =
        Arrays.asList(
            LocalDate.of(2025, 1, 1),
            LocalDate.of(1970, 1, 1),
            LocalDate.of(2100, 12, 31),
            LocalDate.of(1900, 1, 1),
            null);

    for (LocalDate value : values) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", id++);
      row.put("date_partition", value);
      data.add(row);
    }

    // Insert data
    CommitMetrics commitMetrics = insertTestData(table, data);
    assertThat(commitMetrics).isNotNull();
    assertThat(commitMetrics.getAddedFilesCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getAddedRecordsCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getPartitionCommitMetrics()).hasSize(values.size());
    assertThat(
            commitMetrics.getPartitionCommitMetrics().stream()
                .map(p -> p.getPartitionData().getPartitionValues().get(0).getValue())
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(
            values.stream()
                .map(
                    v ->
                        v == null
                            ? null
                            : Literal.of(
                                    WriteUtil.getFormattedValueForDateTimes(
                                        Types.DateType.get(), v))
                                .to(Types.DateType.get())
                                .value())
                .collect(Collectors.toList()));
  }

  private void insertTimeTestData(Table table) {
    List<Map<String, Object>> data = new ArrayList<>();
    long id = 1;

    // Add test data for each test case
    List<LocalTime> values =
        Arrays.asList(
            LocalTime.of(0, 0, 0, 0), // 00:00:00
            LocalTime.of(12, 30, 45, 123456000), // 12:30:45.123456
            LocalTime.of(23, 59, 59, 999999000), // 23:59:59.999999
            LocalTime.of(6, 0, 0, 500000000), // 06:00:00.5
            LocalTime.of(18, 0, 0, 1000), // 18:00:00.000001
            null);

    for (LocalTime value : values) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", id++);
      row.put("time_partition", value);
      data.add(row);
    }

    // Insert data
    CommitMetrics commitMetrics = insertTestData(table, data);
    assertThat(commitMetrics).isNotNull();
    assertThat(commitMetrics.getAddedFilesCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getAddedRecordsCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getPartitionCommitMetrics()).hasSize(values.size());
    assertThat(
            commitMetrics.getPartitionCommitMetrics().stream()
                .map(p -> p.getPartitionData().getPartitionValues().get(0).getValue())
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(
            values.stream()
                .map(
                    v ->
                        v == null
                            ? null
                            : Literal.of(
                                    WriteUtil.getFormattedValueForDateTimes(
                                        Types.TimeType.get(), v))
                                .to(Types.TimeType.get())
                                .value())
                .collect(Collectors.toList()));
  }

  private void insertTimestampTestData(Table table) {
    List<Map<String, Object>> data = new ArrayList<>();
    long id = 1;

    // Add test data for each test case
    List<LocalDateTime> values =
        Arrays.asList(
            LocalDateTime.of(2025, 1, 1, 0, 0, 0, 0), // 2025-01-01 00:00:00
            LocalDateTime.of(2025, 1, 1, 12, 30, 45, 123456000), // 2025-01-01 12:30:45.123456
            LocalDateTime.of(1970, 1, 1, 0, 0, 0, 500000000), // 1970-01-01 00:00:00.5
            LocalDateTime.of(2100, 12, 31, 23, 59, 59, 999999000), // 2100-12-31 23:59:59.999999
            null);

    for (LocalDateTime value : values) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", id++);
      row.put("timestamp_partition", value);
      data.add(row);
    }

    // Insert data
    CommitMetrics commitMetrics = insertTestData(table, data);
    assertThat(commitMetrics).isNotNull();
    assertThat(commitMetrics.getAddedFilesCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getAddedRecordsCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getPartitionCommitMetrics()).hasSize(values.size());
    assertThat(
            commitMetrics.getPartitionCommitMetrics().stream()
                .map(p -> p.getPartitionData().getPartitionValues().get(0).getValue())
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(
            values.stream()
                .map(
                    v ->
                        v == null
                            ? null
                            : Literal.of(
                                    WriteUtil.getFormattedValueForDateTimes(
                                        Types.TimestampType.withoutZone(), v))
                                .to(Types.TimestampType.withoutZone())
                                .value())
                .collect(Collectors.toList()));
  }

  private void insertTimestampTZTestData(Table table) {
    List<Map<String, Object>> data = new ArrayList<>();
    long id = 1;

    // Add test data for each test case
    List<OffsetDateTime> values =
        Arrays.asList(
            OffsetDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
            OffsetDateTime.of(2025, 1, 1, 12, 30, 45, 123456000, ZoneOffset.ofHours(5)),
            OffsetDateTime.of(2025, 1, 1, 12, 30, 45, 500000000, ZoneOffset.ofHours(-5)),
            OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 999999000, ZoneOffset.UTC),
            null);

    for (OffsetDateTime value : values) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", id++);
      row.put("timestamptz_partition", value);
      data.add(row);
    }

    // Insert data
    CommitMetrics commitMetrics = insertTestData(table, data);
    assertThat(commitMetrics).isNotNull();
    assertThat(commitMetrics.getAddedFilesCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getAddedRecordsCount()).isEqualTo(values.size());
    assertThat(commitMetrics.getPartitionCommitMetrics()).hasSize(values.size());
    assertThat(
            commitMetrics.getPartitionCommitMetrics().stream()
                .map(p -> p.getPartitionData().getPartitionValues().get(0).getValue())
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(
            values.stream()
                .map(
                    v ->
                        v == null
                            ? null
                            : Literal.of(
                                    WriteUtil.getFormattedValueForDateTimes(
                                        Types.TimestampType.withZone(), v))
                                .to(Types.TimestampType.withZone())
                                .value())
                .collect(Collectors.toList()));
  }

  private CommitMetrics insertTestData(Table table, List<Map<String, Object>> data) {
    String inputSql = TestUtil.createSelectSql(data, table.schema());
    return swiftLakeEngine.insertInto(table).sql(inputSql).processSourceTables(false).execute();
  }
}
