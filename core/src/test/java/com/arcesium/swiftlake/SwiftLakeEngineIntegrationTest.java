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

import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.mybatis.SwiftLakeMybatisConfiguration;
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
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SwiftLakeEngineIntegrationTest {
  private static SwiftLakeEngine swiftLakeEngine;
  private static String mainTableName;
  private static String secondaryTableName;
  private static String floatingPointTable1Name;
  private static String floatingPointTable2Name;
  private static String floatingPointPartitionedTableName;

  @TempDir private static Path basePath;

  @BeforeAll
  public static void setup() {
    swiftLakeEngine =
        TestUtil.getSwiftLakeEngineBuilder(basePath.toString(), 50, 2)
            .mybatisConfigPath("mybatis/mybatis-config.xml")
            .build();

    int id = 0;
    // Create and populate main test table
    Schema mainSchema =
        new Schema(
            Types.NestedField.optional(++id, "id", Types.LongType.get()),
            Types.NestedField.optional(++id, "name", Types.StringType.get()),
            Types.NestedField.optional(++id, "category", Types.StringType.get()),
            Types.NestedField.optional(++id, "date", Types.DateType.get()),
            Types.NestedField.optional(++id, "int_col", Types.IntegerType.get()),
            Types.NestedField.optional(++id, "long_col", Types.LongType.get()),
            Types.NestedField.optional(++id, "float_col", Types.FloatType.get()),
            Types.NestedField.optional(++id, "double_col", Types.DoubleType.get()),
            Types.NestedField.optional(++id, "decimal_col", Types.DecimalType.of(10, 2)),
            Types.NestedField.optional(++id, "bool_col", Types.BooleanType.get()),
            Types.NestedField.optional(++id, "timestamp_col", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(++id, "timestamptz_col", Types.TimestampType.withZone()),
            Types.NestedField.optional(++id, "time_col", Types.TimeType.get()),
            Types.NestedField.optional(
                ++id,
                "struct_col",
                Types.StructType.of(
                    Types.NestedField.optional(++id, "nested_int", Types.IntegerType.get()),
                    Types.NestedField.optional(++id, "nested_string", Types.StringType.get()),
                    Types.NestedField.optional(
                        ++id,
                        "struct_col",
                        Types.StructType.of(
                            Types.NestedField.optional(
                                ++id, "nested_int", Types.IntegerType.get()))))),
            Types.NestedField.optional(
                ++id, "list_col", Types.ListType.ofRequired(++id, Types.StringType.get())),
            Types.NestedField.optional(
                ++id,
                "map_col",
                Types.MapType.ofRequired(
                    ++id, ++id, Types.StringType.get(), Types.IntegerType.get())));
    PartitionSpec mainSpec = PartitionSpec.builderFor(mainSchema).identity("date").build();

    TableIdentifier mainTableId = TableIdentifier.of("test_db", "query_test_table");
    swiftLakeEngine.getCatalog().createTable(mainTableId, mainSchema, mainSpec);
    mainTableName = mainTableId.toString();

    // Insert test data into main table
    List<Map<String, Object>> mainTestData = new ArrayList<>();
    mainTestData.add(createTestRecord(1L, "John", "A", LocalDate.parse("2025-01-01")));
    mainTestData.add(createTestRecord(2L, "Jane", "B", LocalDate.parse("2025-01-02")));
    mainTestData.add(createTestRecord(3L, "Bob", "A", LocalDate.parse("2025-02-01")));
    mainTestData.add(createTestRecord(4L, "Alice", "B", LocalDate.parse("2025-02-02")));
    mainTestData.add(createTestRecordWithNullValues(5L));

    swiftLakeEngine
        .insertInto(mainTableName)
        .sql(TestUtil.createSelectSql(mainTestData, mainSchema))
        .processSourceTables(false)
        .execute();

    // Create and populate secondary test table for join tests
    Schema secondarySchema =
        new Schema(
            Types.NestedField.required(1, "category", Types.StringType.get()),
            Types.NestedField.required(2, "description", Types.StringType.get()));

    TableIdentifier secondaryTableId = TableIdentifier.of("test_db", "category_table");
    Table secondaryTable =
        swiftLakeEngine.getCatalog().createTable(secondaryTableId, secondarySchema);
    secondaryTableName = secondaryTableId.toString();

    // Insert test data into secondary table
    List<Map<String, Object>> secondaryTestData = new ArrayList<>();
    secondaryTestData.add(Map.of("category", "A", "description", "Category A"));
    secondaryTestData.add(Map.of("category", "B", "description", "Category B"));

    swiftLakeEngine
        .insertInto(secondaryTableName)
        .sql(TestUtil.createSelectSql(secondaryTestData, secondarySchema))
        .processSourceTables(false)
        .execute();

    createTablesForFloatingPointSpecialValues();
  }

  private static void createTablesForFloatingPointSpecialValues() {
    TableIdentifier tableId =
        TableIdentifier.of("test_db", "floating_point_special_values_partitioned_table");
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "float_value", Types.FloatType.get()),
            Types.NestedField.optional(2, "double_value", Types.DoubleType.get()));
    floatingPointPartitionedTableName = tableId.toString();
    swiftLakeEngine
        .getCatalog()
        .createTable(
            tableId,
            schema,
            PartitionSpec.builderFor(schema)
                .identity("float_value")
                .identity("double_value")
                .build());
    swiftLakeEngine
        .insertInto(floatingPointPartitionedTableName)
        .sql(
            "SELECT 1.1::FLOAT float_value, 1.1::DOUBLE double_value"
                + " UNION ALL SELECT -1.1::FLOAT float_value, -1.1::DOUBLE double_value"
                + " UNION ALL SELECT 0::FLOAT float_value, 0::DOUBLE double_value"
                + " UNION ALL SELECT 'inf'::FLOAT float_value, 'inf'::DOUBLE double_value"
                + " UNION ALL SELECT '-inf'::FLOAT float_value, '-inf'::DOUBLE double_value"
                + " UNION ALL SELECT 'nan'::FLOAT float_value, 'nan'::DOUBLE double_value")
        .processSourceTables(false)
        .execute();

    tableId = TableIdentifier.of("test_db", "floating_point_special_values_table_1");
    floatingPointTable1Name = tableId.toString();
    swiftLakeEngine.getCatalog().createTable(tableId, schema);
    swiftLakeEngine
        .insertInto(floatingPointTable1Name)
        .sql(
            "SELECT 1.1::FLOAT float_value, 1.1::DOUBLE double_value"
                + " UNION ALL SELECT -1.1::FLOAT float_value, -1.1::DOUBLE double_value"
                + " UNION ALL SELECT 0::FLOAT float_value, 0::DOUBLE double_value"
                + " UNION ALL SELECT 'inf'::FLOAT float_value, 'inf'::DOUBLE double_value"
                + " UNION ALL SELECT '-inf'::FLOAT float_value, '-inf'::DOUBLE double_value"
                + " UNION ALL SELECT 'nan'::FLOAT float_value, 'nan'::DOUBLE double_value")
        .processSourceTables(false)
        .execute();

    tableId = TableIdentifier.of("test_db", "floating_point_special_values_table_2");
    floatingPointTable2Name = tableId.toString();
    swiftLakeEngine.getCatalog().createTable(tableId, schema);
    swiftLakeEngine
        .insertInto(floatingPointTable2Name)
        .sql("SELECT 1.1::FLOAT float_value, 1.1::DOUBLE double_value")
        .processSourceTables(false)
        .execute();
    swiftLakeEngine
        .insertInto(floatingPointTable2Name)
        .sql("SELECT -1.1::FLOAT float_value, -1.1::DOUBLE double_value")
        .processSourceTables(false)
        .execute();
    swiftLakeEngine
        .insertInto(floatingPointTable2Name)
        .sql("SELECT 0::FLOAT float_value, 0::DOUBLE double_value")
        .processSourceTables(false)
        .execute();
    swiftLakeEngine
        .insertInto(floatingPointTable2Name)
        .sql("SELECT 'inf'::FLOAT float_value, 'inf'::DOUBLE double_value")
        .processSourceTables(false)
        .execute();
    swiftLakeEngine
        .insertInto(floatingPointTable2Name)
        .sql("SELECT '-inf'::FLOAT float_value, '-inf'::DOUBLE double_value")
        .processSourceTables(false)
        .execute();
    swiftLakeEngine
        .insertInto(floatingPointTable2Name)
        .sql("SELECT 'nan'::FLOAT float_value, 'nan'::DOUBLE double_value")
        .processSourceTables(false)
        .execute();
  }

  @AfterAll
  public static void teardown() {
    TestUtil.dropIcebergTables(
        swiftLakeEngine,
        List.of(
            mainTableName,
            secondaryTableName,
            floatingPointPartitionedTableName,
            floatingPointTable1Name,
            floatingPointTable2Name));
    swiftLakeEngine.close();
    try {
      FileUtils.deleteDirectory(new File(basePath.toString()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("provideReadTestCases")
  void testReadOperations(
      String testName,
      String preSql,
      String sql,
      String postSql,
      List<Map<String, Object>> expectedResult,
      Pair<Class<? extends Throwable>, String> expectedError)
      throws Exception {
    if (expectedError == null) {
      List<Map<String, Object>> actualResult =
          TestUtil.executeQuery(swiftLakeEngine, preSql, sql, postSql);
      assertThat(actualResult)
          .as("Test case: %s", testName)
          .usingRecursiveFieldByFieldElementComparator(
              RecursiveComparisonConfiguration.builder()
                  .withComparatorForType(
                      (d1, d2) -> {
                        if (Double.isNaN(d1) && Double.isNaN(d2)) {
                          return 0;
                        }
                        return Double.compare(d1, d2);
                      },
                      Double.class)
                  .withComparatorForType(
                      (f1, f2) -> {
                        if (Float.isNaN(f1) && Float.isNaN(f2)) {
                          return 0;
                        }
                        return Float.compare(f1, f2);
                      },
                      Float.class)
                  .build())
          .containsExactlyInAnyOrderElementsOf(expectedResult);
    } else {
      var throwableAssert =
          assertThatThrownBy(() -> TestUtil.executeQuery(swiftLakeEngine, preSql, sql, postSql));
      if (expectedError.getLeft() != null) {
        throwableAssert.isInstanceOf(expectedError.getLeft());
      }
      if (expectedError.getRight() != null) {
        throwableAssert.hasMessageContaining(expectedError.getRight());
      }
    }
  }

  private static Stream<Arguments> provideReadTestCases() {
    Stream<Arguments> argumentsStream =
        Stream.of(
            Arguments.of(
                "Basic Query",
                null,
                "SELECT * FROM " + mainTableName + " WHERE id = 1",
                null,
                Arrays.asList(
                    createTestRecord(1L, "John", "A", LocalDate.parse("2025-01-01"), true)),
                null),
            Arguments.of(
                "Complex Query",
                null,
                "SELECT category, COUNT(*) as count, MAX(date) as max_date "
                    + "FROM "
                    + mainTableName
                    + " "
                    + "WHERE date BETWEEN DATE '2025-01-01' AND DATE '2025-02-01' "
                    + "GROUP BY category "
                    + "HAVING COUNT(*) > 1 "
                    + "ORDER BY category",
                null,
                Arrays.asList(
                    Map.of(
                        "category", "A", "count", 2L, "max_date", LocalDate.parse("2025-02-01"))),
                null),
            Arguments.of(
                "Empty Result",
                null,
                "SELECT * FROM " + mainTableName + " WHERE id < -1 OR id = NULL OR date = NULL",
                null,
                Arrays.asList(),
                null),
            Arguments.of(
                "Join Query",
                null,
                "SELECT m.id, m.name, m.category, s.description "
                    + "FROM (SELECT * FROM "
                    + mainTableName
                    + " WHERE date = DATE '2025-01-01') m "
                    + "JOIN (SELECT * FROM "
                    + secondaryTableName
                    + ") s ON m.category = s.category ",
                null,
                Arrays.asList(
                    Map.of("id", 1L, "name", "John", "category", "A", "description", "Category A")),
                null),
            Arguments.of(
                "Subquery",
                null,
                "SELECT * FROM (SELECT * FROM "
                    + mainTableName
                    + " WHERE date > DATE '2025-01-01')"
                    + "WHERE category IN (SELECT category FROM "
                    + secondaryTableName
                    + ") ",
                null,
                Arrays.asList(
                    createTestRecord(2L, "Jane", "B", LocalDate.parse("2025-01-02"), true),
                    createTestRecord(3L, "Bob", "A", LocalDate.parse("2025-02-01"), true),
                    createTestRecord(4L, "Alice", "B", LocalDate.parse("2025-02-02"), true)),
                null),
            Arguments.of(
                "Join Filter 1",
                "CREATE TEMP TABLE filter_table AS (SELECT * FROM (VALUES('A')) s(category))",
                "SELECT id, name, m.category, date FROM "
                    + mainTableName
                    + " m "
                    + "JOIN filter_table s ON m.category = s.category "
                    + "WHERE date > DATE '2025-01-01'",
                "DROP TABLE filter_table",
                Arrays.asList(
                    Map.of(
                        "id",
                        3L,
                        "name",
                        "Bob",
                        "category",
                        "A",
                        "date",
                        LocalDate.parse("2025-02-01"))),
                null),
            Arguments.of(
                "Join Filter 2",
                "CREATE TEMP TABLE filter_table AS (SELECT * FROM (VALUES(-6),(4)) s(nested_int))",
                "SELECT id, name FROM "
                    + mainTableName
                    + " m "
                    + "JOIN filter_table s ON struct_col.struct_col.nested_int = nested_int "
                    + "WHERE date > DATE '2025-01-01'",
                "DROP TABLE filter_table",
                Arrays.asList(Map.of("id", 2L, "name", "Jane"), Map.of("id", 3L, "name", "Bob")),
                null),
            Arguments.of(
                "Join Filter 3",
                "CREATE TEMP TABLE filter_table AS (SELECT * FROM (VALUES(-6),(4)) s(nested_int))",
                "SELECT id, name FROM "
                    + mainTableName
                    + " m "
                    + "JOIN filter_table s ON struct_col.struct_col.nested_int = s.nested_int "
                    + "WHERE date > DATE '2025-01-01'",
                "DROP TABLE filter_table",
                Arrays.asList(Map.of("id", 2L, "name", "Jane"), Map.of("id", 3L, "name", "Bob")),
                null),
            Arguments.of(
                "Join Filter 4",
                "CREATE TEMP TABLE filter_table AS (SELECT * FROM (VALUES(-6),(4)) s(nested_int))",
                "SELECT id, name FROM "
                    + mainTableName
                    + " m "
                    + "JOIN filter_table s ON m.struct_col.struct_col.nested_int = nested_int "
                    + "WHERE date > DATE '2025-01-01'",
                "DROP TABLE filter_table",
                Arrays.asList(Map.of("id", 2L, "name", "Jane"), Map.of("id", 3L, "name", "Bob")),
                null),
            Arguments.of(
                "Join Filter 5",
                "CREATE TEMP TABLE filter_table AS (SELECT * FROM (VALUES(-6),(4)) s(nested_int))",
                "SELECT id, name FROM "
                    + mainTableName
                    + " m "
                    + "JOIN filter_table s ON m.struct_col.struct_col.nested_int = s.nested_int "
                    + "WHERE date > DATE '2025-01-01'",
                "DROP TABLE filter_table",
                Arrays.asList(Map.of("id", 2L, "name", "Jane"), Map.of("id", 3L, "name", "Bob")),
                null),
            Arguments.of(
                "Join Filter 6",
                "CREATE TEMP TABLE filter_table AS (SELECT * FROM (VALUES(-6),(4)) s(struct_col))",
                "SELECT id, name FROM "
                    + mainTableName
                    + " m "
                    + "JOIN filter_table s ON struct_col.struct_col.nested_int = struct_col "
                    + "WHERE date > DATE '2025-01-01'",
                "DROP TABLE filter_table",
                null,
                Pair.of(
                    ValidationException.class, "Ambiguous reference to column name 'struct_col'")),
            Arguments.of(
                "Join Filter 7",
                "CREATE TEMP TABLE filter_table AS (SELECT * FROM (VALUES(-6),(4)) s(struct_col))",
                "SELECT id, name FROM "
                    + mainTableName
                    + " m "
                    + "JOIN filter_table s ON struct_col.struct_col.nested_int = m.struct_col "
                    + "WHERE date > DATE '2025-01-01'",
                "DROP TABLE filter_table",
                null,
                Pair.of(
                    ValidationException.class,
                    "Both the columns struct_col.struct_col.nested_int and m.struct_col belong to same table.")),
            Arguments.of(
                "Join Filter 8",
                "CREATE TEMP TABLE filter_table AS (SELECT * FROM (VALUES(-6),(4)) s(struct_col))",
                "SELECT id, name FROM "
                    + mainTableName
                    + " m "
                    + "JOIN filter_table s ON m.struct_col.struct_col.nested_int = s.struct_col "
                    + "WHERE date > DATE '2025-01-01'",
                "DROP TABLE filter_table",
                Arrays.asList(Map.of("id", 2L, "name", "Jane"), Map.of("id", 3L, "name", "Bob")),
                null),
            Arguments.of(
                "Window Function",
                null,
                "SELECT id, name, category, date, ROW_NUMBER() OVER (PARTITION BY category ORDER BY date) as row_num "
                    + "FROM "
                    + mainTableName
                    + " WHERE name IS NOT NULL ORDER BY category, date",
                null,
                Arrays.asList(
                    Map.of(
                        "id",
                        1L,
                        "name",
                        "John",
                        "category",
                        "A",
                        "date",
                        LocalDate.parse("2025-01-01"),
                        "row_num",
                        1L),
                    Map.of(
                        "id",
                        3L,
                        "name",
                        "Bob",
                        "category",
                        "A",
                        "date",
                        LocalDate.parse("2025-02-01"),
                        "row_num",
                        2L),
                    Map.of(
                        "id",
                        2L,
                        "name",
                        "Jane",
                        "category",
                        "B",
                        "date",
                        LocalDate.parse("2025-01-02"),
                        "row_num",
                        1L),
                    Map.of(
                        "id",
                        4L,
                        "name",
                        "Alice",
                        "category",
                        "B",
                        "date",
                        LocalDate.parse("2025-02-02"),
                        "row_num",
                        2L)),
                null),
            Arguments.of(
                "CTE",
                null,
                "WITH ranked_data AS ("
                    + "  SELECT id, name, category, date, ROW_NUMBER() OVER (PARTITION BY category ORDER BY date) as row_num "
                    + "  FROM "
                    + mainTableName
                    + " WHERE name IS NOT NULL) SELECT * FROM ranked_data WHERE row_num = 1",
                null,
                Arrays.asList(
                    Map.of(
                        "id",
                        1L,
                        "name",
                        "John",
                        "category",
                        "A",
                        "date",
                        LocalDate.parse("2025-01-01"),
                        "row_num",
                        1L),
                    Map.of(
                        "id",
                        2L,
                        "name",
                        "Jane",
                        "category",
                        "B",
                        "date",
                        LocalDate.parse("2025-01-02"),
                        "row_num",
                        1L)),
                null),
            Arguments.of(
                "Union",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE category = 'A' "
                    + "UNION ALL "
                    + "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE date > DATE '2025-02-01'",
                null,
                Arrays.asList(
                    Map.of("id", 1L, "name", "John"),
                    Map.of("id", 3L, "name", "Bob"),
                    Map.of("id", 4L, "name", "Alice")),
                null),
            Arguments.of(
                "Limit",
                null,
                "SELECT id, name, category, date FROM " + mainTableName + " ORDER BY id LIMIT 2",
                null,
                Arrays.asList(
                    Map.of(
                        "id",
                        1L,
                        "name",
                        "John",
                        "category",
                        "A",
                        "date",
                        LocalDate.parse("2025-01-01")),
                    Map.of(
                        "id",
                        2L,
                        "name",
                        "Jane",
                        "category",
                        "B",
                        "date",
                        LocalDate.parse("2025-01-02"))),
                null),
            Arguments.of(
                "Distinct",
                null,
                "SELECT DISTINCT category FROM " + mainTableName,
                null,
                Arrays.asList(
                    Map.of("category", "A"),
                    Map.of("category", "B"),
                    new HashMap<>() {
                      {
                        put("category", null);
                      }
                    }),
                null),
            Arguments.of(
                "DuckDB Non-ANSI SQL with Parse Markers",
                null,
                "CREATE TEMPORARY TABLE temp_result AS\n"
                    + "--SWIFTLAKE_PARSE_BEGIN--\n"
                    + "SELECT id, name,\n"
                    + "       CASE WHEN category = 'A' THEN 'Category A' ELSE 'Category B' END AS category_description,\n"
                    + "       date,\n"
                    + "       ROW_NUMBER() OVER (PARTITION BY category ORDER BY date) AS row_num,\n"
                    + "       LIST(name) OVER (PARTITION BY category) AS names_in_category\n"
                    + "FROM "
                    + mainTableName
                    + "\n"
                    + "WHERE date BETWEEN '2025-01-01' AND '2025-12-31'\n"
                    + "QUALIFY row_num <= 2\n"
                    + "ORDER BY id\n"
                    + "LIMIT 3\n"
                    + "--SWIFTLAKE_PARSE_END--\n"
                    + ";\n"
                    + "SELECT * FROM temp_result;",
                null,
                Arrays.asList(
                    Map.of(
                        "id",
                        1L,
                        "name",
                        "John",
                        "category_description",
                        "Category A",
                        "date",
                        LocalDate.parse("2025-01-01"),
                        "row_num",
                        1L,
                        "names_in_category",
                        Arrays.asList("John", "Bob")),
                    Map.of(
                        "id",
                        2L,
                        "name",
                        "Jane",
                        "category_description",
                        "Category B",
                        "date",
                        LocalDate.parse("2025-01-02"),
                        "row_num",
                        1L,
                        "names_in_category",
                        Arrays.asList("Jane", "Alice")),
                    Map.of(
                        "id",
                        3L,
                        "name",
                        "Bob",
                        "category_description",
                        "Category A",
                        "date",
                        LocalDate.parse("2025-02-01"),
                        "row_num",
                        2L,
                        "names_in_category",
                        Arrays.asList("John", "Bob"))),
                null),
            Arguments.of(
                "Numeric Comparisons",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " a WHERE int_col >= -3 AND a.int_col <= 2 AND long_col >= -3 AND a.long_col <= 2 AND float_col < 2.5 AND a.float_col > -3.5 "
                    + " AND double_col BETWEEN -4.0 AND 4.0 AND (decimal_col = 2.99 OR a.decimal_col != -1.99)"
                    + " AND (a.struct_col.nested_int = -3 OR struct_col.nested_int = 2) AND (a.struct_col.struct_col.nested_int = -6 OR struct_col.struct_col.nested_int = 4)",
                null,
                Arrays.asList(Map.of("id", 2L, "name", "Jane"), Map.of("id", 3L, "name", "Bob")),
                null),
            Arguments.of(
                "String Operations",
                null,
                "SELECT * FROM (SELECT id, name FROM "
                    + mainTableName
                    + " WHERE category = 'B') WHERE name LIKE 'J%'",
                null,
                Arrays.asList(Map.of("id", 2L, "name", "Jane")),
                null),
            Arguments.of(
                "Date Comparisons",
                null,
                "SELECT id, name, date FROM "
                    + mainTableName
                    + " WHERE date > DATE '2025-01-15' AND date < DATE '2025-02-15'",
                null,
                Arrays.asList(
                    Map.of("id", 3L, "name", "Bob", "date", LocalDate.parse("2025-02-01")),
                    Map.of("id", 4L, "name", "Alice", "date", LocalDate.parse("2025-02-02"))),
                null),
            Arguments.of(
                "Date Validation with TIME literal type",
                null,
                "SELECT id, name, date FROM "
                    + mainTableName
                    + " WHERE date > TIME '2025-01-15' AND date < DATE '2025-02-15'",
                null,
                null,
                Pair.of(ValidationException.class, "Expected DATE literal or string value")),
            Arguments.of(
                "Date Validation with TIMESTAMP literal type",
                null,
                "SELECT id, name, date FROM "
                    + mainTableName
                    + " WHERE date > DATE '2025-01-15' AND date < TIMESTAMP '2025-02-15'",
                null,
                null,
                Pair.of(ValidationException.class, "Expected DATE literal or string value")),
            Arguments.of(
                "Date Validation with TIMESTAMPTZ literal type",
                null,
                "SELECT id, name, date FROM "
                    + mainTableName
                    + " WHERE date > TIMESTAMPTZ '2025-01-15' AND date < DATE '2025-02-15'",
                null,
                null,
                Pair.of(ValidationException.class, "Expected DATE literal or string value")),
            Arguments.of(
                "Boolean Conditions",
                null,
                "SELECT id, name FROM " + mainTableName + " WHERE bool_col = true",
                null,
                Arrays.asList(Map.of("id", 2L, "name", "Jane"), Map.of("id", 4L, "name", "Alice")),
                null),
            Arguments.of(
                "Decimal Comparisons",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE decimal_col > 2.99 OR decimal_col < -2.50",
                null,
                Arrays.asList(Map.of("id", 3L, "name", "Bob"), Map.of("id", 4L, "name", "Alice")),
                null),
            Arguments.of(
                "Time Comparisons",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE time_col > TIME '02:00:00' AND time_col < TIME'04:02:00'",
                null,
                Arrays.asList(Map.of("id", 2L, "name", "Jane"), Map.of("id", 3L, "name", "Bob")),
                null),
            Arguments.of(
                "Time validation with DATE literal type",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE time_col > DATE '02:00:00' AND time_col < '04:02:00'",
                null,
                null,
                Pair.of(ValidationException.class, "Expected TIME literal or string value")),
            Arguments.of(
                "Time validation with TIMESTAMP literal type",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE time_col > TIMESTAMP '02:00:00' AND time_col < '04:02:00'",
                null,
                null,
                Pair.of(ValidationException.class, "Expected TIME literal or string value")),
            Arguments.of(
                "Time validation with TIMESTAMPTZ literal type",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE time_col > TIMESTAMPTZ '02:00:00' AND time_col < '04:02:00'",
                null,
                null,
                Pair.of(ValidationException.class, "Expected TIME literal or string value")),
            Arguments.of(
                "Timestamp Comparisons",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE timestamp_col > TIMESTAMP '2025-01-01T12:00:00' AND timestamp_col < TIMESTAMP '2025-02-01T12:00:00'",
                null,
                Arrays.asList(Map.of("id", 2L, "name", "Jane"), Map.of("id", 3L, "name", "Bob")),
                null),
            Arguments.of(
                "Timestamp validation with DATE literal type",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE timestamp_col > DATE '2025-01-01T12:00:00' AND timestamp_col < TIMESTAMP '2025-02-01T12:00:00'",
                null,
                null,
                Pair.of(ValidationException.class, "Expected TIMESTAMP literal or string value")),
            Arguments.of(
                "Timestamp validation with TIMESTAMPTZ literal type",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE timestamp_col > TIMESTAMP '2025-01-01T12:00:00' AND timestamp_col < TIMESTAMPTZ '2025-02-01T12:00:00'",
                null,
                null,
                Pair.of(ValidationException.class, "Expected TIMESTAMP literal or string value")),
            Arguments.of(
                "Timestamp validation with TIME literal type",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE timestamp_col > TIMESTAMP '2025-01-01T12:00:00' AND timestamp_col < TIME '2025-02-01T12:00:00'",
                null,
                null,
                Pair.of(ValidationException.class, "Expected TIMESTAMP literal or string value")),
            Arguments.of(
                "TimestampTZ Comparisons",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE timestamptz_col > TIMESTAMPTZ '2025-01-01T12:00:00+05:30' AND timestamptz_col < TIMESTAMPTZ'2025-02-01T12:00:00+05:30'",
                null,
                Arrays.asList(Map.of("id", 2L, "name", "Jane"), Map.of("id", 3L, "name", "Bob")),
                null),
            Arguments.of(
                "TimestampTZ validation with DATE literal type",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE timestamptz_col > DATE '2025-01-01T12:00:00+05:30' AND timestamptz_col < '2025-02-01T12:00:00+05:30'",
                null,
                null,
                Pair.of(ValidationException.class, "Expected TIMESTAMPTZ literal or string value")),
            Arguments.of(
                "TimestampTZ validation with TIME literal type",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE timestamptz_col > TIME '2025-01-01T12:00:00+05:30' AND timestamptz_col < '2025-02-01T12:00:00+05:30'",
                null,
                null,
                Pair.of(ValidationException.class, "Expected TIMESTAMPTZ literal or string value")),
            Arguments.of(
                "TimestampTZ validation with TIMESTAMP literal type",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE timestamptz_col > TIMESTAMP '2025-01-01T12:00:00+05:30' AND timestamptz_col < '2025-02-01T12:00:00+05:30'",
                null,
                null,
                Pair.of(ValidationException.class, "Expected TIMESTAMPTZ literal or string value")),
            Arguments.of(
                "Struct Field Conditions",
                null,
                "SELECT id, name FROM (SELECT id, name, struct_col FROM "
                    + mainTableName
                    + " WHERE struct_col.nested_int > 2 OR struct_col.nested_int = -3) WHERE struct_col.nested_string LIKE 'nested_%'",
                null,
                Arrays.asList(Map.of("id", 3L, "name", "Bob"), Map.of("id", 4L, "name", "Alice")),
                null),
            Arguments.of(
                "List Conditions 1",
                null,
                "SELECT id, name, list_col FROM "
                    + mainTableName
                    + " WHERE list_col[1] = 'list_item_1_1' OR array_contains(list_col, 'list_item_2_3')",
                null,
                null,
                Pair.of(
                    ValidationException.class,
                    "Conditions on List and Map types are not supported in table-level filtering. Please keep these conditions out of the table filter.")),
            Arguments.of(
                "List Conditions 2",
                null,
                "SELECT id, name FROM (SELECT id, name, list_col FROM "
                    + mainTableName
                    + ") WHERE list_col[1] = 'list_item_1_1' OR array_contains(list_col, 'list_item_2_3')",
                null,
                Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 3L, "name", "Bob")),
                null),
            Arguments.of(
                "Map Conditions 1",
                null,
                "SELECT id, name, map_col FROM "
                    + mainTableName
                    + " WHERE map_col['key1_' || CAST(id AS VARCHAR)] = 1",
                null,
                null,
                Pair.of(
                    ValidationException.class,
                    "Conditions on List and Map types are not supported in table-level filtering. Please keep these conditions out of the table filter.")),
            Arguments.of(
                "Map Conditions 2",
                null,
                "SELECT id, name, map_col FROM "
                    + mainTableName
                    + " WHERE map_col['key1_' || CAST(id AS VARCHAR)][1] = 1",
                null,
                null,
                Pair.of(
                    ValidationException.class,
                    "For comparison one side must be a column name and other side must be a literal value")),
            Arguments.of(
                "Map Conditions 3",
                null,
                "SELECT id, name FROM (SELECT id, name, map_col FROM "
                    + mainTableName
                    + ") WHERE map_col['key1_' || CAST(id AS VARCHAR)][1] = 1",
                null,
                Arrays.asList(Map.of("id", 2L, "name", "Jane"), Map.of("id", 4L, "name", "Alice")),
                null),
            Arguments.of(
                "Complex Combined Conditions",
                null,
                "SELECT id, name FROM (SELECT * FROM "
                    + mainTableName
                    + " WHERE (int_col > 0 OR float_col < 0) AND "
                    + "(date BETWEEN DATE '2025-01-01' AND DATE '2025-02-01') AND "
                    + "(bool_col = true OR decimal_col < 0)) WHERE "
                    + "(struct_col.nested_int % 2 = 0) AND "
                    + "(array_contains(list_col, 'list_item_2_' || CAST(id AS VARCHAR)))",
                null,
                Arrays.asList(Map.of("id", 2L, "name", "Jane")),
                null),
            Arguments.of(
                "Logical Operators with Negative Numbers",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE (category = 'B' AND int_col > -2) OR (category = 'A' AND int_col < -1)",
                null,
                Arrays.asList(
                    Map.of("id", 2L, "name", "Jane"),
                    Map.of("id", 3L, "name", "Bob"),
                    Map.of("id", 4L, "name", "Alice")),
                null),
            Arguments.of(
                "IS NULL and IS NOT NULL with Negative Numbers",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE int_col IS NULL OR (int_col IS NOT NULL AND float_col < -1.5)",
                null,
                Arrays.asList(
                    Map.of("id", 1L, "name", "John"),
                    Map.of("id", 3L, "name", "Bob"),
                    new HashMap<>() {
                      {
                        put("id", 5L);
                        put("name", null);
                      }
                    }),
                null),
            Arguments.of(
                "IN and NOT IN with Negative Numbers",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE decimal_col IN (-1.99, -3.99) AND id NOT IN (2, 4)",
                null,
                Arrays.asList(Map.of("id", 1L, "name", "John"), Map.of("id", 3L, "name", "Bob")),
                null),
            Arguments.of(
                "Greater Than Equal and Less Than Equal with Negative Numbers",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE int_col >= -2 AND long_col >= -2 AND float_col <= 4.0",
                null,
                Arrays.asList(Map.of("id", 2L, "name", "Jane"), Map.of("id", 4L, "name", "Alice")),
                null),
            Arguments.of(
                "IS DISTINCT FROM with Negative Numbers",
                null,
                "SELECT id, name FROM " + mainTableName + " WHERE int_col IS DISTINCT FROM NULL",
                null,
                Arrays.asList(
                    Map.of("id", 2L, "name", "Jane"),
                    Map.of("id", 3L, "name", "Bob"),
                    Map.of("id", 4L, "name", "Alice")),
                null),
            Arguments.of(
                "IS NOT DISTINCT FROM with Null",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE float_col IS NOT DISTINCT FROM NULL",
                null,
                Arrays.asList(
                    // Assuming some records have null float_col
                    Map.of("id", 1L, "name", "John"),
                    new HashMap<>() {
                      {
                        put("id", 5L);
                        put("name", null);
                      }
                    }),
                null),
            Arguments.of(
                "BETWEEN with Negative Numbers",
                null,
                "SELECT id, name FROM " + mainTableName + " WHERE double_col BETWEEN -3.5 AND -0.5",
                null,
                Arrays.asList(Map.of("id", 3L, "name", "Bob")),
                null),
            Arguments.of(
                "NOT BETWEEN with Negative Numbers",
                null,
                "SELECT id, name FROM " + mainTableName + " WHERE int_col NOT BETWEEN -2 AND 2",
                null,
                Arrays.asList(Map.of("id", 3L, "name", "Bob"), Map.of("id", 4L, "name", "Alice")),
                null),
            Arguments.of(
                "Complex Condition with Negative Numbers and Multiple Data Types",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE (category IN ('A', 'B') AND int_col < 0) "
                    + "   OR (date BETWEEN DATE '2025-01-01' AND DATE '2025-01-31' AND bool_col = false) "
                    + "   OR (double_col > 2.5 AND decimal_col IS DISTINCT FROM -1.99)",
                null,
                Arrays.asList(
                    Map.of("id", 1L, "name", "John"),
                    Map.of("id", 3L, "name", "Bob"),
                    Map.of("id", 4L, "name", "Alice")),
                null),
            Arguments.of(
                "Timestamp and Time Comparisons",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE timestamp_col > TIMESTAMP '2025-01-01T12:00:00' "
                    + "   AND time_col BETWEEN TIME '02:00:00' AND TIME '04:02:00'",
                null,
                Arrays.asList(Map.of("id", 2L, "name", "Jane"), Map.of("id", 3L, "name", "Bob")),
                null),
            Arguments.of(
                "Complex Nested Condition with Parentheses and Logical Operators",
                null,
                "SELECT id, name FROM "
                    + mainTableName
                    + " WHERE ((category = 'A' AND int_col < -2) OR (category = 'B' AND int_col > -1)) "
                    + "   AND (date >= DATE '2025-01-01' OR bool_col = true) "
                    + "   AND ((float_col BETWEEN -3.5 AND 2.5 AND double_col IS NOT NULL) "
                    + "       OR (decimal_col < -1.5 OR decimal_col > 1.5)) "
                    + "   AND (timestamp_col > TIMESTAMP '2025-01-01T00:00:00' "
                    + "       OR (time_col >= TIME '01:00:00' AND time_col < TIME '03:04:00')) "
                    + "   AND (struct_col.nested_int != 0) ",
                null,
                Arrays.asList(
                    Map.of("id", 2L, "name", "Jane"),
                    Map.of("id", 3L, "name", "Bob"),
                    Map.of("id", 4L, "name", "Alice")),
                null),
            Arguments.of(
                "Invalid table",
                null,
                "SELECT * FROM test_db.non_existent_table",
                null,
                null,
                Pair.of(
                    NoSuchTableException.class,
                    "Table does not exist: test_db.non_existent_table")));

    return Stream.of(
            argumentsStream,
            provideSpecialFloatingPointTestCases(floatingPointPartitionedTableName, true),
            provideSpecialFloatingPointTestCases(floatingPointPartitionedTableName, false),
            provideSpecialFloatingPointTestCases(floatingPointTable1Name, true),
            provideSpecialFloatingPointTestCases(floatingPointTable1Name, false),
            provideSpecialFloatingPointTestCases(floatingPointTable2Name, true),
            provideSpecialFloatingPointTestCases(floatingPointTable2Name, false))
        .flatMap(stream -> stream);
  }

  private static Stream<Arguments> provideSpecialFloatingPointTestCases(
      String tableName, boolean useFloatType) {
    String columnName = "double_value";
    Object nanValue = Double.NaN;
    Object positiveInfinity = Double.POSITIVE_INFINITY;
    Object negativeInfinity = Double.NEGATIVE_INFINITY;
    Object positiveValue = 1.1d;
    Object negativeValue = -1.1d;
    Object zeroValue = 0.0d;
    String type = "double";
    if (useFloatType) {
      columnName = "float_value";
      nanValue = Float.NaN;
      positiveInfinity = Float.POSITIVE_INFINITY;
      negativeInfinity = Float.NEGATIVE_INFINITY;
      positiveValue = 1.1f;
      negativeValue = -1.1f;
      zeroValue = 0.0f;
      type = "float";
    }
    var allRecords =
        List.of(
            Map.of(columnName, positiveValue),
            Map.of(columnName, negativeValue),
            Map.of(columnName, zeroValue),
            Map.of(columnName, positiveInfinity),
            Map.of(columnName, negativeInfinity),
            Map.of(columnName, nanValue));
    return Stream.of(
        // NaN
        Arguments.of(
            "Test 1 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT " + columnName + " FROM " + tableName + " WHERE " + columnName + " = 'NaN'",
            null,
            List.of(Map.of("" + columnName + "", nanValue)),
            null),
        Arguments.of(
            "Test 2 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT " + columnName + " FROM " + tableName + " WHERE " + columnName + " != 'NaN'",
            null,
            List.of(
                Map.of("" + columnName + "", positiveValue),
                Map.of("" + columnName + "", negativeValue),
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", positiveInfinity),
                Map.of("" + columnName + "", negativeInfinity)),
            null),
        Arguments.of(
            "Test 3 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT " + columnName + " FROM " + tableName + " WHERE " + columnName + " < 'NaN'",
            null,
            List.of(
                Map.of("" + columnName + "", positiveValue),
                Map.of("" + columnName + "", negativeValue),
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", positiveInfinity),
                Map.of("" + columnName + "", negativeInfinity)),
            null),
        Arguments.of(
            "Test 4 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT " + columnName + " FROM " + tableName + " WHERE " + columnName + " > 'NaN'",
            null,
            List.of(),
            null),
        Arguments.of(
            "Test 5 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT " + columnName + " FROM " + tableName + " WHERE " + columnName + " <= 'NaN'",
            null,
            allRecords,
            null),
        Arguments.of(
            "Test 6 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT " + columnName + " FROM " + tableName + " WHERE " + columnName + " >= 'NaN'",
            null,
            List.of(Map.of("" + columnName + "", nanValue)),
            null),
        Arguments.of(
            "Test 7 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " IS DISTINCT FROM 'NaN'",
            null,
            List.of(
                Map.of("" + columnName + "", positiveValue),
                Map.of("" + columnName + "", negativeValue),
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", positiveInfinity),
                Map.of("" + columnName + "", negativeInfinity)),
            null),
        Arguments.of(
            "Test 8 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " IS NOT DISTINCT FROM 'NaN'",
            null,
            List.of(Map.of("" + columnName + "", nanValue)),
            null),
        Arguments.of(
            "Test 9 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " IN ('NaN', 1.1)",
            null,
            List.of(
                Map.of("" + columnName + "", nanValue),
                Map.of("" + columnName + "", positiveValue)),
            null),
        Arguments.of(
            "Test 10 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " IN ('NaN', NULL)",
            null,
            List.of(Map.of("" + columnName + "", nanValue)),
            null),
        Arguments.of(
            "Test 11 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE NOT("
                + columnName
                + " IN ('NaN', NULL))",
            null,
            List.of(),
            null),
        Arguments.of(
            "Test 12 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " NOT IN('NaN', 1.1)",
            null,
            List.of(
                Map.of("" + columnName + "", negativeValue),
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", positiveInfinity),
                Map.of("" + columnName + "", negativeInfinity)),
            null),
        Arguments.of(
            "Test 13 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " NOT IN ('NaN', NULL)",
            null,
            List.of(),
            null),
        Arguments.of(
            "Test 14 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE NOT("
                + columnName
                + " NOT IN ('NaN', NULL))",
            null,
            List.of(Map.of("" + columnName + "", nanValue)),
            null),
        Arguments.of(
            "Test 15 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " NOT IN(-1.1, 1.1)",
            null,
            List.of(
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", nanValue),
                Map.of("" + columnName + "", positiveInfinity),
                Map.of("" + columnName + "", negativeInfinity)),
            null),
        Arguments.of(
            "Test 16 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " IN(-1.1, 1.1)",
            null,
            List.of(
                Map.of("" + columnName + "", positiveValue),
                Map.of("" + columnName + "", negativeValue)),
            null),

        // Positive Infinity
        Arguments.of(
            "Test 17 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT " + columnName + " FROM " + tableName + " WHERE " + columnName + " = 'inf'",
            null,
            List.of(Map.of("" + columnName + "", positiveInfinity)),
            null),
        Arguments.of(
            "Test 18 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT " + columnName + " FROM " + tableName + " WHERE " + columnName + " != '+inf'",
            null,
            List.of(
                Map.of("" + columnName + "", positiveValue),
                Map.of("" + columnName + "", negativeValue),
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", nanValue),
                Map.of("" + columnName + "", negativeInfinity)),
            null),
        Arguments.of(
            "Test 19 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " < 'infinity'",
            null,
            List.of(
                Map.of("" + columnName + "", positiveValue),
                Map.of("" + columnName + "", negativeValue),
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", negativeInfinity)),
            null),
        Arguments.of(
            "Test 20 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " > '+infinity'",
            null,
            List.of(Map.of("" + columnName + "", nanValue)),
            null),
        Arguments.of(
            "Test 21 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " <= 'infiniTy'",
            null,
            List.of(
                Map.of("" + columnName + "", positiveValue),
                Map.of("" + columnName + "", negativeValue),
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", positiveInfinity),
                Map.of("" + columnName + "", negativeInfinity)),
            null),
        Arguments.of(
            "Test 22 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " >= 'Infinity'",
            null,
            List.of(
                Map.of("" + columnName + "", nanValue),
                Map.of("" + columnName + "", positiveInfinity)),
            null),
        Arguments.of(
            "Test 23 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " IS DISTINCT FROM 'inf'",
            null,
            List.of(
                Map.of("" + columnName + "", positiveValue),
                Map.of("" + columnName + "", negativeValue),
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", nanValue),
                Map.of("" + columnName + "", negativeInfinity)),
            null),
        Arguments.of(
            "Test 24 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " IS NOT DISTINCT FROM 'inf'",
            null,
            List.of(Map.of("" + columnName + "", positiveInfinity)),
            null),
        Arguments.of(
            "Test 25 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " IN ('inf', 1.1)",
            null,
            List.of(
                Map.of("" + columnName + "", positiveInfinity),
                Map.of("" + columnName + "", positiveValue)),
            null),
        Arguments.of(
            "Test 26 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " NOT IN('inf', 1.1)",
            null,
            List.of(
                Map.of("" + columnName + "", negativeValue),
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", nanValue),
                Map.of("" + columnName + "", negativeInfinity)),
            null),
        // Negative Infinity
        Arguments.of(
            "Test 27 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT " + columnName + " FROM " + tableName + " WHERE " + columnName + " = '-inf'",
            null,
            List.of(Map.of("" + columnName + "", negativeInfinity)),
            null),
        Arguments.of(
            "Test 28 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT " + columnName + " FROM " + tableName + " WHERE " + columnName + " != '-inF'",
            null,
            List.of(
                Map.of("" + columnName + "", positiveValue),
                Map.of("" + columnName + "", negativeValue),
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", nanValue),
                Map.of("" + columnName + "", positiveInfinity)),
            null),
        Arguments.of(
            "Test 29 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " < '-infinity'",
            null,
            List.of(),
            null),
        Arguments.of(
            "Test 30 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " > '-infiniTY'",
            null,
            List.of(
                Map.of("" + columnName + "", positiveValue),
                Map.of("" + columnName + "", negativeValue),
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", positiveInfinity),
                Map.of("" + columnName + "", nanValue)),
            null),
        Arguments.of(
            "Test 31 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " <= '-infinity'",
            null,
            List.of(Map.of("" + columnName + "", negativeInfinity)),
            null),
        Arguments.of(
            "Test 32 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " >= '-infinity'",
            null,
            allRecords,
            null),
        Arguments.of(
            "Test 33 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " IS DISTINCT FROM '-inf'",
            null,
            List.of(
                Map.of("" + columnName + "", positiveValue),
                Map.of("" + columnName + "", negativeValue),
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", nanValue),
                Map.of("" + columnName + "", positiveInfinity)),
            null),
        Arguments.of(
            "Test 34 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " IS NOT DISTINCT FROM '-inf'",
            null,
            List.of(Map.of("" + columnName + "", negativeInfinity)),
            null),
        Arguments.of(
            "Test 35 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " IN ('-inf', 1.1)",
            null,
            List.of(
                Map.of("" + columnName + "", negativeInfinity),
                Map.of("" + columnName + "", positiveValue)),
            null),
        Arguments.of(
            "Test 36 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " NOT IN('-inf', 1.1)",
            null,
            List.of(
                Map.of("" + columnName + "", negativeValue),
                Map.of("" + columnName + "", zeroValue),
                Map.of("" + columnName + "", nanValue),
                Map.of("" + columnName + "", positiveInfinity)),
            null),
        Arguments.of(
            "Test 37 - " + tableName + " - " + (useFloatType ? "Float" : "Double") + " Type",
            null,
            "SELECT "
                + columnName
                + " FROM "
                + tableName
                + " WHERE "
                + columnName
                + " IN ('-asdfinf', 1.1)",
            null,
            null,
            Pair.of(ValidationException.class, "Invalid " + type + " value '-asdfinf'")));
  }

  private static Map<String, Object> createTestRecord(
      Long id, String name, String category, LocalDate date) {
    return createTestRecord(id, name, category, date, false);
  }

  private static Map<String, Object> createTestRecord(
      Long id, String name, String category, LocalDate date, boolean convertToUTC) {
    Map<String, Object> record = new HashMap<>();
    record.put("id", id);
    record.put("name", name);
    record.put("category", category);
    record.put("date", date);
    record.put("int_col", id == 1 ? null : (id % 2 == 0 ? 1 : -1) * id.intValue());
    record.put("long_col", id == 1 ? null : (id % 2 == 0 ? 1 : -1) * id);
    record.put("float_col", id == 1 ? null : (id % 2 == 0 ? 1 : -1) * id.floatValue());
    record.put("double_col", id == 1 ? null : (id % 2 == 0 ? 1 : -1) * id.doubleValue());
    record.put(
        "decimal_col", new BigDecimal(id + ".99").multiply(new BigDecimal(id % 2 == 0 ? 1 : -1)));
    record.put("bool_col", id % 2 == 0);
    record.put(
        "timestamp_col",
        LocalDateTime.parse(
            date + String.format("T%02d:%02d:%02d.%06d", id % 24, id % 60, id % 60, id)));
    OffsetDateTime offsetDateTime =
        OffsetDateTime.parse(
            date + String.format("T%02d:%02d:%02d.%06d+05:30", id % 24, id % 60, id % 60, id));
    record.put(
        "timestamptz_col",
        convertToUTC ? offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC) : offsetDateTime);
    record.put(
        "time_col",
        LocalTime.parse(String.format("%02d:%02d:%02d.%06d", id % 24, id % 60, id % 60, id)));
    record.put(
        "struct_col",
        Map.of(
            "nested_int",
            (id % 2 == 0 ? 1 : -1) * id.intValue(),
            "nested_string",
            "nested_" + id,
            "struct_col",
            Map.of("nested_int", (id % 2 == 0 ? 2 : -2) * id.intValue())));
    record.put("list_col", Arrays.asList("list_item_1_" + id, "list_item_2_" + id));
    record.put(
        "map_col",
        Map.of("key1_" + id, (id % 2 == 0 ? 1 : -1), "key2_" + id, (id % 2 == 0 ? 2 : -2)));
    return record;
  }

  private static Map<String, Object> createTestRecordWithNullValues(Long id) {
    Map<String, Object> record = new HashMap<>();
    record.put("id", id);
    record.put("name", null);
    record.put("category", null);
    record.put("date", null);
    record.put("int_col", null);
    record.put("long_col", null);
    record.put("float_col", null);
    record.put("double_col", null);
    record.put("decimal_col", null);
    record.put("bool_col", null);
    record.put("timestamp_col", null);
    record.put("timestamptz_col", null);
    record.put("time_col", null);
    record.put("struct_col", null);
    record.put("list_col", null);
    record.put("map_col", null);
    return record;
  }

  private static TestRecord getTestRecord(Map<String, Object> values) {
    TestRecord record = new TestRecord();
    record.setId((Long) values.get("id"));
    record.setName((String) values.get("name"));
    record.setCategory((String) values.get("category"));
    record.setDate((LocalDate) values.get("date"));
    record.setIntValue((Integer) values.get("int_col"));
    record.setLongValue((Long) values.get("long_col"));
    record.setFloatValue((Float) values.get("float_col"));
    record.setDoubleValue((Double) values.get("double_col"));
    record.setDecimalValue((BigDecimal) values.get("decimal_col"));
    record.setBooleanValue((Boolean) values.get("bool_col"));
    record.setTimestampValue((LocalDateTime) values.get("timestamp_col"));
    record.setOffsetDateTimeValue((OffsetDateTime) values.get("timestamptz_col"));
    record.setLocalTimeValue((LocalTime) values.get("time_col"));
    if (values.get("struct_col") != null) {
      record.setStructValue(new TestRecord.NestedStruct());
      record
          .getStructValue()
          .setIntValue((Integer) ((Map) values.get("struct_col")).get("nested_int"));
      record
          .getStructValue()
          .setStringValue((String) ((Map) values.get("struct_col")).get("nested_string"));
    }
    return record;
  }

  @Test
  void testTimeTravel() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "category", Types.StringType.get()),
            Types.NestedField.required(4, "date", Types.DateType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("date").build();

    TableIdentifier tableIdentifier = TableIdentifier.of("test_db", "time_travel_query_test_table");
    swiftLakeEngine.getCatalog().createTable(tableIdentifier, schema, spec);
    String tableName = tableIdentifier.toString();

    // Insert test data into main table
    List<Map<String, Object>> inputData = new ArrayList<>();
    inputData.add(
        Map.of("id", 1L, "name", "John", "category", "A", "date", LocalDate.parse("2025-01-01")));
    inputData.add(
        Map.of("id", 2L, "name", "Jane", "category", "B", "date", LocalDate.parse("2025-01-02")));
    inputData.add(
        Map.of("id", 3L, "name", "Bob", "category", "A", "date", LocalDate.parse("2025-02-01")));
    inputData.add(
        Map.of("id", 4L, "name", "Alice", "category", "B", "date", LocalDate.parse("2025-02-02")));

    swiftLakeEngine
        .insertInto(tableName)
        .sql(TestUtil.createSelectSql(inputData, schema))
        .processSourceTables(false)
        .execute();

    LocalDateTime beforeUpdateTime = LocalDateTime.now();
    long beforeSnapshotId = swiftLakeEngine.getTable(tableName).currentSnapshot().snapshotId();
    // Create a new branch
    swiftLakeEngine.getTable(tableName).manageSnapshots().createBranch("test-branch").commit();
    // Create a new tag
    swiftLakeEngine
        .getTable(tableName)
        .manageSnapshots()
        .createTag("test-tag", beforeSnapshotId)
        .commit();

    // Make an update
    swiftLakeEngine
        .update(tableName)
        .conditionSql("id = 1")
        .updateSets(Map.of("name", "John Doe"))
        .execute();

    // Query using time travel to before the update
    String sql =
        "SELECT * FROM "
            + TestUtil.getTableNameForTimestamp(tableIdentifier, beforeUpdateTime)
            + " WHERE id = 1";
    List<Map<String, Object>> result = TestUtil.executeQuery(swiftLakeEngine, sql);

    assertThat(result).hasSize(1);
    assertThat(result.get(0))
        .containsEntry("id", 1L)
        .containsEntry("name", "John"); // Should be the old value

    // Query using time travel to before the update
    sql =
        "SELECT * FROM "
            + TestUtil.getTableNameForSnapshot(tableIdentifier, beforeSnapshotId)
            + " WHERE id = 1";
    result = TestUtil.executeQuery(swiftLakeEngine, sql);

    assertThat(result).hasSize(1);
    assertThat(result.get(0))
        .containsEntry("id", 1L)
        .containsEntry("name", "John"); // Should be the old value

    // Query using time travel to the test-branch (which should not have the update)
    sql =
        "SELECT * FROM "
            + TestUtil.getTableNameForBranch(tableIdentifier, "test-branch")
            + " WHERE id = 1";
    result = TestUtil.executeQuery(swiftLakeEngine, sql);

    assertThat(result).hasSize(1);
    assertThat(result.get(0))
        .containsEntry("id", 1L)
        .containsEntry("name", "John"); // Should be the old value

    // Query using time travel to the test-tag (which should not have the update)
    sql =
        "SELECT * FROM "
            + TestUtil.getTableNameForTag(tableIdentifier, "test-tag")
            + " WHERE id = 1";
    result = TestUtil.executeQuery(swiftLakeEngine, sql);

    assertThat(result).hasSize(1);
    assertThat(result.get(0))
        .containsEntry("id", 1L)
        .containsEntry("name", "John"); // Should be the old value

    TestUtil.dropIcebergTable(swiftLakeEngine, tableName);
  }

  @Test
  void testMyBatisIntegration() throws Exception {
    SwiftLakeMybatisConfiguration configuration = swiftLakeEngine.createMybatisConfiguration();
    configuration.addMapper(TestMapper.class);
    SqlSessionFactory sqlSessionFactory = swiftLakeEngine.createSqlSessionFactory(configuration);

    try (SqlSession session = sqlSessionFactory.openSession()) {
      TestMapper mapper = session.getMapper(TestMapper.class);
      List<TestRecord> result = mapper.getDataById(mainTableName, 1L);
      TestRecord expected =
          getTestRecord(createTestRecord(1L, "John", "A", LocalDate.parse("2025-01-01"), true));
      assertThat(result)
          .usingRecursiveFieldByFieldElementComparator()
          .containsExactlyInAnyOrderElementsOf(Arrays.asList(expected));

      result = mapper.getDataById(mainTableName, 2L);
      expected =
          getTestRecord(createTestRecord(2L, "Jane", "B", LocalDate.parse("2025-01-02"), true));
      assertThat(result)
          .usingRecursiveFieldByFieldElementComparator()
          .containsExactlyInAnyOrderElementsOf(Arrays.asList(expected));

      result = mapper.getDataById(mainTableName, 3L);
      expected =
          getTestRecord(createTestRecord(3L, "Bob", "A", LocalDate.parse("2025-02-01"), true));
      assertThat(result)
          .usingRecursiveFieldByFieldElementComparator()
          .containsExactlyInAnyOrderElementsOf(Arrays.asList(expected));

      result = mapper.getDataById(mainTableName, 4L);
      expected =
          getTestRecord(createTestRecord(4L, "Alice", "B", LocalDate.parse("2025-02-02"), true));
      assertThat(result)
          .usingRecursiveFieldByFieldElementComparator()
          .containsExactlyInAnyOrderElementsOf(Arrays.asList(expected));

      result = mapper.getDataById(mainTableName, 5L);
      expected = getTestRecord(createTestRecordWithNullValues(5L));
      assertThat(result)
          .usingRecursiveFieldByFieldElementComparator()
          .containsExactlyInAnyOrderElementsOf(Arrays.asList(expected));
    }
  }

  @Test
  void testMyBatisIntegrationWithXML() {
    SqlSessionFactory sqlSessionFactory = swiftLakeEngine.getSqlSessionFactory();

    try (SqlSession session = sqlSessionFactory.openSession()) {
      List<TestRecord> result =
          session.selectList(
              "TestMapper.getDataById", Map.of("tableName", mainTableName, "id", 1L));
      TestRecord expected =
          getTestRecord(createTestRecord(1L, "John", "A", LocalDate.parse("2025-01-01"), true));
      assertThat(result)
          .usingRecursiveFieldByFieldElementComparator()
          .containsExactlyInAnyOrderElementsOf(Arrays.asList(expected));

      result =
          session.selectList(
              "TestMapper.getDataById", Map.of("tableName", mainTableName, "id", 2L));
      expected =
          getTestRecord(createTestRecord(2L, "Jane", "B", LocalDate.parse("2025-01-02"), true));
      assertThat(result)
          .usingRecursiveFieldByFieldElementComparator()
          .containsExactlyInAnyOrderElementsOf(Arrays.asList(expected));

      result =
          session.selectList(
              "TestMapper.getDataById", Map.of("tableName", mainTableName, "id", 3L));
      expected =
          getTestRecord(createTestRecord(3L, "Bob", "A", LocalDate.parse("2025-02-01"), true));
      assertThat(result)
          .usingRecursiveFieldByFieldElementComparator()
          .containsExactlyInAnyOrderElementsOf(Arrays.asList(expected));

      result =
          session.selectList(
              "TestMapper.getDataById", Map.of("tableName", mainTableName, "id", 4L));
      expected =
          getTestRecord(createTestRecord(4L, "Alice", "B", LocalDate.parse("2025-02-02"), true));
      assertThat(result)
          .usingRecursiveFieldByFieldElementComparator()
          .containsExactlyInAnyOrderElementsOf(Arrays.asList(expected));

      result =
          session.selectList(
              "TestMapper.getDataById", Map.of("tableName", mainTableName, "id", 5L));
      expected = getTestRecord(createTestRecordWithNullValues(5L));
      assertThat(result)
          .usingRecursiveFieldByFieldElementComparator()
          .containsExactlyInAnyOrderElementsOf(Arrays.asList(expected));
    }
  }

  @Test
  void testMyBatisIntegrationWithPreparedStatement() {
    SwiftLakeMybatisConfiguration configuration = swiftLakeEngine.createMybatisConfiguration();
    configuration.addMapper(TestMapper.class);
    SqlSessionFactory sqlSessionFactory = swiftLakeEngine.createSqlSessionFactory(configuration);

    try (SqlSession session = sqlSessionFactory.openSession()) {
      TestMapper mapper = session.getMapper(TestMapper.class);
      TestRecord expected =
          getTestRecord(createTestRecord(2L, "Jane", "B", LocalDate.parse("2025-01-02"), true));
      List<TestRecord> result = mapper.getDataByRecord(mainTableName, expected);
      assertThat(result)
          .usingRecursiveFieldByFieldElementComparator()
          .containsExactlyInAnyOrderElementsOf(Arrays.asList(expected));
    }
  }

  @Test
  void testMyBatisIntegrationWithXMLAndPreparedStatement() {
    SqlSessionFactory sqlSessionFactory = swiftLakeEngine.getSqlSessionFactory();

    try (SqlSession session = sqlSessionFactory.openSession()) {
      TestRecord expected =
          getTestRecord(createTestRecord(2L, "Jane", "B", LocalDate.parse("2025-01-02"), true));
      List<TestRecord> result =
          session.selectList(
              "TestMapper.getDataByRecord", Map.of("tableName", mainTableName, "record", expected));
      assertThat(result)
          .usingRecursiveFieldByFieldElementComparator()
          .containsExactlyInAnyOrderElementsOf(Arrays.asList(expected));
    }
  }

  interface TestMapper {
    @Results(
        id = "userResult",
        value = {
          @Result(property = "id", column = "id"),
          @Result(property = "name", column = "name"),
          @Result(property = "category", column = "category"),
          @Result(property = "date", column = "date"),
          @Result(property = "intValue", column = "int_col"),
          @Result(property = "longValue", column = "long_col"),
          @Result(property = "floatValue", column = "float_col"),
          @Result(property = "doubleValue", column = "double_col"),
          @Result(property = "decimalValue", column = "decimal_col"),
          @Result(property = "booleanValue", column = "bool_col"),
          @Result(property = "timestampValue", column = "timestamp_col"),
          @Result(property = "offsetDateTimeValue", column = "timestamptz_col"),
          @Result(property = "localTimeValue", column = "time_col"),
          @Result(property = "structValue.intValue", column = "nested_int"),
          @Result(property = "structValue.stringValue", column = "nested_string")
        })
    @Select(
        "SELECT id,name,category,date,int_col,long_col,float_col,double_col,decimal_col,bool_col,timestamp_col,timestamptz_col,time_col,struct_col.nested_int AS nested_int,struct_col.nested_string AS nested_string FROM ${tableName} WHERE id = ${id}")
    List<TestRecord> getDataById(@Param("tableName") String tableName, @Param("id") Long id);

    @Results(
        id = "userResult2",
        value = {
          @Result(property = "id", column = "id"),
          @Result(property = "name", column = "name"),
          @Result(property = "category", column = "category"),
          @Result(property = "date", column = "date"),
          @Result(property = "intValue", column = "int_col"),
          @Result(property = "longValue", column = "long_col"),
          @Result(property = "floatValue", column = "float_col"),
          @Result(property = "doubleValue", column = "double_col"),
          @Result(property = "decimalValue", column = "decimal_col"),
          @Result(property = "booleanValue", column = "bool_col"),
          @Result(property = "timestampValue", column = "timestamp_col"),
          @Result(property = "offsetDateTimeValue", column = "timestamptz_col"),
          @Result(property = "localTimeValue", column = "time_col"),
          @Result(property = "structValue.intValue", column = "nested_int"),
          @Result(property = "structValue.stringValue", column = "nested_string")
        })
    @Select(
        "SELECT id,name,category,date,int_col,long_col,float_col,double_col,decimal_col,bool_col,timestamp_col,timestamptz_col,time_col,struct_col.nested_int AS nested_int,struct_col.nested_string AS nested_string FROM ${tableName}"
            + " WHERE id=#{record.id} AND name=#{record.name} AND category=#{record.category} AND date=#{record.date} AND int_col=#{record.intValue} AND long_col=#{record.longValue} AND float_col=#{record.floatValue} AND double_col=#{record.doubleValue}"
            + " AND decimal_col=#{record.decimalValue} AND bool_col=#{record.booleanValue} AND timestamp_col=#{record.timestampValue} AND timestamptz_col=#{record.offsetDateTimeValue} AND time_col=#{record.localTimeValue}"
            + " AND struct_col.nested_int=#{record.structValue.intValue} AND struct_col.nested_string=#{record.structValue.stringValue}")
    List<TestRecord> getDataByRecord(
        @Param("tableName") String tableName, @Param("record") TestRecord record);
  }
}
