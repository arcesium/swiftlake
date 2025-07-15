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
package com.arcesium.swiftlake.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expressions;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.types.Types;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SqlQueryProcessorTest {
  private static DuckDBConnection duckDBConnection;

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private Connection mockConnection;

  @Mock private Table mockTable;

  @Mock private Catalog mockCatalog;

  private SqlQueryProcessor sqlQueryProcessor;

  @BeforeAll
  static void setUpAll() throws SQLException {
    duckDBConnection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
  }

  @AfterAll
  static void tearDownAll() throws SQLException {
    if (duckDBConnection != null) {
      duckDBConnection.close();
    }
  }

  @BeforeEach
  void setUp() {
    sqlQueryProcessor = new SqlQueryProcessor(mockSwiftLakeEngine);
  }

  @Test
  void testParseConditionExpression() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()),
            Types.NestedField.required(4, "active", Types.BooleanType.get()));

    assertThat(
            Expressions.resolveExpression(
                    sqlQueryProcessor.parseConditionExpression("id = 1", schema, null))
                .toString())
        .isEqualTo("ref(name=\"id\") == 1");
    assertThat(
            Expressions.resolveExpression(
                    sqlQueryProcessor.parseConditionExpression("name = 'John'", schema, null))
                .toString())
        .isEqualTo("ref(name=\"name\") == \"John\"");
    assertThat(
            Expressions.resolveExpression(
                    sqlQueryProcessor.parseConditionExpression(
                        "age > 18 AND active = true", schema, null))
                .toString())
        .isEqualTo("(ref(name=\"age\") > 18 and ref(name=\"active\") == true)");
    assertThat(
            Expressions.resolveExpression(
                    sqlQueryProcessor.parseConditionExpression("id IN (1, 2, 3)", schema, null))
                .toString())
        .isEqualTo("ref(name=\"id\") in (1, 2, 3)");
    assertThat(
            Expressions.resolveExpression(
                    sqlQueryProcessor.parseConditionExpression(
                        "age BETWEEN 20 AND 30", schema, null))
                .toString())
        .isEqualTo("(ref(name=\"age\") >= 20 and ref(name=\"age\") <= 30)");

    assertThatThrownBy(
            () -> sqlQueryProcessor.parseConditionExpression("invalid_column = 1", schema, null))
        .isInstanceOf(ValidationException.class);
  }

  @ParameterizedTest
  @MethodSource("provideQueries")
  void testProcessQueries(
      String sql,
      Map<Integer, Object> params,
      String expression,
      String preExecuteSql,
      String postExecuteSql)
      throws Exception {
    when(mockSwiftLakeEngine.getTable(anyString())).thenReturn(mockTable);
    int id = 0;
    when(mockTable.schema())
        .thenReturn(
            new Schema(
                Types.NestedField.required(++id, "id", Types.LongType.get()),
                Types.NestedField.required(++id, "name", Types.StringType.get()),
                Types.NestedField.required(++id, "age", Types.IntegerType.get()),
                Types.NestedField.required(++id, "flag", Types.BooleanType.get()),
                Types.NestedField.optional(
                    ++id,
                    "struct",
                    Types.StructType.of(
                        Arrays.asList(
                            Types.NestedField.optional(++id, "date", Types.DateType.get()),
                            Types.NestedField.optional(++id, "double", Types.DoubleType.get()),
                            Types.NestedField.optional(
                                ++id, "timestamp", Types.TimestampType.withoutZone()),
                            Types.NestedField.optional(
                                ++id,
                                "struct",
                                Types.StructType.of(
                                    Arrays.asList(
                                        Types.NestedField.optional(
                                            ++id,
                                            "offset_date_time",
                                            Types.TimestampType.withZone()),
                                        Types.NestedField.optional(
                                            ++id, "local_time", Types.TimeType.get()),
                                        Types.NestedField.optional(
                                            ++id, "big_decimal", Types.DecimalType.of(38, 10)),
                                        Types.NestedField.optional(
                                            ++id, "float", Types.FloatType.get())))))))));

    Pair<String, List<TableFilter>> result = null;
    try (Connection connection = duckDBConnection.duplicate()) {
      if (preExecuteSql != null) {
        try (Statement stmt = connection.createStatement()) {
          stmt.executeUpdate(preExecuteSql);
        }
      }
      try {
        result = sqlQueryProcessor.process(sql, params, connection, null);
      } finally {
        if (postExecuteSql != null) {
          try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(postExecuteSql);
          }
        }
      }
    }
    assertThat(result).isNotNull();
    assertThat(result.getRight()).hasSize(1);
    assertThat(result.getRight().get(0).getTableName()).isEqualTo("db.my_table");
    assertThat(Expressions.resolveExpression(result.getRight().get(0).getCondition()).toString())
        .isEqualTo(expression);
    assertThat(result.getLeft())
        .isEqualTo(sql.replace("db.my_table", result.getRight().get(0).getPlaceholder()));
  }

  private static Stream<Arguments> provideQueries() {
    return Stream.of(
        Arguments.of("SELECT * FROM db.my_table WHERE true", null, "true", null, null),
        Arguments.of("SELECT * FROM db.my_table WHERE false", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE flag", null, "ref(name=\"flag\") == true", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE flag = true",
            null,
            "ref(name=\"flag\") == true",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE false = flag",
            null,
            "ref(name=\"flag\") == false",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id = 1", null, "ref(name=\"id\") == 1", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE 1 = id", null, "ref(name=\"id\") == 1", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id != 1", null, "ref(name=\"id\") != 1", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE 1 != id", null, "ref(name=\"id\") != 1", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id <> 1", null, "ref(name=\"id\") != 1", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE 1 <> id", null, "ref(name=\"id\") != 1", null, null),
        Arguments.of(
            "SELECT name, age FROM db.my_table WHERE age > 18",
            null,
            "ref(name=\"age\") > 18",
            null,
            null),
        Arguments.of(
            "SELECT name, age FROM db.my_table WHERE age >= 18",
            null,
            "ref(name=\"age\") >= 18",
            null,
            null),
        Arguments.of(
            "SELECT name, age FROM db.my_table WHERE age < 18",
            null,
            "ref(name=\"age\") < 18",
            null,
            null),
        Arguments.of(
            "SELECT name, age FROM db.my_table WHERE age <= 18",
            null,
            "ref(name=\"age\") <= 18",
            null,
            null),
        Arguments.of(
            "SELECT name, age FROM db.my_table WHERE 18 > age",
            null,
            "ref(name=\"age\") < 18",
            null,
            null),
        Arguments.of(
            "SELECT name, age FROM db.my_table WHERE 18 >= age",
            null,
            "ref(name=\"age\") <= 18",
            null,
            null),
        Arguments.of(
            "SELECT name, age FROM db.my_table WHERE 18 < age",
            null,
            "ref(name=\"age\") > 18",
            null,
            null),
        Arguments.of(
            "SELECT name, age FROM db.my_table WHERE 18 <= age",
            null,
            "ref(name=\"age\") >= 18",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE age BETWEEN 20 AND 30",
            null,
            "(ref(name=\"age\") >= 20 and ref(name=\"age\") <= 30)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE age NOT BETWEEN 20 AND 30",
            null,
            "(ref(name=\"age\") < 20 or ref(name=\"age\") > 30)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id IS NULL",
            null,
            "is_null(ref(name=\"id\"))",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id IS NOT NULL",
            null,
            "not_null(ref(name=\"id\"))",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id IS DISTINCT FROM NULL",
            null,
            "not_null(ref(name=\"id\"))",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NULL IS DISTINCT FROM id",
            null,
            "not_null(ref(name=\"id\"))",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id IS NOT DISTINCT FROM NULL",
            null,
            "is_null(ref(name=\"id\"))",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NULL IS NOT DISTINCT FROM id",
            null,
            "is_null(ref(name=\"id\"))",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id IS DISTINCT FROM 1",
            null,
            "ref(name=\"id\") != 1",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE 1 IS DISTINCT FROM id",
            null,
            "ref(name=\"id\") != 1",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id IS NOT DISTINCT FROM 1",
            null,
            "ref(name=\"id\") == 1",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE 1 IS NOT DISTINCT FROM id",
            null,
            "ref(name=\"id\") == 1",
            null,
            null),
        Arguments.of("SELECT * FROM db.my_table WHERE age = NULL", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE age = ?",
            new HashMap<Integer, Object>() {
              {
                put(1, null);
              }
            },
            "false",
            null,
            null),
        Arguments.of("SELECT * FROM db.my_table WHERE age != NULL", null, "false", null, null),
        Arguments.of("SELECT * FROM db.my_table WHERE age > NULL", null, "false", null, null),
        Arguments.of("SELECT * FROM db.my_table WHERE age >= NULL", null, "false", null, null),
        Arguments.of("SELECT * FROM db.my_table WHERE age < NULL", null, "false", null, null),
        Arguments.of("SELECT * FROM db.my_table WHERE age <= NULL", null, "false", null, null),
        Arguments.of("SELECT * FROM db.my_table WHERE NULL != age", null, "false", null, null),
        Arguments.of("SELECT * FROM db.my_table WHERE NULL > age", null, "false", null, null),
        Arguments.of("SELECT * FROM db.my_table WHERE NULL >= age", null, "false", null, null),
        Arguments.of("SELECT * FROM db.my_table WHERE NULL < age", null, "false", null, null),
        Arguments.of("SELECT * FROM db.my_table WHERE NULL <= age", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE age BETWEEN 1 AND NULL", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE age BETWEEN NULL AND 2", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE age BETWEEN NULL AND NULL", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id IN (NULL)",
            null,
            "ref(name=\"id\") in ()",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id IN (NULL, NULL)",
            null,
            "ref(name=\"id\") in ()",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id IN (1, NULL)",
            null,
            "ref(name=\"id\") in (1)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id NOT IN (NULL, 1, NULL)", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NOT (id IN (NULL, 1, NULL))",
            null,
            "false",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NOT (id NOT IN (1, NULL))",
            null,
            "ref(name=\"id\") in (1)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id NOT IN (?, ?, ?)",
            new HashMap<Integer, Object>() {
              {
                put(1, null);
                put(2, 1);
                put(3, null);
              }
            },
            "false",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id IN (1, 2, 3)",
            null,
            "ref(name=\"id\") in (1, 2, 3)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id NOT IN (1, 2, 3)",
            null,
            "ref(name=\"id\") not in (1, 2, 3)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NOT flag",
            null,
            "ref(name=\"flag\") != true",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NOT (id IN (1, 2, 3))",
            null,
            "ref(name=\"id\") not in (1, 2, 3)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NOT (NULL <= age)", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE true OR (id = NULL)", null, "true", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE false OR (id = NULL)", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE (id = NULL) OR true", null, "true", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE (id = NULL) OR false", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id = 1 OR id = 2",
            null,
            "(ref(name=\"id\") == 1 or ref(name=\"id\") == 2)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE true AND (id = NULL)", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE false AND (id = NULL)", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE (id = NULL) AND true", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE (id = NULL) AND false", null, "false", null, null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE id = 1 AND id = 2",
            null,
            "(ref(name=\"id\") == 1 and ref(name=\"id\") == 2)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NOT id = 1 OR id = 2",
            null,
            "(ref(name=\"id\") != 1 or ref(name=\"id\") == 2)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NOT id = 1 AND id = 2",
            null,
            "(ref(name=\"id\") != 1 and ref(name=\"id\") == 2)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NOT id = 1 OR NOT id = 2",
            null,
            "(ref(name=\"id\") != 1 or ref(name=\"id\") != 2)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NOT id = 1 AND NOT id = 2",
            null,
            "(ref(name=\"id\") != 1 and ref(name=\"id\") != 2)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NOT (id = 1 OR id = 2)",
            null,
            "(ref(name=\"id\") != 1 and ref(name=\"id\") != 2)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NOT (id = 1 AND id = 2)",
            null,
            "(ref(name=\"id\") != 1 or ref(name=\"id\") != 2)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE NOT (id = ? AND id = ?)",
            Map.of(1, (Object) 1, 2, 2),
            "(ref(name=\"id\") != 1 or ref(name=\"id\") != 2)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE (id >= 1 AND (age < 10 OR age > 20 OR NOT (flag OR name != 'John' OR "
                + "id IN (4, 5, 6)) OR id = 2 OR NOT flag AND name IS NOT NULL OR (age BETWEEN 20 AND 30 AND id IS NOT DISTINCT FROM 1 AND NOT flag)))",
            null,
            "(ref(name=\"id\") >= 1 and (((((ref(name=\"age\") < 10 or ref(name=\"age\") > 20) or ((ref(name=\"flag\") != true and ref(name=\"name\") == \"John\") "
                + "and ref(name=\"id\") not in (4, 5, 6))) or ref(name=\"id\") == 2) or (ref(name=\"flag\") != true and not_null(ref(name=\"name\")))) "
                + "or (((ref(name=\"age\") >= 20 and ref(name=\"age\") <= 30) and ref(name=\"id\") == 1) and ref(name=\"flag\") != true)))",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE struct.date = '2025-01-31' AND flag AND id IN (1, 3, 5) OR name = 'Jane' OR struct.double < 12.908 AND "
                + "NOT (struct.timestamp IS NOT NULL AND (flag = true OR flag = false) AND NOT flag AND struct.timestamp > TIMESTAMP '2021-09-09T12:31:12.129789') "
                + "AND NOT (NOT (NOT (struct.struct.local_time <= TIME '12:31:12.129789'))) AND ((struct.struct.offset_date_time >= TIMESTAMPTZ '2021-09-09T12:31:12.129789+05:30')) "
                + "OR id = 12312 OR struct.struct.float != 0.0000011 AND TIMESTAMP '2021-09-09T12:31:12.129789' BETWEEN struct.timestamp AND struct.timestamp AND "
                + "TIMESTAMP '2021-09-09T12:31:12.129789' NOT BETWEEN struct.timestamp AND struct.timestamp AND struct.date BETWEEN DATE '2012-01-01' AND DATE '2050-02-02' "
                + "AND struct.date NOT BETWEEN '2012-01-01' AND '2050-02-02' OR struct.struct.big_decimal > 0.12345",
            null,
            "(((((((ref(name=\"struct.date\") == \"2025-01-31\" and ref(name=\"flag\") == true) and ref(name=\"id\") in (1, 3, 5)) "
                + "or ref(name=\"name\") == \"Jane\") or (((ref(name=\"struct.double\") < 12.908 and (((is_null(ref(name=\"struct.timestamp\")) "
                + "or (ref(name=\"flag\") != true and ref(name=\"flag\") != false)) or ref(name=\"flag\") == true) "
                + "or ref(name=\"struct.timestamp\") <= \"2021-09-09T12:31:12.129789\")) and ref(name=\"struct.struct.local_time\") > \"12:31:12.129789\") "
                + "and ref(name=\"struct.struct.offset_date_time\") >= \"2021-09-09T12:31:12.129789+05:30\")) or ref(name=\"id\") == 12312) "
                + "or ((((ref(name=\"struct.struct.float\") != 1.1E-6 and (ref(name=\"struct.timestamp\") <= \"2021-09-09T12:31:12.129789\" "
                + "and ref(name=\"struct.timestamp\") >= \"2021-09-09T12:31:12.129789\")) and (ref(name=\"struct.timestamp\") > \"2021-09-09T12:31:12.129789\" "
                + "or ref(name=\"struct.timestamp\") < \"2021-09-09T12:31:12.129789\")) and (ref(name=\"struct.date\") >= \"2012-01-01\" "
                + "and ref(name=\"struct.date\") <= \"2050-02-02\")) and (ref(name=\"struct.date\") < \"2012-01-01\" or ref(name=\"struct.date\") > \"2050-02-02\"))) "
                + "or ref(name=\"struct.struct.big_decimal\") > 0.12345)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE struct.date = ? AND flag AND id IN (?, ?, ?) OR name = ? OR struct.double < ? AND "
                + "NOT (struct.timestamp IS NOT NULL AND (flag = ? OR flag = ?) AND NOT flag AND struct.timestamp > ?) "
                + "AND NOT (NOT (NOT (struct.struct.local_time <= ?))) AND ((struct.struct.offset_date_time >= ?)) "
                + "OR id = ? OR struct.struct.float != ? AND ? BETWEEN struct.timestamp AND struct.timestamp AND "
                + "? NOT BETWEEN struct.timestamp AND struct.timestamp AND struct.date BETWEEN ? AND ? AND struct.date NOT BETWEEN ? AND ? "
                + "OR struct.struct.big_decimal > ?",
            new HashMap<Integer, Object>() {
              {
                put(1, LocalDate.parse("2025-01-31"));
                put(2, 1);
                put(3, 3);
                put(4, 5);
                put(5, "Jane");
                put(6, 12.908);
                put(7, true);
                put(8, false);
                put(9, LocalDateTime.parse("2021-09-09T12:31:12.129789"));
                put(10, LocalTime.parse("12:31:12.129789"));
                put(11, OffsetDateTime.parse("2021-09-09T12:31:12.129789+05:30"));
                put(12, 12312L);
                put(13, 0.0000011f);
                put(14, LocalDateTime.parse("2021-09-09T12:31:12.129789"));
                put(15, Timestamp.valueOf("2021-09-09 12:31:12.129789"));
                put(16, LocalDate.parse("2012-01-01"));
                put(17, Date.valueOf("2050-02-02"));
                put(18, LocalDate.parse("2012-01-01"));
                put(19, LocalDate.parse("2050-02-02"));
                put(20, new BigDecimal("0.12345"));
              }
            },
            "(((((((ref(name=\"struct.date\") == \"2025-01-31\" and ref(name=\"flag\") == true) and ref(name=\"id\") in (1, 3, 5)) "
                + "or ref(name=\"name\") == \"Jane\") or (((ref(name=\"struct.double\") < 12.908 and (((is_null(ref(name=\"struct.timestamp\")) "
                + "or (ref(name=\"flag\") != true and ref(name=\"flag\") != false)) or ref(name=\"flag\") == true) or ref(name=\"struct.timestamp\") <= \"2021-09-09T12:31:12.129789\")) "
                + "and ref(name=\"struct.struct.local_time\") > \"12:31:12.129789\") and ref(name=\"struct.struct.offset_date_time\") >= \"2021-09-09T12:31:12.129789+05:30\")) "
                + "or ref(name=\"id\") == 12312) or ((((ref(name=\"struct.struct.float\") != 1.1E-6 and (ref(name=\"struct.timestamp\") <= \"2021-09-09T12:31:12.129789\" "
                + "and ref(name=\"struct.timestamp\") >= \"2021-09-09T12:31:12.129789\")) and (ref(name=\"struct.timestamp\") > \"2021-09-09T12:31:12.129789\" "
                + "or ref(name=\"struct.timestamp\") < \"2021-09-09T12:31:12.129789\")) and (ref(name=\"struct.date\") >= \"2012-01-01\" and ref(name=\"struct.date\") <= \"2050-02-02\")) "
                + "and (ref(name=\"struct.date\") < \"2012-01-01\" or ref(name=\"struct.date\") > \"2050-02-02\"))) or ref(name=\"struct.struct.big_decimal\") > 0.12345)",
            null,
            null),
        Arguments.of(
            "SELECT * FROM db.my_table a JOIN filter_table b "
                + "ON (a.id = b.id AND a.id != b.id AND a.id < b.id AND a.id > b.id AND a.id <= b.id AND a.id >= b.id AND a.id IS DISTINCT FROM b.id AND a.id IS NOT DISTINCT FROM b.id AND a.id BETWEEN b.id AND b.id)",
            null,
            "((((((((ref(name=\"id\") == 1 and ref(name=\"id\") != 1) and ref(name=\"id\") < 1) and ref(name=\"id\") > 1) and ref(name=\"id\") <= 1) "
                + "and ref(name=\"id\") >= 1) and ref(name=\"id\") != 1) and ref(name=\"id\") == 1) and (ref(name=\"id\") >= 1 and ref(name=\"id\") <= 1))",
            "CREATE TEMP TABLE filter_table AS (SELECT * FROM (SELECT 1 AS id UNION SELECT NULL) ORDER BY id)",
            "DROP TABLE filter_table"),
        Arguments.of(
            "SELECT * FROM db.my_table a"
                + " JOIN filter_table c "
                + "ON (c.id = a.id AND c.id != a.id AND c.id > a.id AND c.id < a.id AND c.id >= a.id AND c.id <= a.id AND c.id IS DISTINCT FROM a.id AND c.id IS NOT DISTINCT FROM a.id)",
            null,
            "(((((((ref(name=\"id\") == 1 and ref(name=\"id\") != 1) and ref(name=\"id\") < 1) and ref(name=\"id\") > 1) and ref(name=\"id\") <= 1) "
                + "and ref(name=\"id\") >= 1) and ref(name=\"id\") != 1) and ref(name=\"id\") == 1)",
            "CREATE TEMP TABLE filter_table AS (SELECT * FROM (SELECT 1 AS id UNION SELECT NULL) ORDER BY id)",
            "DROP TABLE filter_table"),
        Arguments.of(
            "SELECT * FROM db.my_table a JOIN filter_table b "
                + "ON (a.id = b.id AND a.id != b.id AND a.id < b.id AND a.id > b.id AND a.id <= b.id AND a.id >= b.id AND a.id IS DISTINCT FROM b.id AND a.id IS NOT DISTINCT FROM b.id AND a.id BETWEEN b.id AND b.id)",
            null,
            "(((((((((ref(name=\"id\") == 1 and ref(name=\"id\") != 1) and ref(name=\"id\") < 1) and ref(name=\"id\") > 1) and ref(name=\"id\") <= 1) and ref(name=\"id\") >= 1) and ref(name=\"id\") != 1) and ref(name=\"id\") == 1) "
                + "and (ref(name=\"id\") >= 1 and ref(name=\"id\") <= 1)) or ((((((((ref(name=\"id\") == 999 and ref(name=\"id\") != 999) and ref(name=\"id\") < 999) and ref(name=\"id\") > 999) and ref(name=\"id\") <= 999) "
                + "and ref(name=\"id\") >= 999) and ref(name=\"id\") != 999) and ref(name=\"id\") == 999) and (ref(name=\"id\") >= 999 and ref(name=\"id\") <= 999)))",
            "CREATE TEMP TABLE filter_table AS (SELECT * FROM (SELECT 1 AS id UNION SELECT 999) ORDER BY id)",
            "DROP TABLE filter_table"),
        Arguments.of(
            "SELECT * FROM db.my_table a"
                + " JOIN filter_table c "
                + "ON (c.id = a.id AND c.id != a.id AND c.id > a.id AND c.id < a.id AND c.id >= a.id AND c.id <= a.id AND c.id IS DISTINCT FROM a.id AND c.id IS NOT DISTINCT FROM a.id)",
            null,
            "((((((((ref(name=\"id\") == 1 and ref(name=\"id\") != 1) and ref(name=\"id\") < 1) and ref(name=\"id\") > 1) and ref(name=\"id\") <= 1) and ref(name=\"id\") >= 1) and ref(name=\"id\") != 1) and ref(name=\"id\") == 1) "
                + "or (((((((ref(name=\"id\") == 999 and ref(name=\"id\") != 999) and ref(name=\"id\") < 999) and ref(name=\"id\") > 999) and ref(name=\"id\") <= 999) and ref(name=\"id\") >= 999) and ref(name=\"id\") != 999) and ref(name=\"id\") == 999))",
            "CREATE TEMP TABLE filter_table AS (SELECT * FROM (SELECT 1 AS id UNION SELECT 999) ORDER BY id)",
            "DROP TABLE filter_table"));
  }

  @Test
  void testProcessWithInvalidTable() {
    String sql = "SELECT * FROM db.non_existent_table";
    when(mockSwiftLakeEngine.getTable(anyString()))
        .thenThrow(new SwiftLakeException(new RuntimeException(), "Table not found"));

    assertThatThrownBy(() -> sqlQueryProcessor.process(sql, mockConnection))
        .isInstanceOf(SwiftLakeException.class);
  }

  @ParameterizedTest
  @CsvSource({
    "db.my_table$timestamp_2023-01-01T00:00:00, db.my_table, 2023-01-01T00:00:00",
    "db.my_table$branch_main, db.my_table, main",
    "db.my_table$tag_v1.0, db.my_table, v1.0",
    "db.my_table$snapshot_123456, db.my_table, 123456"
  })
  void testParseTimeTravelOptions(
      String tableName, String expectedTableName, String expectedOption) {
    Pair<String, TimeTravelOptions> result = sqlQueryProcessor.parseTimeTravelOptions(tableName);

    assertThat(result).isNotNull();
    assertThat(result.getLeft()).isEqualTo(expectedTableName);
    assertThat(result.getRight()).isNotNull();

    TimeTravelOptions options = result.getRight();
    if (expectedOption.contains("-")) {
      assertThat(options.getTimestamp()).isEqualTo(LocalDateTime.parse(expectedOption));
    } else if (expectedOption.matches("\\d+")) {
      assertThat(options.getSnapshotId()).isEqualTo(Long.parseLong(expectedOption));
    } else {
      assertThat(options.getBranchOrTagName()).isEqualTo(expectedOption);
    }
  }

  @Test
  void testGetTableName() {
    when(mockTable.name()).thenReturn("my_catalog.my_schema.db.my_table");
    when(mockSwiftLakeEngine.getCatalog()).thenReturn(mockCatalog);
    when(mockCatalog.name()).thenReturn("my_catalog");
    String result = sqlQueryProcessor.getTableName(mockTable);
    assertThat(result).isEqualTo("my_schema.db.my_table");

    when(mockTable.name()).thenReturn("my_schema.db.my_table");
    result = sqlQueryProcessor.getTableName(mockTable);
    assertThat(result).isEqualTo("my_schema.db.my_table");
  }

  @Test
  void testProcessWithMultipleParseBlocks() throws Exception {
    String sql =
        "--SWIFTLAKE_PARSE_BEGIN--\n"
            + "SELECT * FROM db.table1 WHERE id > 10 \n"
            + "--SWIFTLAKE_PARSE_END--\n"
            + "UNION ALL SELECT * FROM db.table2 WHERE name = 'John' UNION ALL\n"
            + "--SWIFTLAKE_PARSE_BEGIN--\n"
            + "SELECT * FROM db.table3 WHERE age < 30\n"
            + "--SWIFTLAKE_PARSE_END--\n";

    when(mockSwiftLakeEngine.getTable(anyString())).thenReturn(mockTable);
    when(mockTable.schema())
        .thenReturn(
            new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "age", Types.IntegerType.get())));

    Pair<String, List<TableFilter>> result = sqlQueryProcessor.process(sql, mockConnection);

    assertThat(result).isNotNull();
    assertThat(result.getRight()).hasSize(2);
    assertThat(result.getRight().get(0).getTableName()).isEqualTo("db.table1");
    assertThat(Expressions.resolveExpression(result.getRight().get(0).getCondition()).toString())
        .isEqualTo("ref(name=\"id\") > 10");
    assertThat(result.getRight().get(1).getTableName()).isEqualTo("db.table3");
    assertThat(Expressions.resolveExpression(result.getRight().get(1).getCondition()).toString())
        .isEqualTo("ref(name=\"age\") < 30");
    assertThat(result.getLeft())
        .isEqualTo(
            "SELECT * FROM db.table1 WHERE id > 10"
                    .replace("db.table1", result.getRight().get(0).getPlaceholder())
                + "\nUNION ALL SELECT * FROM db.table2 WHERE name = 'John' UNION ALL\n"
                + "SELECT * FROM db.table3 WHERE age < 30\n\n"
                    .replace("db.table3", result.getRight().get(1).getPlaceholder()));
  }

  @Test
  void testProcessWithUnion() throws Exception {
    String sql =
        "SELECT * FROM db.table1 WHERE id < 100 UNION SELECT * FROM db.table2 WHERE id > 200";

    when(mockSwiftLakeEngine.getTable(anyString())).thenReturn(mockTable);
    when(mockTable.schema())
        .thenReturn(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));

    Pair<String, List<TableFilter>> result = sqlQueryProcessor.process(sql, mockConnection);

    assertThat(result).isNotNull();
    assertThat(result.getRight()).hasSize(2);
    assertThat(result.getRight().get(0).getTableName()).isEqualTo("db.table1");
    assertThat(Expressions.resolveExpression(result.getRight().get(0).getCondition()).toString())
        .isEqualTo("ref(name=\"id\") < 100");
    assertThat(result.getRight().get(1).getTableName()).isEqualTo("db.table2");
    assertThat(Expressions.resolveExpression(result.getRight().get(1).getCondition()).toString())
        .isEqualTo("ref(name=\"id\") > 200");
    assertThat(result.getLeft())
        .isEqualTo(
            sql.replace("db.table1", result.getRight().get(0).getPlaceholder())
                .replace("db.table2", result.getRight().get(1).getPlaceholder()));
  }

  @Test
  void testProcessWithComplexDataTypes() throws Exception {
    String sql =
        "SELECT * FROM db.complex_table WHERE "
            + "array_column[0] > 10 AND "
            + "map_column['key'] = 'value' AND "
            + "struct_column.nested_field = true";

    when(mockSwiftLakeEngine.getTable(anyString())).thenReturn(mockTable);
    when(mockTable.schema())
        .thenReturn(
            new Schema(
                Types.NestedField.required(
                    1, "array_column", Types.ListType.ofRequired(10, Types.IntegerType.get())),
                Types.NestedField.required(
                    2,
                    "map_column",
                    Types.MapType.ofRequired(
                        20, 21, Types.StringType.get(), Types.StringType.get())),
                Types.NestedField.required(
                    3,
                    "struct_column",
                    Types.StructType.of(
                        Types.NestedField.required(30, "nested_field", Types.BooleanType.get())))));

    assertThatThrownBy(() -> sqlQueryProcessor.process(sql, mockConnection))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining(
            "Conditions on List and Map types are not supported in table-level filtering. Please keep these conditions out of the table filter.");

    String updatedSql =
        "SELECT * FROM (SELECT * FROM db.complex_table WHERE struct_column.nested_field = true) WHERE "
            + "array_column[0] > 10 AND "
            + "map_column['key'] = 'value'";

    Pair<String, List<TableFilter>> result = sqlQueryProcessor.process(updatedSql, mockConnection);
    assertThat(result).isNotNull();
    assertThat(result.getRight()).hasSize(1);
    assertThat(result.getRight().get(0).getTableName()).isEqualTo("db.complex_table");
    assertThat(Expressions.resolveExpression(result.getRight().get(0).getCondition()).toString())
        .isEqualTo("ref(name=\"struct_column.nested_field\") == true");
    assertThat(result.getLeft())
        .isEqualTo(
            "SELECT * FROM (SELECT * FROM "
                + result.getRight().get(0).getPlaceholder()
                + " WHERE struct_column.nested_field = true) WHERE array_column[0] > 10 AND map_column['key'] = 'value'");
  }

  @Test
  void testProcessWithInvalidSyntax() {
    String sql = "SELECT * FORM db.invalid_table";

    assertThatThrownBy(() -> sqlQueryProcessor.process(sql, mockConnection))
        .isInstanceOf(SwiftLakeException.class);
  }

  @ParameterizedTest
  @MethodSource("provideUnsupportedConditions")
  void testProcessWithUnsupportedConditions(
      String sql, Class<?> expectionClass, String exceptionMessage) {
    when(mockSwiftLakeEngine.getTable(anyString())).thenReturn(mockTable);
    when(mockTable.schema())
        .thenReturn(
            new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "age", Types.IntegerType.get())));

    assertThatThrownBy(() -> sqlQueryProcessor.process(sql, mockConnection))
        .isInstanceOf(expectionClass)
        .hasMessageContaining(exceptionMessage);
  }

  private static Stream<Arguments> provideUnsupportedConditions() {
    return Stream.of(
        Arguments.of(
            "SELECT * FROM db.my_table WHERE 1=1",
            ValidationException.class,
            "For comparison one side must be a column name and other side must be a literal value"),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE 1 BETWEEN id AND 2",
            ValidationException.class,
            "For comparison one side must be a column name and other side must be a literal value"),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE length(name)=10",
            ValidationException.class,
            "For comparison one side must be a column name and other side must be a literal value"),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE name NOT LIKE 'J%'",
            ValidationException.class, "Unsupported Expression name NOT LIKE 'J%'"),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE name LIKE 'J%'",
            ValidationException.class, "Unsupported Expression name LIKE 'J%'"),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE array_column[0] > 10",
            ValidationException.class,
            "Conditions on List and Map types are not supported in table-level filtering. Please keep these conditions out of the table filter."),
        Arguments.of(
            "SELECT * FROM db.my_table WHERE map_column['key'] = 'value'",
            ValidationException.class,
            "Conditions on List and Map types are not supported in table-level filtering. Please keep these conditions out of the table filter."));
  }
}
