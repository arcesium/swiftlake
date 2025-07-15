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
package com.arcesium.swiftlake.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.TestUtil;
import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DataImportUtilIntegrationTest {

  private static SwiftLakeEngine swiftLakeEngine;
  private static Connection duckDBConnection;
  @TempDir private static Path basePath;

  @BeforeAll
  static void setUpAll() throws SQLException {
    swiftLakeEngine = TestUtil.createSwiftLakeEngine(basePath.toString());
    duckDBConnection = DriverManager.getConnection("jdbc:duckdb:");
  }

  @AfterAll
  static void tearDownAll() throws SQLException {
    if (duckDBConnection != null) {
      duckDBConnection.close();
    }
    if (swiftLakeEngine != null) {
      swiftLakeEngine.close();
    }
  }

  @Test
  void testWriteResultSetToParquetFile() throws Exception {
    try (Statement stmt = duckDBConnection.createStatement()) {
      stmt.execute(
          "CREATE TABLE test_table ("
              + "id INTEGER, integer_col INTEGER, bigint_col BIGINT, "
              + "double_col DOUBLE, decimal_col DECIMAL(38,9), "
              + "string_col VARCHAR(10), text_col TEXT, bool_col BOOLEAN, "
              + "date_col DATE, time_col TIME, timestamp_col TIMESTAMP)");
      stmt.execute(
          "INSERT INTO test_table VALUES "
              + "(1, 121, 121231231, 121.1214, 1241.1231, '%2Fasf%2F', 'type1\\0$**/~`!@#$%^&*()-_+=|?.,<>', true, '2024-01-01', '04:05:06', '2004-10-19 10:23:54.123456'), "
              + "(2, 10000, 14500000, 456.789, 47851.1452, '\"\"\"''''', 'type<>?:{1}1||[]...%#@!~````-_+=+;,.', false, '2024-02-01', '01:02:03', '2024-10-19 01:02:03.456789'), "
              + "(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
    }

    File parquetFile = null;
    try {
      parquetFile = File.createTempFile("test", ".parquet");

      try (Statement stmt = duckDBConnection.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM test_table WHERE False")) {
        DataImportUtil.writeResultSetToParquetFile(rs, parquetFile.getAbsolutePath());
      }
      assertThat(parquetFile).exists().isFile().isNotEmpty();
      List<Map<String, Object>> data = readParquetFile(parquetFile.getAbsolutePath());
      assertThat(data).hasSize(0);

      try (Statement stmt = duckDBConnection.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
        DataImportUtil.writeResultSetToParquetFile(rs, parquetFile.getAbsolutePath());
      }

      assertThat(parquetFile).exists().isFile().isNotEmpty();

      data = readParquetFile(parquetFile.getAbsolutePath());
      assertThat(data).hasSize(3);
      assertThat(data.get(0))
          .containsEntry("id", 1)
          .containsEntry("integer_col", 121)
          .containsEntry("bigint_col", 121231231L)
          .containsEntry("double_col", 121.1214)
          .containsEntry(
              "decimal_col", new BigDecimal("1241.1231").setScale(9, RoundingMode.HALF_UP))
          .containsEntry("string_col", "%2Fasf%2F")
          .containsEntry("text_col", "type1\\0$**/~`!@#$%^&*()-_+=|?.,<>")
          .containsEntry("bool_col", true)
          .containsEntry("date_col", LocalDate.of(2024, 1, 1))
          .containsEntry("time_col", LocalTime.of(4, 5, 6))
          .containsEntry("timestamp_col", LocalDateTime.of(2004, 10, 19, 10, 23, 54, 123456000));
    } finally {
      if (parquetFile != null) {
        parquetFile.delete();
      }
    }
  }

  @Test
  void testWriteMapsToParquetFile() throws Exception {
    List<Map<String, Object>> data = createTestMapData();
    List<Pair<String, org.apache.iceberg.types.Type>> columns = createTestColumnDefinitions();

    File parquetFile = null;
    try {
      parquetFile = File.createTempFile("test", ".parquet");

      DataImportUtil.writeMapsToParquetFile(columns, List.of(), parquetFile.getAbsolutePath());
      assertThat(parquetFile).exists().isFile().isNotEmpty();
      List<Map<String, Object>> readData = readParquetFile(parquetFile.getAbsolutePath());
      assertThat(readData).hasSize(0);

      DataImportUtil.writeMapsToParquetFile(columns, data, parquetFile.getAbsolutePath());
      assertThat(parquetFile).exists().isFile().isNotEmpty();
      readData = readParquetFile(parquetFile.getAbsolutePath());
      assertThat(readData).hasSize(3);
      assertThat(readData).containsExactlyElementsOf(data);
      assertThat(readData.get(0)).containsEntry("id", 1);
      assertThat(readData.get(1)).containsEntry("id", 2);
      assertThat(readData.get(2)).containsEntry("id", 3);
      assertThat(readData.get(2)).containsEntry("integer_col", null);
    } finally {
      if (parquetFile != null) {
        parquetFile.delete();
      }
    }
  }

  @Test
  void testWriteDataToParquetFile() throws Exception {
    List<DataImportTestRecord> records = createTestRecordData();
    List<DataColumnAccessor> mappers = createTestColumnAccessors();

    File parquetFile = null;
    try {
      parquetFile = File.createTempFile("test", ".parquet");

      DataImportUtil.writeDataToParquetFile(mappers, List.of(), parquetFile.getAbsolutePath());
      assertThat(parquetFile).exists().isFile().isNotEmpty();
      List<Map<String, Object>> readData = readParquetFile(parquetFile.getAbsolutePath());
      assertThat(readData).hasSize(0);

      DataImportUtil.writeDataToParquetFile(mappers, records, parquetFile.getAbsolutePath());
      assertThat(parquetFile).exists().isFile().isNotEmpty();
      readData = readParquetFile(parquetFile.getAbsolutePath());
      assertThat(readData).hasSize(1);
      assertThat(readData.get(0))
          .containsEntry("id", 1)
          .containsEntry("integer_col", 121)
          .containsEntry("bigint_col", 121231231L)
          .containsEntry("float_col", 1.11f)
          .containsEntry("double_col", 121.1214)
          .containsEntry(
              "decimal_col",
              new BigDecimal("1241.1231")
                  .setScale(9, RoundingMode.HALF_UP)
                  .setScale(9, RoundingMode.HALF_UP))
          .containsEntry("string_col", "%2Fasf%2F")
          .containsEntry("text_col", "type1\\0$**/~`!@#$%^&*()-_+=|?.,<>")
          .containsEntry("bool_col", true)
          .containsEntry("date_col", LocalDate.of(2024, 1, 1))
          .containsEntry("time_col", LocalTime.of(4, 5, 6))
          .containsEntry("timestamp_col", LocalDateTime.of(2004, 10, 19, 10, 23, 54, 123456000));
    } finally {
      if (parquetFile != null) {
        parquetFile.delete();
      }
    }
  }

  @Test
  void testCreateDuckDBTable() throws SQLException {
    String tableName = "test_create_table";
    List<Pair<String, org.apache.iceberg.types.Type>> columns = createTestColumnDefinitions();

    DataImportUtil.createDuckDBTable(swiftLakeEngine, tableName, columns, false, true);

    try (Connection connection = swiftLakeEngine.createConnection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
      ResultSetMetaData metaData = rs.getMetaData();
      int i = 0;
      assertThat(metaData.getColumnCount()).isEqualTo(12);
      assertThat(metaData.getColumnName(++i).toLowerCase()).isEqualTo("id");
      assertThat(metaData.getColumnName(++i).toLowerCase()).isEqualTo("integer_col");
      assertThat(metaData.getColumnName(++i).toLowerCase()).isEqualTo("bigint_col");
      assertThat(metaData.getColumnName(++i).toLowerCase()).isEqualTo("float_col");
      assertThat(metaData.getColumnName(++i).toLowerCase()).isEqualTo("double_col");
      assertThat(metaData.getColumnName(++i).toLowerCase()).isEqualTo("decimal_col");
      assertThat(metaData.getColumnName(++i).toLowerCase()).isEqualTo("string_col");
      assertThat(metaData.getColumnName(++i).toLowerCase()).isEqualTo("text_col");
      assertThat(metaData.getColumnName(++i).toLowerCase()).isEqualTo("bool_col");
      assertThat(metaData.getColumnName(++i).toLowerCase()).isEqualTo("date_col");
      assertThat(metaData.getColumnName(++i).toLowerCase()).isEqualTo("time_col");
      assertThat(metaData.getColumnName(++i).toLowerCase()).isEqualTo("timestamp_col");

      stmt.executeUpdate("DROP TABLE " + tableName);
    }
  }

  @ParameterizedTest
  @MethodSource("provideDataForDuckDBTable")
  void testWriteToDuckDBTable(
      boolean mapData, List<?> data, List<?> columnAccessors, String expectedSql)
      throws SQLException {
    String tableName = "test_write_table";

    if (mapData) {
      DataImportUtil.createDuckDBTable(
          swiftLakeEngine,
          tableName,
          (List<Pair<String, org.apache.iceberg.types.Type>>) columnAccessors,
          false,
          true);

      DataImportUtil.writeMapsToDuckDBTable(
          swiftLakeEngine,
          (List<Pair<String, org.apache.iceberg.types.Type>>) columnAccessors,
          (List<Map<String, Object>>) data,
          tableName);
    } else {
      DataImportUtil.createDuckDBTableForColumnMappers(
          swiftLakeEngine, tableName, (List<DataColumnAccessor>) columnAccessors, false, true);

      DataImportUtil.writeDataToDuckDBTable(
          swiftLakeEngine, (List<DataColumnAccessor>) columnAccessors, data, tableName);
    }

    try (Connection connection = swiftLakeEngine.createConnection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(expectedSql)) {
      if (data.isEmpty()) {
        assertThat(rs.next()).isFalse();
      } else {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("id")).isEqualTo(1);
        assertThat(rs.getInt("integer_col")).isEqualTo(121);
        assertThat(rs.getLong("bigint_col")).isEqualTo(121231231L);
        assertThat(rs.getFloat("float_col")).isEqualTo(1.11f);
        assertThat(rs.getDouble("double_col")).isEqualTo(121.1214);
        assertThat(rs.getBigDecimal("decimal_col"))
            .isEqualTo(
                new BigDecimal("1241.1231")
                    .setScale(9, RoundingMode.HALF_UP)
                    .setScale(9, RoundingMode.HALF_UP));
        assertThat(rs.getString("string_col")).isEqualTo("%2Fasf%2F");
        assertThat(rs.getString("text_col")).isEqualTo("type1\\0$**/~`!@#$%^&*()-_+=|?.,<>");
        assertThat(rs.getBoolean("bool_col")).isTrue();
        assertThat(rs.getDate("date_col").toLocalDate()).isEqualTo(LocalDate.of(2024, 1, 1));
        assertThat(rs.getTime("time_col").toLocalTime()).isEqualTo(LocalTime.of(4, 5, 6));
        assertThat(rs.getTimestamp("timestamp_col").toLocalDateTime())
            .isEqualTo(LocalDateTime.of(2004, 10, 19, 10, 23, 54, 123456000));
      }
      stmt.executeUpdate("DROP TABLE " + tableName);
    }
  }

  private static Stream<Arguments> provideDataForDuckDBTable() {
    return Stream.of(
        Arguments.of(
            true, List.of(), createTestColumnDefinitions(), "SELECT * FROM test_write_table"),
        Arguments.of(
            true,
            createTestMapData(),
            createTestColumnDefinitions(),
            "SELECT * FROM test_write_table"),
        Arguments.of(
            false, List.of(), createTestColumnAccessors(), "SELECT * FROM test_write_table"),
        Arguments.of(
            false,
            createTestRecordData(),
            createTestColumnAccessors(),
            "SELECT * FROM test_write_table"));
  }

  private static List<Map<String, Object>> createTestMapData() {
    List<Map<String, Object>> data = new ArrayList<>();

    // First record
    Map<String, Object> valuesMap = new HashMap<>();
    valuesMap.put("id", 1);
    valuesMap.put("integer_col", 121);
    valuesMap.put("bigint_col", 121231231L);
    valuesMap.put("float_col", 1.11f);
    valuesMap.put("double_col", 121.1214);
    valuesMap.put("decimal_col", new BigDecimal("1241.1231").setScale(9, RoundingMode.HALF_UP));
    valuesMap.put("string_col", "%2Fasf%2F");
    valuesMap.put("text_col", "type1\\0$**/~`!@#$%^&*()-_+=|?.,<>");
    valuesMap.put("bool_col", true);
    valuesMap.put("date_col", LocalDate.of(2024, 1, 1));
    valuesMap.put("time_col", LocalTime.of(4, 5, 6));
    valuesMap.put("timestamp_col", LocalDateTime.of(2004, 10, 19, 10, 23, 54, 123456000));
    data.add(valuesMap);

    // Second record
    valuesMap = new HashMap<>();
    valuesMap.put("id", 2);
    valuesMap.put("integer_col", 10000);
    valuesMap.put("bigint_col", 14500000L);
    valuesMap.put("float_col", 4.215f);
    valuesMap.put("double_col", 456.789);
    valuesMap.put("decimal_col", new BigDecimal("47851.1452").setScale(9, RoundingMode.HALF_UP));
    valuesMap.put("string_col", "\"\"\"''''");
    valuesMap.put("text_col", "type<>?:{1}1||[]...%#@!~````-_+=+;,.");
    valuesMap.put("bool_col", false);
    valuesMap.put("date_col", LocalDate.of(2024, 2, 1));
    valuesMap.put("time_col", LocalTime.of(1, 2, 3));
    valuesMap.put("timestamp_col", LocalDateTime.of(2024, 10, 19, 1, 2, 3, 456789000));
    data.add(valuesMap);

    // Third record with null values
    valuesMap = new HashMap<>();
    valuesMap.put("id", 3);
    valuesMap.put("integer_col", null);
    valuesMap.put("bigint_col", null);
    valuesMap.put("float_col", null);
    valuesMap.put("double_col", null);
    valuesMap.put("decimal_col", null);
    valuesMap.put("string_col", null);
    valuesMap.put("text_col", null);
    valuesMap.put("bool_col", null);
    valuesMap.put("date_col", null);
    valuesMap.put("time_col", null);
    valuesMap.put("timestamp_col", null);
    data.add(valuesMap);

    return data;
  }

  private static List<Pair<String, org.apache.iceberg.types.Type>> createTestColumnDefinitions() {
    return Arrays.asList(
        Pair.of("id", Types.IntegerType.get()),
        Pair.of("integer_col", Types.IntegerType.get()),
        Pair.of("bigint_col", Types.LongType.get()),
        Pair.of("float_col", Types.FloatType.get()),
        Pair.of("double_col", Types.DoubleType.get()),
        Pair.of("decimal_col", Types.DecimalType.of(38, 9)),
        Pair.of("string_col", Types.StringType.get()),
        Pair.of("text_col", Types.StringType.get()),
        Pair.of("bool_col", Types.BooleanType.get()),
        Pair.of("date_col", Types.DateType.get()),
        Pair.of("time_col", Types.TimeType.get()),
        Pair.of("timestamp_col", Types.TimestampType.withoutZone()));
  }

  private static List<DataImportTestRecord> createTestRecordData() {
    List<DataImportTestRecord> records = new ArrayList<>();
    DataImportTestRecord record = new DataImportTestRecord();
    record.setId(1);
    record.setIntValue(121);
    record.setLongValue(121231231L);
    record.setFloatValue(1.11f);
    record.setDoubleValue(121.1214);
    record.setBigDecimalValue(new BigDecimal("1241.1231").setScale(9, RoundingMode.HALF_UP));
    record.setStringValue("%2Fasf%2F");
    record.setTextValue("type1\\0$**/~`!@#$%^&*()-_+=|?.,<>");
    record.setBooleanValue(true);
    record.setDateValue(LocalDate.of(2024, 1, 1));
    record.setTimeValue(LocalTime.of(4, 5, 6));
    record.setDateTimeValue(LocalDateTime.of(2004, 10, 19, 10, 23, 54, 123456000));
    records.add(record);
    return records;
  }

  private static List<DataColumnAccessor> createTestColumnAccessors() {
    return Arrays.asList(
        DataColumnAccessor.of("id", Types.IntegerType.get(), DataImportTestRecord::getId),
        DataColumnAccessor.of(
            "integer_col", Types.IntegerType.get(), DataImportTestRecord::getIntValue),
        DataColumnAccessor.of(
            "bigint_col", Types.LongType.get(), DataImportTestRecord::getLongValue),
        DataColumnAccessor.of(
            "float_col", Types.FloatType.get(), DataImportTestRecord::getFloatValue),
        DataColumnAccessor.of(
            "double_col", Types.DoubleType.get(), DataImportTestRecord::getDoubleValue),
        DataColumnAccessor.of(
            "decimal_col", Types.DecimalType.of(38, 9), DataImportTestRecord::getBigDecimalValue),
        DataColumnAccessor.of(
            "string_col", Types.StringType.get(), DataImportTestRecord::getStringValue),
        DataColumnAccessor.of(
            "text_col", Types.StringType.get(), DataImportTestRecord::getTextValue),
        DataColumnAccessor.of(
            "bool_col", Types.BooleanType.get(), DataImportTestRecord::isBooleanValue),
        DataColumnAccessor.of("date_col", Types.DateType.get(), DataImportTestRecord::getDateValue),
        DataColumnAccessor.of("time_col", Types.TimeType.get(), DataImportTestRecord::getTimeValue),
        DataColumnAccessor.of(
            "timestamp_col",
            Types.TimestampType.withoutZone(),
            DataImportTestRecord::getDateTimeValue));
  }

  private List<Map<String, Object>> readParquetFile(String filePath) throws SQLException {
    try (Statement stmt = duckDBConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM read_parquet('" + filePath + "')")) {
      return TestUtil.getRecords(rs);
    }
  }

  private static class DataImportTestRecord {
    private int id;
    private Integer intValue;
    private Long longValue;
    private Float floatValue;
    private Double doubleValue;
    private BigDecimal bigDecimalValue;
    private String stringValue;
    private String textValue;
    private Boolean booleanValue;
    private LocalDate dateValue;
    private LocalTime timeValue;
    private LocalDateTime dateTimeValue;

    // Getters and setters
    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public Integer getIntValue() {
      return intValue;
    }

    public void setIntValue(Integer intValue) {
      this.intValue = intValue;
    }

    public Long getLongValue() {
      return longValue;
    }

    public void setLongValue(Long longValue) {
      this.longValue = longValue;
    }

    public Float getFloatValue() {
      return floatValue;
    }

    public void setFloatValue(Float floatValue) {
      this.floatValue = floatValue;
    }

    public Double getDoubleValue() {
      return doubleValue;
    }

    public void setDoubleValue(Double doubleValue) {
      this.doubleValue = doubleValue;
    }

    public BigDecimal getBigDecimalValue() {
      return bigDecimalValue;
    }

    public void setBigDecimalValue(BigDecimal bigDecimalValue) {
      this.bigDecimalValue = bigDecimalValue;
    }

    public String getStringValue() {
      return stringValue;
    }

    public void setStringValue(String stringValue) {
      this.stringValue = stringValue;
    }

    public String getTextValue() {
      return textValue;
    }

    public void setTextValue(String textValue) {
      this.textValue = textValue;
    }

    public Boolean isBooleanValue() {
      return booleanValue;
    }

    public void setBooleanValue(Boolean booleanValue) {
      this.booleanValue = booleanValue;
    }

    public LocalDate getDateValue() {
      return dateValue;
    }

    public void setDateValue(LocalDate dateValue) {
      this.dateValue = dateValue;
    }

    public LocalTime getTimeValue() {
      return timeValue;
    }

    public void setTimeValue(LocalTime timeValue) {
      this.timeValue = timeValue;
    }

    public LocalDateTime getDateTimeValue() {
      return dateTimeValue;
    }

    public void setDateTimeValue(LocalDateTime dateTimeValue) {
      this.dateTimeValue = dateTimeValue;
    }
  }
}
