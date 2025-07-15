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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.FileUtil;
import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.dao.CommonDao;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.expressions.ResolvedExpression;
import com.arcesium.swiftlake.io.SwiftLakeFileIO;
import com.arcesium.swiftlake.metrics.PartitionData;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import com.arcesium.swiftlake.sql.SchemaEvolution;
import com.arcesium.swiftlake.sql.SqlQueryProcessor;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class WriteUtilTest {

  @Mock private CommonDao mockCommonDao;
  @Mock private SwiftLakeEngine mockSwiftLakeEngine;
  @Mock private Table mockTable;
  @Mock private SqlQueryProcessor mockSqlQueryProcessor;
  @Mock private SwiftLakeSqlSessionFactory mockSqlSessionFactory;
  @Mock private SwiftLakeFileIO mockFileIO;

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void testCheckMergeCardinality() {
    when(mockCommonDao.mergeCardinalityCheck("testTable")).thenReturn(false);
    assertThatCode(() -> WriteUtil.checkMergeCardinality(mockCommonDao, "testTable"))
        .doesNotThrowAnyException();

    when(mockCommonDao.mergeCardinalityCheck("testTable")).thenReturn(true);
    assertThatThrownBy(() -> WriteUtil.checkMergeCardinality(mockCommonDao, "testTable"))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void testGetColumns() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    when(mockTable.schema()).thenReturn(schema);

    List<String> columns = WriteUtil.getColumns(mockTable);
    assertThat(columns).containsExactly("id", "name");
  }

  @Test
  void testValidateColumns() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    when(mockTable.schema()).thenReturn(schema);

    assertThatCode(() -> WriteUtil.validateColumns(mockTable, Arrays.asList("id", "name")))
        .doesNotThrowAnyException();

    assertThatThrownBy(
            () -> WriteUtil.validateColumns(mockTable, Arrays.asList("id", "invalidColumn")))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void testValidateTableFilterColumns() {
    List<String> keyColumns = Arrays.asList("id", "code");
    List<String> filterColumns = Arrays.asList("id");

    assertThatCode(() -> WriteUtil.validateTableFilterColumns(filterColumns, keyColumns))
        .doesNotThrowAnyException();

    assertThatThrownBy(
            () -> WriteUtil.validateTableFilterColumns(Arrays.asList("invalidColumn"), keyColumns))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void testGetRemainingColumns() {
    List<String> allColumns = Arrays.asList("id", "name", "age", "address");
    List<String> keyColumns = Arrays.asList("id", "name");

    List<String> remainingColumns = WriteUtil.getRemainingColumns(allColumns, keyColumns);
    assertThat(remainingColumns).containsExactly("age", "address");
  }

  @Test
  void testGetSql() {
    when(mockSqlSessionFactory.getSql("testId", null)).thenReturn("SELECT * FROM test");

    String sql = WriteUtil.getSql(mockSqlSessionFactory, "testId", null);
    assertThat(sql).isEqualTo("SELECT * FROM test");

    assertThatThrownBy(() -> WriteUtil.getSql(null, "testId", null))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void testSplitParquetFile() throws Exception {
    String output = tempDir.resolve(UUID.randomUUID().toString()).toString();
    new File(output).mkdirs();

    // Initialize DuckDB connection
    Class.forName("org.duckdb.DuckDBDriver");
    DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
    try {
      String filePath = Path.of(output, UUID.randomUUID() + ".parquet").toString();

      // Create a Parquet file using DuckDB
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
            "COPY (SELECT generate_series AS value FROM generate_series(1, 10000)) "
                + "TO '"
                + filePath
                + "' (FORMAT PARQUET, ROW_GROUP_SIZE 2000, COMPRESSION ZSTD)");
      }

      String splitFilesPath = Path.of(output, UUID.randomUUID().toString()).toString();

      WriteUtil.splitParquetFile(filePath, splitFilesPath, 5000L);

      List<File> files =
          FileUtil.getFilesInFolder(splitFilesPath).stream()
              .filter(f -> f.getName().endsWith(".parquet"))
              .collect(Collectors.toList());

      assertThat(files).hasSize(2);

      files.forEach(f -> assertThat(f.length()).isBetween(5000L, 7000L));

      // Query the split files using DuckDB
      List<Pair<String, Object>> data = new ArrayList<>();
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs =
            stmt.executeQuery("SELECT SUM(value) AS total FROM '" + splitFilesPath + "/*.parquet'");
        if (rs.next()) {
          data.add(Pair.of("total", rs.getObject("total")));
        }
      }

      BigInteger expectedSum = BigInteger.valueOf(10000L * (10000L + 1) / 2);
      assertThat(data.get(0).getRight()).isEqualTo(expectedSum);
    } finally {
      conn.close();
    }
  }

  @Test
  void testGetCondition() {
    Expression mockExpression = mock(ResolvedExpression.class);
    when(mockSwiftLakeEngine.getSchemaEvolution()).thenReturn(mock(SchemaEvolution.class));
    when(mockSwiftLakeEngine.getSchemaEvolution().getDuckDBFilterSql(any(), any()))
        .thenReturn("id > 10");

    Pair<String, Expression> result =
        WriteUtil.getCondition(
            mockSwiftLakeEngine, mockSqlQueryProcessor, mockTable, mockExpression, null);

    assertThat(result.getLeft()).isEqualTo("id > 10");
    assertThat(result.getRight()).isEqualTo(mockExpression);

    String conditionSql = "id > 10";
    when(mockSqlQueryProcessor.parseConditionExpression(eq(conditionSql), any(), any()))
        .thenReturn(mockExpression);

    // Test with null expression and valid SQL
    result =
        WriteUtil.getCondition(
            mockSwiftLakeEngine, mockSqlQueryProcessor, mockTable, null, conditionSql);
    assertThat(result.getLeft()).isEqualTo("id > 10");
    assertThat(result.getRight()).isEqualTo(mockExpression);

    // Test with null expression and null SQL
    assertThatThrownBy(
            () ->
                WriteUtil.getCondition(
                    mockSwiftLakeEngine, mockSqlQueryProcessor, mockTable, null, null))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void testGetEqualityConditionExpression() {
    Schema schema = getSchema();
    List<String> columns =
        schema.columns().stream().map(Types.NestedField::name).collect(Collectors.toList());
    List<Map<String, Object>> data = createTestData(schema);

    Expression expr = WriteUtil.getEqualityConditionExpression(schema, columns, data);
    assertThat(expr).isNotNull();
    expr = Expressions.resolveExpression(expr);
    assertThat(expr.toString())
        .isEqualTo(
            "(((((((((((ref(name=\"col0\") == \"2020-02-02\" and ref(name=\"col1\") == \"Value1!@#$%^&*()_+\") "
                + "and ref(name=\"col2\") == 1) and ref(name=\"col3\") == 1.124) and ref(name=\"col4\") == \"2020-02-02T12:12:12.123456\") "
                + "and ref(name=\"col5\") == false) and ref(name=\"col6\") == \"12:12:12.123456\") and ref(name=\"col7\") "
                + "== \"2020-02-02T12:12:12.123456+05:30\") and ref(name=\"col8\") == 1.124) and ref(name=\"col9\") == 200) "
                + "and ref(name=\"col10\") == 0.121) or ((((((((((ref(name=\"col0\") == \"2021-03-03\" and ref(name=\"col1\") == "
                + "\"Value2!@#$%^&*()_+\") and ref(name=\"col2\") == 2) and ref(name=\"col3\") == 2.235) and ref(name=\"col4\") == "
                + "\"2021-03-03T13:13:13.234567\") and ref(name=\"col5\") == true) and ref(name=\"col6\") == \"13:13:13.234567\") "
                + "and ref(name=\"col7\") == \"2021-03-03T13:13:13.234567+06:30\") and ref(name=\"col8\") == 2.235) "
                + "and ref(name=\"col9\") == 300) and ref(name=\"col10\") == 0.232))");
  }

  private Schema getSchema() {
    return new Schema(
        Types.NestedField.optional(1, "col0", Types.DateType.get()),
        Types.NestedField.optional(2, "col1", Types.StringType.get()),
        Types.NestedField.optional(3, "col2", Types.IntegerType.get()),
        Types.NestedField.optional(4, "col3", Types.DoubleType.get()),
        Types.NestedField.optional(5, "col4", Types.TimestampType.withoutZone()),
        Types.NestedField.optional(6, "col5", Types.BooleanType.get()),
        Types.NestedField.optional(7, "col6", Types.TimeType.get()),
        Types.NestedField.optional(8, "col7", Types.TimestampType.withZone()),
        Types.NestedField.optional(9, "col8", Types.DecimalType.of(38, 14)),
        Types.NestedField.optional(10, "col9", Types.LongType.get()),
        Types.NestedField.optional(11, "col10", Types.FloatType.get()));
  }

  private List<Map<String, Object>> createTestData(Schema schema) {
    List<Map<String, Object>> data = new ArrayList<>();

    Map<String, Object> record1 = new HashMap<>();
    Map<String, Object> record2 = new HashMap<>();

    IntStream.range(0, schema.columns().size())
        .forEach(
            i -> {
              String colName = schema.columns().get(i).name();
              switch (i) {
                case 0:
                  record1.put(colName, LocalDate.parse("2020-02-02"));
                  record2.put(colName, LocalDate.parse("2021-03-03"));
                  break;
                case 1:
                  record1.put(colName, "Value1!@#$%^&*()_+");
                  record2.put(colName, "Value2!@#$%^&*()_+");
                  break;
                case 2:
                  record1.put(colName, 1);
                  record2.put(colName, 2);
                  break;
                case 3:
                  record1.put(colName, 1.124);
                  record2.put(colName, 2.235);
                  break;
                case 4:
                  record1.put(colName, LocalDateTime.parse("2020-02-02T12:12:12.123456"));
                  record2.put(colName, LocalDateTime.parse("2021-03-03T13:13:13.234567"));
                  break;
                case 5:
                  record1.put(colName, false);
                  record2.put(colName, true);
                  break;
                case 6:
                  record1.put(colName, LocalTime.parse("12:12:12.123456"));
                  record2.put(colName, LocalTime.parse("13:13:13.234567"));
                  break;
                case 7:
                  record1.put(colName, OffsetDateTime.parse("2020-02-02T12:12:12.123456+05:30"));
                  record2.put(colName, OffsetDateTime.parse("2021-03-03T13:13:13.234567+06:30"));
                  break;
                case 8:
                  record1.put(colName, new BigDecimal("1.124"));
                  record2.put(colName, new BigDecimal("2.235"));
                  break;
                case 9:
                  record1.put(colName, 200L);
                  record2.put(colName, 300L);
                  break;
                case 10:
                  record1.put(colName, 0.121f);
                  record2.put(colName, 0.232f);
                  break;
              }
            });

    data.add(record1);
    data.add(record2);

    return data;
  }

  @Test
  void testGetSqlWithProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    when(mockSwiftLakeEngine.getSchemaEvolution()).thenReturn(mock(SchemaEvolution.class));
    when(mockSwiftLakeEngine.getSchemaEvolution().getProjectionExprWithTypeCasting(any(), any()))
        .thenReturn("id, name");

    String result =
        WriteUtil.getSqlWithProjection(
            mockSwiftLakeEngine,
            schema,
            "SELECT * FROM table",
            null,
            true,
            Arrays.asList("id", "name"));

    assertThat(result).contains("SELECT id, name FROM (SELECT * FROM table)");
  }

  @Test
  void testGetSqlWithProjectionWithAdditionalColumns() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    when(mockSwiftLakeEngine.getSchemaEvolution()).thenReturn(mock(SchemaEvolution.class));
    when(mockSwiftLakeEngine.getSchemaEvolution().getProjectionExprWithTypeCasting(any(), any()))
        .thenReturn("id, name");

    String result =
        WriteUtil.getSqlWithProjection(
            mockSwiftLakeEngine,
            schema,
            "SELECT * FROM table",
            List.of("col1", "col2"),
            true,
            Arrays.asList("id", "name"));

    assertThat(result).contains("SELECT id, name,col1,col2 FROM (SELECT * FROM table)");
  }

  @Test
  void testGetPartitionFilterExpression() {
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(
                new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "date", Types.DateType.get())))
            .identity("date")
            .build();

    when(mockTable.schema()).thenReturn(partitionSpec.schema());
    when(mockTable.spec()).thenReturn(partitionSpec);

    List<PartitionData> partitionDataList =
        Arrays.asList(
            new PartitionData(
                1, Arrays.asList(Pair.of(partitionSpec.fields().get(0), "2023-01-01"))));

    Expression result = WriteUtil.getPartitionFilterExpression(mockTable, partitionDataList);
    assertThat(result).isNotNull();
    result = Expressions.resolveExpression(result);
    assertThat(result.toString()).isEqualTo("identity(ref(name=\"date\")) == \"2023-01-01\"");
  }

  @Test
  void testProcessSourceTables() {
    when(mockSwiftLakeEngine.getSqlQueryProcessor()).thenReturn(mockSqlQueryProcessor);
    when(mockSqlQueryProcessor.process(anyString(), any(), any(), any()))
        .thenReturn(Pair.of("transformedSQL", new ArrayList<>()));

    Pair<String, List<InputFiles>> result =
        WriteUtil.processSourceTables(mockSwiftLakeEngine, "SELECT * FROM table", mockTable);

    assertThat(result.getLeft()).isEqualTo("transformedSQL");
    assertThat(result.getRight()).isEmpty();
  }

  @Test
  void testUploadDebugFiles() throws IOException {
    File sourceFolder = tempDir.toFile();
    File testFile = new File(sourceFolder, "test.txt");
    assertThat(testFile.createNewFile()).isTrue(); // Actually create the file

    WriteUtil.uploadDebugFiles(mockFileIO, "/upload/path", sourceFolder.getPath(), "prefix");

    verify(mockFileIO, times(1))
        .uploadFiles(
            argThat(
                files ->
                    files.size() == 1
                        && files.get(0).getSourceDataFilePath().endsWith("test.txt")
                        && files
                            .get(0)
                            .getDestinationDataFilePath()
                            .equals("/upload/path/prefix/test.txt")));
  }

  @Test
  void testUploadDebugFilesWithNoFiles() {
    File sourceFolder = tempDir.toFile();
    WriteUtil.uploadDebugFiles(mockFileIO, "/upload/path", sourceFolder.getPath(), "prefix");

    verify(mockFileIO, never()).uploadFiles(any());
  }

  @Test
  void testUploadDebugFilesWithNullUploadPath() throws IOException {
    File sourceFolder = tempDir.toFile();
    File testFile = new File(sourceFolder, "test.txt");
    assertThat(testFile.createNewFile()).isTrue();

    WriteUtil.uploadDebugFiles(mockFileIO, null, sourceFolder.getPath(), "prefix");

    verify(mockFileIO, never()).uploadFiles(any());
  }

  @Test
  void testUploadDebugFile() {
    File sourceFile = new File(tempDir.toFile(), "debug.txt");
    WriteUtil.uploadDebugFile(
        mockFileIO, "/upload/path", sourceFile.getPath(), "prefix", "debug.txt");

    verify(mockFileIO, times(1)).uploadFiles(any());
  }

  @Test
  void testGetFormattedValueForDateTimes() throws Exception {
    String formattedDate =
        WriteUtil.getFormattedValueForDateTimes(Types.DateType.get(), LocalDate.of(2025, 1, 1));
    assertThat(formattedDate).isEqualTo("2025-01-01");

    formattedDate =
        WriteUtil.getFormattedValueForDateTimes(Types.DateType.get(), Date.valueOf("2025-01-01"));
    assertThat(formattedDate).isEqualTo("2025-01-01");

    String formattedTimestamp =
        WriteUtil.getFormattedValueForDateTimes(
            Types.TimestampType.withoutZone(), LocalDateTime.parse("2025-01-01T12:01:01.123456"));
    assertThat(formattedTimestamp).isEqualTo("2025-01-01T12:01:01.123456");

    formattedTimestamp =
        WriteUtil.getFormattedValueForDateTimes(
            Types.TimestampType.withoutZone(), Timestamp.valueOf("2025-01-01 12:01:01.123456"));
    assertThat(formattedTimestamp).isEqualTo("2025-01-01T12:01:01.123456");

    formattedTimestamp =
        WriteUtil.getFormattedValueForDateTimes(
            Types.TimestampType.withZone(),
            OffsetDateTime.parse("2025-01-01T12:01:01.123456+05:30"));
    assertThat(formattedTimestamp).isEqualTo("2025-01-01T12:01:01.123456+05:30");

    String formattedTime =
        WriteUtil.getFormattedValueForDateTimes(
            Types.TimeType.get(), LocalTime.parse("12:01:01.123456"));
    assertThat(formattedTime).isEqualTo("12:01:01.123456");
  }
}
