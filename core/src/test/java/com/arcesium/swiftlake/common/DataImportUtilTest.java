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
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.sql.SchemaEvolution;
import java.io.File;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DataImportUtilTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private DuckDBConnection mockConnection;

  @Mock private ResultSet mockResultSet;

  @Mock private ResultSetMetaData mockMetaData;

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    when(mockSwiftLakeEngine.createDuckDBConnection()).thenReturn(mockConnection);
    when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
  }

  @Test
  void testWriteResultSetToParquetFile() throws SQLException {
    File outputFile = tempDir.resolve("output.parquet").toFile();
    when(mockMetaData.getColumnCount()).thenReturn(2);
    when(mockMetaData.getColumnName(1)).thenReturn("id");
    when(mockMetaData.getColumnName(2)).thenReturn("name");
    when(mockMetaData.getColumnType(1)).thenReturn(java.sql.Types.INTEGER);
    when(mockMetaData.getColumnType(2)).thenReturn(java.sql.Types.VARCHAR);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getObject("id")).thenReturn(1, 2);
    when(mockResultSet.getObject("name")).thenReturn("Alice", "Bob");

    DataImportUtil.writeResultSetToParquetFile(mockResultSet, outputFile.getAbsolutePath());

    assertThat(outputFile).exists();
    assertThat(outputFile).isFile();
    assertThat(outputFile.length()).isGreaterThan(0);
  }

  @Test
  void testWriteMapsToParquetFile() {
    File outputFile = tempDir.resolve("output.parquet").toFile();
    List<org.apache.commons.lang3.tuple.Pair<String, Type>> columns =
        Arrays.asList(
            org.apache.commons.lang3.tuple.Pair.of("id", Types.IntegerType.get()),
            org.apache.commons.lang3.tuple.Pair.of("name", Types.StringType.get()));
    List<Map<String, Object>> data =
        Arrays.asList(Map.of("id", 1, "name", "Alice"), Map.of("id", 2, "name", "Bob"));

    DataImportUtil.writeMapsToParquetFile(columns, data, outputFile.getAbsolutePath());

    assertThat(outputFile).exists();
    assertThat(outputFile).isFile();
    assertThat(outputFile.length()).isGreaterThan(0);
  }

  @Test
  void testWriteDataToParquetFile() {
    File outputFile = tempDir.resolve("output.parquet").toFile();
    List<DataColumnAccessor> columnAccessors =
        Arrays.asList(
            DataColumnAccessor.of("id", Types.IntegerType.get(), TestObject::getId),
            DataColumnAccessor.of("name", Types.StringType.get(), TestObject::getName));
    List<TestObject> data = Arrays.asList(new TestObject(1, "Alice"), new TestObject(2, "Bob"));

    DataImportUtil.writeDataToParquetFile(columnAccessors, data, outputFile.getAbsolutePath());

    assertThat(outputFile).exists();
    assertThat(outputFile).isFile();
    assertThat(outputFile.length()).isGreaterThan(0);
  }

  @Test
  void testCreateDuckDBTable() throws SQLException {
    when(mockMetaData.getColumnCount()).thenReturn(2);
    when(mockMetaData.getColumnName(1)).thenReturn("id");
    when(mockMetaData.getColumnName(2)).thenReturn("name");
    when(mockMetaData.getColumnType(1)).thenReturn(java.sql.Types.INTEGER);
    when(mockMetaData.getColumnType(2)).thenReturn(java.sql.Types.VARCHAR);

    Statement mockStatement = mock(Statement.class);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockSwiftLakeEngine.getSchemaEvolution()).thenReturn(new SchemaEvolution());
    DataImportUtil.createDuckDBTable(mockSwiftLakeEngine, "test_table", mockResultSet, true, true);

    verify(mockStatement).executeUpdate(contains("CREATE OR REPLACE TEMP TABLE test_table"));
  }

  @Test
  void testWriteResultSetToDuckDBTable() throws SQLException {
    when(mockMetaData.getColumnCount()).thenReturn(2);
    when(mockMetaData.getColumnName(1)).thenReturn("id");
    when(mockMetaData.getColumnName(2)).thenReturn("name");
    when(mockMetaData.getColumnType(1)).thenReturn(java.sql.Types.INTEGER);
    when(mockMetaData.getColumnType(2)).thenReturn(java.sql.Types.VARCHAR);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getObject("id")).thenReturn(1, 2);
    when(mockResultSet.getObject("name")).thenReturn("Alice", "Bob");

    PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

    DataImportUtil.writeResultSetToDuckDBTable(mockResultSet, "test_table", mockConnection);

    verify(mockPreparedStatement, times(2)).addBatch();
    verify(mockPreparedStatement, times(1)).executeBatch();
  }

  @Test
  void testWriteMapsToDuckDBTable() throws SQLException {
    List<org.apache.commons.lang3.tuple.Pair<String, Type>> columns =
        Arrays.asList(
            org.apache.commons.lang3.tuple.Pair.of("id", Types.IntegerType.get()),
            org.apache.commons.lang3.tuple.Pair.of("name", Types.StringType.get()));
    List<Map<String, Object>> data =
        Arrays.asList(Map.of("id", 1, "name", "Alice"), Map.of("id", 2, "name", "Bob"));

    PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

    DataImportUtil.writeMapsToDuckDBTable(columns, data, "test_table", mockConnection);

    verify(mockPreparedStatement, times(2)).addBatch();
    verify(mockPreparedStatement, times(1)).executeBatch();
  }

  @Test
  void testWriteDataToDuckDBTable() throws SQLException {
    List<DataColumnAccessor> columnAccessors =
        Arrays.asList(
            DataColumnAccessor.of("id", Types.IntegerType.get(), TestObject::getId),
            DataColumnAccessor.of("name", Types.StringType.get(), TestObject::getName));
    List<TestObject> data = Arrays.asList(new TestObject(1, "Alice"), new TestObject(2, "Bob"));

    PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

    DataImportUtil.writeDataToDuckDBTable(columnAccessors, data, "test_table", mockConnection);

    verify(mockPreparedStatement, times(2)).addBatch();
    verify(mockPreparedStatement, times(1)).executeBatch();
  }

  private static class TestObject {
    private final int id;
    private final String name;

    TestObject(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }
  }
}
