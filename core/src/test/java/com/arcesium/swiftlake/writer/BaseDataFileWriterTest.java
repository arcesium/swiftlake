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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.TestUtil;
import com.arcesium.swiftlake.commands.WriteUtil;
import com.arcesium.swiftlake.common.DataFile;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.dao.CommonDao;
import com.arcesium.swiftlake.io.SwiftLakeFileIO;
import com.arcesium.swiftlake.sql.SchemaEvolution;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BaseDataFileWriterTest {

  @Mock private SwiftLakeEngine mockEngine;
  @Mock private Table mockTable;
  @Mock private SwiftLakeFileIO mockFileIO;
  @Mock private CommonDao mockCommonDao;
  @Mock private Statement mockStatement;

  private TestDataFileWriter dataFileWriter;

  private static class TestDataFileWriter extends BaseDataFileWriter {
    public TestDataFileWriter(SwiftLakeEngine engine, Table table) {
      super(engine, table);
    }

    @Override
    public List<DataFile> write() {
      return new ArrayList<>();
    }
  }

  @BeforeEach
  void setUp() {
    when(mockTable.io()).thenReturn(mockFileIO);
    when(mockEngine.getCommonDao()).thenReturn(mockCommonDao);
    dataFileWriter = new TestDataFileWriter(mockEngine, mockTable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testExecuteSqlAndCreateLocalDataFiles(boolean nullSortOrder) throws SQLException {
    String sql = "SELECT * FROM test_table";
    String outputDir = "/tmp/output";
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    SortOrder sortOrder = nullSortOrder ? null : SortOrder.unsorted();
    Long targetFileSizeBytes = 1024L;
    List<String> additionalColumns = Arrays.asList("col1", "col2");
    boolean writeToPerThreadParquetFile = false;
    Long rowGroupSize = 128L;
    List<String> columns = new ArrayList<>();

    when(mockCommonDao.getCopyToFileSql(anyString(), anyString(), anyString(), anyString()))
        .thenAnswer(
            invocation ->
                "COPY (SELECT * FROM ("
                    + invocation.getArgument(0)
                    + ") "
                    + invocation.getArgument(1)
                    + ") TO '"
                    + invocation.getArgument(2)
                    + "' ("
                    + invocation.getArgument(3)
                    + ");");

    try (MockedStatic<WriteUtil> mockedWriteUtil = mockStatic(WriteUtil.class)) {
      mockedWriteUtil
          .when(
              () ->
                  WriteUtil.getSqlWithProjection(
                      mockEngine, schema, sql, additionalColumns, false, columns))
          .thenReturn("SELECT id FROM test_table");

      List<String> result =
          dataFileWriter.executeSqlAndCreateLocalDataFiles(
              mockStatement,
              sql,
              outputDir,
              schema,
              sortOrder,
              targetFileSizeBytes,
              additionalColumns,
              writeToPerThreadParquetFile,
              rowGroupSize,
              columns);

      assertThat(result).isNotNull();
      verify(mockStatement)
          .executeUpdate(
              matches(
                  "COPY \\(SELECT \\* FROM \\(SELECT id FROM test_table\\) \\) TO '/tmp/output/.*' \\(FORMAT 'PARQUET', "
                      + "COMPRESSION 'ZSTD', FIELD_IDS \\{id: 1\\}, FILE_SIZE_BYTES 1024, ROW_GROUP_SIZE 128\\);"));
    }
  }

  @Test
  void testExecuteSqlAndCreateLocalDataFilesWithSortOrder() throws SQLException {
    String sql = "SELECT * FROM test_table";
    String outputDir = "/tmp/output";
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(4, "date", Types.DateType.get()));
    SortOrder sortOrder =
        SortOrder.builderFor(schema)
            .asc("id")
            .desc("name")
            .asc("age", NullOrder.NULLS_LAST)
            .desc("date", NullOrder.NULLS_FIRST)
            .build();
    Long targetFileSizeBytes = 1024L;
    List<String> additionalColumns = new ArrayList<>();
    boolean writeToPerThreadParquetFile = false;
    Long rowGroupSize = 128L;
    List<String> columns = new ArrayList<>();

    when(mockCommonDao.getCopyToFileSql(anyString(), anyString(), anyString(), anyString()))
        .thenAnswer(
            invocation ->
                "COPY (SELECT * FROM ("
                    + invocation.getArgument(0)
                    + ") "
                    + invocation.getArgument(1)
                    + ") TO '"
                    + invocation.getArgument(2)
                    + "' ("
                    + invocation.getArgument(3)
                    + ");");

    try (MockedStatic<WriteUtil> mockedWriteUtil = mockStatic(WriteUtil.class)) {
      mockedWriteUtil
          .when(
              () ->
                  WriteUtil.getSqlWithProjection(
                      mockEngine, schema, sql, additionalColumns, false, columns))
          .thenReturn("SELECT id FROM test_table");
      mockedWriteUtil
          .when(() -> WriteUtil.splitParquetFile(anyString(), anyString(), eq(targetFileSizeBytes)))
          .thenReturn(List.of("/tmp/output/file.parquet"));
      List<String> result =
          dataFileWriter.executeSqlAndCreateLocalDataFiles(
              mockStatement,
              sql,
              outputDir,
              schema,
              sortOrder,
              targetFileSizeBytes,
              additionalColumns,
              writeToPerThreadParquetFile,
              rowGroupSize,
              columns);

      assertThat(result).isNotNull();
      assertThat(result).hasSize(1);
      assertThat(result.get(0)).isEqualTo("/tmp/output/file.parquet");
      verify(mockStatement)
          .executeUpdate(
              matches(
                  "COPY \\(SELECT \\* FROM \\(SELECT id FROM test_table\\) ORDER BY id ASC NULLS FIRST, "
                      + "name DESC NULLS LAST, age ASC NULLS LAST, date DESC NULLS FIRST\\) TO '/tmp/output/.*\\.parquet' "
                      + "\\(FORMAT 'PARQUET', COMPRESSION 'ZSTD', FIELD_IDS \\{id: 1,name: 2,age: 3,date: 4\\}, ROW_GROUP_SIZE 128\\);"));
    }
  }

  @Test
  void testExecuteSqlAndCreateLocalDataFilesWithPerThreadParquetFile() throws SQLException {
    String sql = "SELECT * FROM test_table";
    String outputDir = "/tmp/output";
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").build();
    Long targetFileSizeBytes = null;
    List<String> additionalColumns = List.of("col1");
    boolean writeToPerThreadParquetFile = true;
    Long rowGroupSize = 128L;
    List<String> columns = new ArrayList<>();

    when(mockCommonDao.getCopyToFileSql(anyString(), anyString(), anyString(), anyString()))
        .thenAnswer(
            invocation ->
                "COPY (SELECT * FROM ("
                    + invocation.getArgument(0)
                    + ") "
                    + invocation.getArgument(1)
                    + ") TO '"
                    + invocation.getArgument(2)
                    + "' ("
                    + invocation.getArgument(3)
                    + ");");

    try (MockedStatic<WriteUtil> mockedWriteUtil = mockStatic(WriteUtil.class)) {
      mockedWriteUtil
          .when(
              () ->
                  WriteUtil.getSqlWithProjection(
                      mockEngine, schema, sql, additionalColumns, false, columns))
          .thenReturn("SELECT id FROM test_table");

      List<String> result =
          dataFileWriter.executeSqlAndCreateLocalDataFiles(
              mockStatement,
              sql,
              outputDir,
              schema,
              sortOrder,
              targetFileSizeBytes,
              additionalColumns,
              writeToPerThreadParquetFile,
              rowGroupSize,
              columns);

      assertThat(result).isNotNull();
      verify(mockStatement)
          .executeUpdate(
              matches(
                  "COPY \\(SELECT \\* FROM \\(SELECT id FROM test_table\\) ORDER BY id ASC NULLS FIRST\\) TO "
                      + "'/tmp/output/.*\\.parquet' \\(FORMAT 'PARQUET', COMPRESSION 'ZSTD', FIELD_IDS \\{id: 1\\}, ROW_GROUP_SIZE 128\\);"));

      sortOrder = SortOrder.unsorted();
      result =
          dataFileWriter.executeSqlAndCreateLocalDataFiles(
              mockStatement,
              sql,
              outputDir,
              schema,
              sortOrder,
              targetFileSizeBytes,
              additionalColumns,
              writeToPerThreadParquetFile,
              rowGroupSize,
              columns);

      assertThat(result).isNotNull();
      verify(mockStatement)
          .executeUpdate(
              matches(
                  "COPY \\(SELECT \\* FROM \\(SELECT id FROM test_table\\) \\) TO '/tmp/output/.*' \\(FORMAT 'PARQUET', "
                      + "COMPRESSION 'ZSTD', FIELD_IDS \\{id: 1\\}, PER_THREAD_OUTPUT True, ROW_GROUP_SIZE 128\\);"));
    }
  }

  @Test
  void testGetUniqueParquetFileName() {
    String fileName = dataFileWriter.getUniqueParquetFileName();
    assertThat(fileName).startsWith("data_").endsWith(".parquet");
  }

  @Test
  void testGetUniqueName() {
    String name = dataFileWriter.getUniqueName("test");
    assertThat(name).startsWith("test_");
  }

  @Test
  void testSortOrderExists() {
    SortOrder emptyOrder = SortOrder.unsorted();
    assertThat(dataFileWriter.sortOrderExists(emptyOrder)).isFalse();

    SortOrder nonEmptyOrder =
        SortOrder.builderFor(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
            .asc("id")
            .build();
    assertThat(dataFileWriter.sortOrderExists(nonEmptyOrder)).isTrue();
  }

  @Test
  void testGetNewFilePathUnpartitioned() {
    when(mockTable.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(mockTable.locationProvider()).thenReturn(mock(LocationProvider.class));
    when(mockTable.locationProvider().newDataLocation(anyString()))
        .thenReturn("/path/test.parquet");

    String result = dataFileWriter.getNewFilePath(mockTable, null, "test.parquet");

    assertThat(result).isEqualTo("/path/test.parquet");
    verify(mockTable.locationProvider()).newDataLocation("test.parquet");
  }

  @Test
  void testGetNewFilePathPartitioned() {
    PartitionSpec spec =
        PartitionSpec.builderFor(
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
            .identity("id")
            .build();
    StructLike partitionData = mock(StructLike.class);
    when(mockTable.spec()).thenReturn(spec);
    when(mockTable.locationProvider()).thenReturn(mock(LocationProvider.class));
    when(mockTable
            .locationProvider()
            .newDataLocation(eq(spec), eq(partitionData), eq("test.parquet")))
        .thenReturn("/path/test.parquet");

    String result = dataFileWriter.getNewFilePath(mockTable, partitionData, "test.parquet");

    assertThat(result).isEqualTo("/path/test.parquet");
    verify(mockTable.locationProvider())
        .newDataLocation(eq(spec), eq(partitionData), eq("test.parquet"));
  }

  @Test
  void testGetNewFilePathPartitionedWithNullData() {
    PartitionSpec spec =
        PartitionSpec.builderFor(
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
            .identity("id")
            .build();
    when(mockTable.spec()).thenReturn(spec);

    assertThatThrownBy(() -> dataFileWriter.getNewFilePath(mockTable, null, "test.parquet"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Partitioning exists on table. Provide partition data.");
  }

  @Test
  void testGetNewFilePathUnpartitionedWithPartitionData() {
    when(mockTable.spec()).thenReturn(PartitionSpec.unpartitioned());

    StructLike partitionData = mock(StructLike.class);
    assertThatThrownBy(
            () -> dataFileWriter.getNewFilePath(mockTable, partitionData, "test.parquet"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Partitioning does not exist on table.");
  }

  @Test
  void testGetProjectedColumns() {
    List<Types.NestedField> fields = new ArrayList<>();
    fields.add(Types.NestedField.required(1, "id", Types.LongType.get()));
    fields.add(Types.NestedField.optional(2, "name", Types.StringType.get()));

    when(mockTable.schema()).thenReturn(new Schema(fields));
    when(mockEngine.getSchemaEvolution()).thenReturn(mock(SchemaEvolution.class));
    when(mockEngine.getSchemaEvolution().getProjectionListWithTypeCasting(anyList(), any()))
        .thenReturn(List.of("id", "name"));

    List<String> result = dataFileWriter.getProjectedColumns();
    assertThat(result).containsExactly("id", "name");
  }

  @Test
  void testGetOrderBySql() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    SortOrder sortOrder =
        SortOrder.builderFor(schema).asc("id").desc("name", NullOrder.NULLS_LAST).build();

    Method getOrderBySqlMethod =
        BaseDataFileWriter.class.getDeclaredMethod("getOrderBySql", SortOrder.class);
    getOrderBySqlMethod.setAccessible(true);

    String result = (String) getOrderBySqlMethod.invoke(dataFileWriter, sortOrder);
    assertThat(result).isEqualTo("ORDER BY id ASC NULLS FIRST, name DESC NULLS LAST");
  }

  @Test
  void testGetIcebergTableFieldIdsForDuckDB() throws Exception {
    Schema schema = TestUtil.createComplexSchema();

    Method getIcebergTableFieldIdsForDuckDBMethod =
        BaseDataFileWriter.class.getDeclaredMethod(
            "getIcebergTableFieldIdsForDuckDB", Schema.class);
    getIcebergTableFieldIdsForDuckDBMethod.setAccessible(true);

    String result = (String) getIcebergTableFieldIdsForDuckDBMethod.invoke(dataFileWriter, schema);
    assertThat(result)
        .isEqualTo(
            "{int_1: 1,float_1: 2,string_1: 3,struct_1: {__duckdb_field_id: 4, struct_1_string_1: 5,"
                + "struct_1_decimal_1: 6,struct_1_struct_1: {__duckdb_field_id: 7, struct_1_struct_1_date_1: 8}},"
                + "list_1: {__duckdb_field_id: 9, element: {__duckdb_field_id: 10, list_1_string_1: 11,list_1_double_1: 12,"
                + "list_1_struct_1: {__duckdb_field_id: 13, list_1_struct_1_date_1: 14}}},list_2: {__duckdb_field_id: 15, "
                + "element: {__duckdb_field_id: 16, element: 17}},map_1: {__duckdb_field_id: 18, key: 19,value: "
                + "{__duckdb_field_id: 20, map_1_string_1: 21,map_1_double_1: 22,map_1_struct_1: {__duckdb_field_id: 23, "
                + "map_1_struct_1_date_1: 24}}}}");
  }

  @Test
  void testPrepareNewDataFilesWithEmptyList() {
    List<DataFile> result =
        dataFileWriter.prepareNewDataFiles(mockTable, new ArrayList<>(), SortOrder.unsorted());
    assertThat(result).isEmpty();
  }
}
