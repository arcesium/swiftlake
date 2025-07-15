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

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.DataFile;
import com.arcesium.swiftlake.io.SwiftLakeFileIO;
import com.arcesium.swiftlake.sql.SwiftLakeConnection;
import java.nio.file.Path;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.LocationProvider;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class UnpartitionedDataFileWriterTest {

  @Mock private SwiftLakeEngine mockEngine;
  @Mock private Table mockTable;
  @Mock private Schema mockSchema;
  @Mock private SortOrder mockSortOrder;
  @Mock private SwiftLakeConnection mockConnection;
  @Mock private Statement mockStatement;
  @Mock private SwiftLakeFileIO mockFileIO;

  @TempDir Path tempDir;

  private UnpartitionedDataFileWriter writer;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);

    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "134217728");
    when(mockTable.properties()).thenReturn(tableProperties);
    when(mockTable.schema()).thenReturn(mockSchema);
    when(mockTable.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(mockTable.sortOrder()).thenReturn(mockSortOrder);
    when(mockTable.io()).thenReturn(mockFileIO);

    when(mockEngine.getLocalDir()).thenReturn(tempDir.toString());
    when(mockEngine.createConnection(false)).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);

    writer =
        UnpartitionedDataFileWriter.builderFor(mockEngine, mockTable, "SELECT * FROM test_table")
            .build();
  }

  @Test
  void testWrite() throws Exception {
    UnpartitionedDataFileWriter spyWriter = spy(writer);
    List<String> mockLocalFiles = Arrays.asList("/tmp/file1.parquet", "/tmp/file2.parquet");
    doReturn(mockLocalFiles).when(spyWriter).createLocalDataFiles();
    when(mockTable.locationProvider()).thenReturn(mock(LocationProvider.class));
    when(mockTable.locationProvider().newDataLocation(anyString()))
        .thenAnswer(invocation -> "/newpath/" + invocation.getArgument(0));

    List<DataFile> expectedDataFiles =
        Arrays.asList(
            new DataFile("/tmp/file1.parquet", "remote://path/file1.parquet"),
            new DataFile("/tmp/file2.parquet", "remote://path/file2.parquet"));
    doReturn(expectedDataFiles).when(spyWriter).prepareNewDataFiles(eq(mockTable), any(), any());

    List<DataFile> result = spyWriter.write();

    assertThat(result).isEqualTo(expectedDataFiles);
    verify(mockFileIO).uploadFiles(expectedDataFiles);
  }

  @Test
  void testCreateLocalDataFiles() throws Exception {
    UnpartitionedDataFileWriter spyWriter = spy(writer);
    doReturn(Collections.singletonList("/tmp/test.parquet"))
        .when(spyWriter)
        .executeSqlAndCreateLocalDataFiles(
            eq(mockStatement),
            eq("SELECT * FROM test_table"),
            anyString(),
            eq(mockSchema),
            eq(mockSortOrder),
            eq(134217728L),
            any(),
            eq(false),
            any(),
            any());

    List<String> result = spyWriter.createLocalDataFiles();

    assertThat(result).isNotNull();
    assertThat(result).hasSameElementsAs(Collections.singletonList("/tmp/test.parquet"));
  }

  @Test
  void testBuilderConfiguration() {
    Schema schema = new Schema();
    SortOrder sortOrder = SortOrder.unsorted();
    UnpartitionedDataFileWriter customWriter =
        UnpartitionedDataFileWriter.builderFor(mockEngine, mockTable, "SELECT * FROM test_table")
            .schema(schema)
            .sortOrder(sortOrder)
            .targetFileSizeBytes(67108864L) // 64 MB
            .additionalColumns(Arrays.asList("col1", "col2"))
            .writeToPerThreadParquetFile(true)
            .tmpDir("/custom/tmp/dir")
            .rowGroupSize(1048576L) // 1 MB
            .columns(Arrays.asList("id", "name", "value"))
            .build();

    assertThat(customWriter).isNotNull();
    assertThat(customWriter).extracting("swiftLakeEngine").isEqualTo(mockEngine);
    assertThat(customWriter).extracting("table").isEqualTo(mockTable);
    assertThat(customWriter).extracting("sql").isEqualTo("SELECT * FROM test_table");
    assertThat(customWriter).extracting("schema").isEqualTo(schema);
    assertThat(customWriter).extracting("sortOrder").isEqualTo(sortOrder);
    assertThat(customWriter).extracting("targetFileSizeBytes").isEqualTo(67108864L);
    assertThat(customWriter)
        .extracting("additionalColumns", as(InstanceOfAssertFactories.list(String.class)))
        .hasSameElementsAs(Arrays.asList("col1", "col2"));
    assertThat(customWriter).extracting("writeToPerThreadParquetFile").isEqualTo(true);
    assertThat(customWriter).extracting("tmpDir").isEqualTo("/custom/tmp/dir");
    assertThat(customWriter).extracting("rowGroupSize").isEqualTo(1048576L);
    assertThat(customWriter)
        .extracting("columns", as(InstanceOfAssertFactories.list(String.class)))
        .hasSameElementsAs(Arrays.asList("id", "name", "value"));
  }

  @Test
  void testSkipDataSorting() throws Exception {
    UnpartitionedDataFileWriter writerWithoutSorting =
        UnpartitionedDataFileWriter.builderFor(mockEngine, mockTable, "SELECT * FROM test_table_2")
            .skipDataSorting(true)
            .build();

    UnpartitionedDataFileWriter spyWriter = spy(writerWithoutSorting);
    doReturn(Collections.singletonList("/tmp/test.parquet"))
        .when(spyWriter)
        .executeSqlAndCreateLocalDataFiles(
            eq(mockStatement),
            eq("SELECT * FROM test_table_2"),
            anyString(),
            eq(mockSchema),
            isNull(),
            eq(134217728L),
            any(),
            eq(false),
            any(),
            any());

    List<String> result = spyWriter.createLocalDataFiles();

    assertThat(result).isNotEmpty();
    assertThat(result).hasSameElementsAs(Collections.singletonList("/tmp/test.parquet"));
  }

  @Test
  void testWriteWithAdditionalColumns() throws Exception {
    List<String> additionalColumns = Arrays.asList("extra_col1", "extra_col2");
    UnpartitionedDataFileWriter customWriter =
        UnpartitionedDataFileWriter.builderFor(mockEngine, mockTable, "SELECT * FROM test_table_2")
            .additionalColumns(additionalColumns)
            .build();

    UnpartitionedDataFileWriter spyWriter = spy(customWriter);
    doReturn(Collections.singletonList("/tmp/test.parquet"))
        .when(spyWriter)
        .executeSqlAndCreateLocalDataFiles(
            eq(mockStatement),
            eq("SELECT * FROM test_table_2"),
            anyString(),
            eq(mockSchema),
            eq(mockSortOrder),
            eq(134217728L),
            eq(additionalColumns),
            eq(false),
            any(),
            any());

    List<String> result = spyWriter.createLocalDataFiles();

    assertThat(result).isNotNull();
    assertThat(result).hasSameElementsAs(Collections.singletonList("/tmp/test.parquet"));
  }

  @Test
  void testWriteToPerThreadParquetFile() throws Exception {
    UnpartitionedDataFileWriter customWriter =
        UnpartitionedDataFileWriter.builderFor(mockEngine, mockTable, "SELECT * FROM test_table_2")
            .writeToPerThreadParquetFile(true)
            .build();
    UnpartitionedDataFileWriter spyWriter = spy(customWriter);
    doReturn(Collections.singletonList("/tmp/test.parquet"))
        .when(spyWriter)
        .executeSqlAndCreateLocalDataFiles(
            eq(mockStatement),
            eq("SELECT * FROM test_table_2"),
            anyString(),
            eq(mockSchema),
            eq(mockSortOrder),
            eq(134217728L),
            isNull(),
            eq(true),
            any(),
            isNull());

    List<String> result = spyWriter.createLocalDataFiles();

    assertThat(result).isNotNull();
    assertThat(result).hasSameElementsAs(Collections.singletonList("/tmp/test.parquet"));
  }

  @Test
  void testWriteWithCustomRowGroupSize() throws Exception {
    UnpartitionedDataFileWriter customWriter =
        UnpartitionedDataFileWriter.builderFor(mockEngine, mockTable, "SELECT * FROM test_table_2")
            .rowGroupSize(524288L)
            .build();
    UnpartitionedDataFileWriter spyWriter = spy(customWriter);
    doReturn(Collections.singletonList("/tmp/test.parquet"))
        .when(spyWriter)
        .executeSqlAndCreateLocalDataFiles(
            eq(mockStatement),
            eq("SELECT * FROM test_table_2"),
            anyString(),
            eq(mockSchema),
            eq(mockSortOrder),
            eq(134217728L),
            isNull(),
            eq(false),
            eq(524288L),
            isNull());

    List<String> result = spyWriter.createLocalDataFiles();

    assertThat(result).isNotNull();
    assertThat(result).hasSameElementsAs(Collections.singletonList("/tmp/test.parquet"));
  }

  @Test
  void testWriteWithSpecificColumns() throws Exception {
    List<String> columns = Arrays.asList("id", "name", "age");
    UnpartitionedDataFileWriter customWriter =
        UnpartitionedDataFileWriter.builderFor(mockEngine, mockTable, "SELECT * FROM test_table_2")
            .columns(columns)
            .build();
    UnpartitionedDataFileWriter spyWriter = spy(customWriter);
    doReturn(Collections.singletonList("/tmp/test.parquet"))
        .when(spyWriter)
        .executeSqlAndCreateLocalDataFiles(
            eq(mockStatement),
            eq("SELECT * FROM test_table_2"),
            anyString(),
            eq(mockSchema),
            eq(mockSortOrder),
            eq(134217728L),
            isNull(),
            eq(false),
            any(),
            eq(columns));

    List<String> result = spyWriter.createLocalDataFiles();

    assertThat(result).isNotNull();
    assertThat(result).hasSameElementsAs(Collections.singletonList("/tmp/test.parquet"));
  }
}
