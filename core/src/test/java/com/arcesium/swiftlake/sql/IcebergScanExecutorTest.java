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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.commands.WriteUtil;
import com.arcesium.swiftlake.common.InputFile;
import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.SwiftLakeConfiguration;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.expressions.ResolvedExpression;
import com.arcesium.swiftlake.io.DefaultInputFile;
import com.arcesium.swiftlake.io.DefaultInputFiles;
import com.arcesium.swiftlake.io.SwiftLakeFileIO;
import com.arcesium.swiftlake.metrics.PartitionData;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IcebergScanExecutorTest {

  @Mock private SwiftLakeEngine mockEngine;

  @Mock private SwiftLakeConfiguration mockConfig;

  @Mock private Table mockTable;

  @Mock private TableScan mockTableScan;

  @Mock private SwiftLakeFileIO mockFileIO;

  @Mock private Connection mockConnection;

  @Mock private SqlQueryProcessor mockSqlQueryProcessor;
  @Mock private SchemaEvolution mockSchemaEvolution;
  @Mock private PartitionSpec mockPartitionSpec1;
  @Mock private PartitionSpec mockPartitionSpec2;
  private IcebergScanExecutor icebergScanExecutor;

  @BeforeEach
  void setUp() {
    icebergScanExecutor = new IcebergScanExecutor(mockEngine, mockConfig);
  }

  @Test
  void testExecuteTableScanWithAllowDirectReadTrue() throws IOException {
    when(mockEngine.getSchemaEvolution()).thenReturn(mockSchemaEvolution);
    when(mockConfig.isAllowFullTableScan()).thenReturn(true);
    Pair<CloseableIterable<FileScanTask>, List<DataFile>> mockScanData = createMockFileScanTasks();
    Map<String, String> downloadedFiles =
        configureTableScan(
            mockTable,
            mockTableScan,
            mockScanData.getLeft(),
            mockScanData.getRight(),
            "SELECT * FROM table");

    // Mock the SwiftLakeFileIO behavior
    when(mockFileIO.isDownloadable(eq(mockScanData.getRight().get(0).location()), anyLong()))
        .thenReturn(true);
    when(mockFileIO.isDownloadable(eq(mockScanData.getRight().get(1).location()), anyLong()))
        .thenReturn(false);
    when(mockFileIO.isDownloadable(eq(mockScanData.getRight().get(2).location()), anyLong()))
        .thenReturn(true);
    when(mockFileIO.isDownloadable(eq(mockScanData.getRight().get(3).location()), anyLong()))
        .thenReturn(false);

    boolean allowDirectRead = true;
    TableScanResult result =
        icebergScanExecutor.executeTableScan(
            mockTable, Expressions.alwaysTrue(), false, false, null, null, null, allowDirectRead);

    assertThat(result).isNotNull();
    verify(mockFileIO)
        .newInputFiles(
            argThat(
                list ->
                    list.size() == 2
                        && list.stream()
                            .allMatch(
                                path ->
                                    path.equals(mockScanData.getRight().get(0).location())
                                        || path.equals(
                                            mockScanData.getRight().get(2).location()))));
    assertThat(result.getScanResult().getRight()).hasSize(4);
    assertThat(result.getScanResult().getRight())
        .containsExactlyInAnyOrderElementsOf(mockScanData.getRight());
    Map<String, String> actualDownloadedFiles =
        result.getInputFiles().getInputFiles().stream()
            .filter(i -> i.getLocalFileLocation() != null)
            .collect(Collectors.toMap(InputFile::getLocation, InputFile::getLocalFileLocation));
    assertThat(actualDownloadedFiles).containsExactlyInAnyOrderEntriesOf(downloadedFiles);
    List<String> remoteFiles =
        result.getInputFiles().getInputFiles().stream()
            .filter(i -> i.getLocalFileLocation() == null)
            .map(i -> i.getLocation())
            .collect(Collectors.toList());
    assertThat(remoteFiles)
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                mockScanData.getRight().get(1).location(),
                mockScanData.getRight().get(3).location()));
    List<String> localLocations =
        result.getInputFiles().getInputFiles().stream()
            .filter(i -> i.getLocalFileLocation() != null)
            .map(i -> i.getLocalFileLocation())
            .collect(Collectors.toList());
    verify(mockSchemaEvolution)
        .getSelectSQLForDataFiles(
            eq(mockTable), eq(localLocations), eq(remoteFiles), eq(false), eq(false));
  }

  @Test
  void testExecuteTableScanWithAllowDirectReadFalse() throws IOException {
    when(mockEngine.getSchemaEvolution()).thenReturn(mockSchemaEvolution);
    when(mockConfig.isAllowFullTableScan()).thenReturn(true);
    Pair<CloseableIterable<FileScanTask>, List<DataFile>> mockScanData = createMockFileScanTasks();
    Map<String, String> downloadedFiles =
        configureTableScan(
            mockTable,
            mockTableScan,
            mockScanData.getLeft(),
            mockScanData.getRight(),
            "SELECT * FROM table");

    boolean allowDirectRead = false;
    TableScanResult result =
        icebergScanExecutor.executeTableScan(
            mockTable, Expressions.alwaysTrue(), false, false, null, null, null, allowDirectRead);

    assertThat(result).isNotNull();
    verify(mockFileIO, never()).isDownloadable(anyString(), anyLong());
    verify(mockFileIO).newInputFiles(argThat(list -> list.size() == 4));
    assertThat(result.getScanResult().getRight()).hasSize(4);
    assertThat(result.getScanResult().getRight())
        .containsExactlyInAnyOrderElementsOf(mockScanData.getRight());
    Map<String, String> actualDownloadedFiles =
        result.getInputFiles().getInputFiles().stream()
            .filter(i -> i.getLocalFileLocation() != null)
            .collect(Collectors.toMap(InputFile::getLocation, InputFile::getLocalFileLocation));
    assertThat(actualDownloadedFiles).hasSize(4);
    assertThat(actualDownloadedFiles).containsExactlyInAnyOrderEntriesOf(downloadedFiles);
    List<String> remoteFiles =
        result.getInputFiles().getInputFiles().stream()
            .filter(i -> i.getLocalFileLocation() == null)
            .map(i -> i.getLocation())
            .collect(Collectors.toList());
    assertThat(remoteFiles).isEmpty();
    List<String> localLocations =
        result.getInputFiles().getInputFiles().stream()
            .filter(i -> i.getLocalFileLocation() != null)
            .map(i -> i.getLocalFileLocation())
            .collect(Collectors.toList());
    verify(mockSchemaEvolution)
        .getSelectSQLForDataFiles(
            eq(mockTable), eq(localLocations), eq(List.of()), eq(false), eq(false));
  }

  @Test
  void testExecuteTableScansAndUpdateSql_WithoutSql() {
    when(mockConfig.isAllowFullTableScan()).thenReturn(true);
    when(mockEngine.getSqlQueryProcessor()).thenReturn(mockSqlQueryProcessor);
    when(mockSqlQueryProcessor.parseTimeTravelOptions(anyString())).thenReturn(null);
    when(mockEngine.getSchemaEvolution()).thenReturn(mockSchemaEvolution);

    Pair<CloseableIterable<FileScanTask>, List<DataFile>> mockScanData = createMockFileScanTasks();
    CloseableIterable<FileScanTask> mockIterable = mockScanData.getLeft();
    Expression expression1 = Expressions.equal("id", 1);
    TableFilter filter1 = new TableFilter("table1", expression1);
    configureTable("table1", mockTable, null);
    configureTableScan(
        mockTable, mockTableScan, mockIterable, null, "(SELECT * FROM 'file1.parquet')");

    Expression expression2 = Expressions.in("id", 1, 2);
    TableFilter filter2 = new TableFilter("table2", expression2, "{{table2}}");
    Table mockTable2 = mock(Table.class);
    TableScan mockTableScan2 = mock(TableScan.class);
    configureTable("table2", mockTable2, null);
    configureTableScan(mockTable2, mockTableScan2, null, null, "(SELECT * FROM 'file2.parquet')");

    Expression expression3 = Expressions.notEqual("id", 1);
    String condition3 = "id NOT IN (1)";
    Map<Integer, Object> conditionParams = Map.of(1, "value1");
    TableFilter filter3 = new TableFilter("table3", condition3, "{{table3}}", conditionParams);
    Table mockTable3 = mock(Table.class);
    TableScan mockTableScan3 = mock(TableScan.class);
    Schema mockSchema3 = mock(Schema.class);
    configureTable("table3", mockTable3, mockSchema3);
    configureTableScan(mockTable3, mockTableScan3, null, null, "(SELECT * FROM 'file3.parquet')");
    when(mockSqlQueryProcessor.parseConditionExpression(eq(condition3), any(), any()))
        .thenReturn(expression3);

    String sql =
        "SELECT * FROM table1 UNION ALL SELECT * FROM {{table2}} UNION ALL SELECT * FROM {{table3}}";
    List<TableFilter> tableFilters = Arrays.asList(filter1, filter2, filter3);
    Pair<String, List<InputFiles>> result =
        icebergScanExecutor.executeTableScansAndUpdateSql(sql, tableFilters, mockConnection, true);

    assertThat(result).isNotNull();
    assertThat(result.getLeft())
        .isEqualTo(
            "SELECT * FROM (SELECT * FROM 'file1.parquet') UNION ALL SELECT * FROM (SELECT * FROM 'file2.parquet') UNION ALL SELECT * FROM (SELECT * FROM 'file3.parquet')");
    assertThat(result.getRight()).isNotEmpty();
    verify(mockFileIO, atLeastOnce()).isDownloadable(anyString(), anyLong());
    verify(mockSqlQueryProcessor)
        .parseConditionExpression(eq(condition3), eq(mockSchema3), eq(conditionParams));
  }

  @Test
  void testExecuteTableScansAndUpdateSql_WithSql() {
    when(mockConfig.isAllowFullTableScan()).thenReturn(true);
    when(mockEngine.getSqlQueryProcessor()).thenReturn(mockSqlQueryProcessor);
    when(mockEngine.getSchemaEvolution()).thenReturn(mockSchemaEvolution);

    String tableSql1 = "SELECT id, name FROM table1 WHERE id = 1";
    Map<Integer, Object> params1 = Map.of(1, "value1");
    TableFilter filter1 = new TableFilter("table1", null, "{{table1}}", params1, tableSql1);
    Expression expression1 = Expressions.equal("id", 1);

    configureTable("table1", mockTable, null);
    Pair<CloseableIterable<FileScanTask>, List<DataFile>> mockScanData = createMockFileScanTasks();
    configureTableScan(
        mockTable,
        mockTableScan,
        mockScanData.getLeft(),
        mockScanData.getRight(),
        "(SELECT * FROM 'file1.parquet')");
    when(mockSqlQueryProcessor.process(eq(tableSql1), any(), eq(mockConnection), any()))
        .thenReturn(
            Pair.of(
                "SELECT id, name FROM {{processed_table1}} WHERE id = 1",
                Arrays.asList(new TableFilter("table1", expression1, "{{processed_table1}}"))));

    String tableSql2 = "SELECT id, name FROM table2 WHERE id IN (1,2)";
    Map<Integer, Object> params2 = Map.of(1, "value2");
    TableFilter filter2 = new TableFilter("table2", null, "{{table2}}", params2, tableSql2);
    Expression expression2 = Expressions.in("id", 1, 2);
    Table mockTable2 = mock(Table.class);
    TableScan mockTableScan2 = mock(TableScan.class);

    configureTable("table2", mockTable2, null);
    configureTableScan(mockTable2, mockTableScan2, null, null, "(SELECT * FROM 'file2.parquet')");
    when(mockSqlQueryProcessor.process(eq(tableSql2), any(), eq(mockConnection), any()))
        .thenReturn(
            Pair.of(
                "SELECT id, name FROM {{processed_table2}} WHERE id IN (1,2)",
                Arrays.asList(new TableFilter("table2", expression2, "{{processed_table2}}"))));

    String sql =
        "SELECT id, name FROM {{table1}} WHERE id = 1 UNION ALL SELECT id, name FROM {{table2}} WHERE id IN (1,2))";
    List<TableFilter> tableFilters = Arrays.asList(filter1, filter2);
    Pair<String, List<InputFiles>> result =
        icebergScanExecutor.executeTableScansAndUpdateSql(sql, tableFilters, mockConnection, false);

    assertThat(result).isNotNull();
    assertThat(result.getLeft())
        .isEqualTo(
            "SELECT id, name FROM (SELECT * FROM 'file1.parquet') WHERE id = 1 UNION ALL SELECT id, name FROM (SELECT * FROM 'file2.parquet') WHERE id IN (1,2))");
    assertThat(result.getRight()).isNotEmpty();
    verify(mockFileIO, never()).isDownloadable(anyString(), anyLong());
    verify(mockSqlQueryProcessor).process(eq(tableSql1), eq(params1), eq(mockConnection), any());
    verify(mockSqlQueryProcessor).process(eq(tableSql2), eq(params2), eq(mockConnection), any());
  }

  @Test
  void testExecuteTableScansAndUpdateSql_WithInvalidTableFilter() {
    String sql = "SELECT * FROM {{table1}}";
    TableFilter filter = new TableFilter("table1", "SELECT * FROM table1", "{{table1}}", null, sql);
    List<TableFilter> tableFilters = Arrays.asList(filter);

    when(mockEngine.getSqlQueryProcessor()).thenReturn(mockSqlQueryProcessor);
    when(mockSqlQueryProcessor.process(eq(sql), any(), eq(mockConnection), any()))
        .thenReturn(
            Pair.of(
                "SELECT * FROM processed_table",
                Arrays.asList(
                    new TableFilter("table1", Expressions.alwaysTrue()),
                    new TableFilter("extra_table", Expressions.alwaysTrue()))));

    assertThatThrownBy(
            () ->
                icebergScanExecutor.executeTableScansAndUpdateSql(
                    sql, tableFilters, mockConnection, true))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid table filter");
  }

  @Test
  void testValidateFullTableScan() {
    when(mockConfig.isAllowFullTableScan()).thenReturn(false);

    assertThatThrownBy(
            () ->
                icebergScanExecutor.validateFullTableScan(
                    org.apache.iceberg.expressions.Expressions.alwaysTrue(), "testTable"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Operation prohibited: Full table scan");

    when(mockConfig.isAllowFullTableScan()).thenReturn(true);
    assertThatCode(
            () ->
                icebergScanExecutor.validateFullTableScan(
                    org.apache.iceberg.expressions.Expressions.alwaysTrue(), "testTable"))
        .doesNotThrowAnyException();
  }

  @Test
  void testFixExpression() {
    org.apache.iceberg.expressions.Expression originalExpression =
        Expressions.resolveExpression(
                Expressions.and(
                    Expressions.equal("id", 1),
                    Expressions.or(Expressions.lessThan("value", 10), Expressions.isNull("name"))))
            .getExpression();

    org.apache.iceberg.expressions.Expression fixedExpression =
        icebergScanExecutor.fixExpression(originalExpression);

    assertThat(fixedExpression).isNotNull();
    assertThat(fixedExpression).isNotEqualTo(originalExpression);
    assertThat(fixedExpression.toString()).isEqualTo(originalExpression.toString());
  }

  @Test
  void testFixPredicateWithLargeInClause() {
    List<Integer> largeList = new ArrayList<>();
    for (int i = 0; i < 250; i++) {
      largeList.add(i);
    }
    org.apache.iceberg.expressions.Expression expression =
        Expressions.resolveExpression(Expressions.in("id", largeList)).getExpression();

    org.apache.iceberg.expressions.Expression result =
        icebergScanExecutor.fixExpression(expression);

    List<Integer> list1 = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      list1.add(i);
    }
    List<Integer> list2 = new ArrayList<>();
    for (int i = 200; i < 250; i++) {
      list2.add(i);
    }
    Expression expectedExpression =
        Expressions.resolveExpression(
            Expressions.or(Expressions.in("id", list1), Expressions.in("id", list2)));
    assertThat(result).isNotNull();
    assertThat(result.toString()).isEqualTo(expectedExpression.toString());
  }

  @Test
  void testGetPartitionLevelRecordCounts() throws IOException {
    PartitionField mockPartitionField = mock(PartitionField.class);
    List<PartitionData> partitionDataList =
        Arrays.asList(
            new PartitionData(1, Arrays.asList(Pair.of(mockPartitionField, "2023-01-01"))),
            new PartitionData(2, Arrays.asList(Pair.of(mockPartitionField, "2023-01-02"))));
    ResolvedExpression expression =
        Expressions.resolveExpression(Expressions.equal("date", "2023-01-01"));

    // Mock file scan tasks
    FileScanTask task1 = mockFileScanTask(1, 100L);
    FileScanTask task2 = mockFileScanTask(2, 200L);

    when(mockTable.newScan()).thenReturn(mockTableScan);
    when(mockTableScan.filter(expression.getExpression())).thenReturn(mockTableScan);
    when(mockTable.specs()).thenReturn(Map.of(1, mockPartitionSpec1, 2, mockPartitionSpec2));
    when(mockTableScan.planFiles())
        .thenReturn(CloseableIterable.withNoopClose(Arrays.asList(task1, task2)));

    try (MockedStatic<WriteUtil> mockedStatic = mockStatic(WriteUtil.class)) {
      // Mock the static method
      mockedStatic
          .when(() -> WriteUtil.getPartitionFilterExpression(mockTable, partitionDataList))
          .thenReturn(expression);
      // Test with PartitionData list
      List<Pair<PartitionData, Long>> resultWithList =
          icebergScanExecutor.getPartitionLevelRecordCounts(mockTable, partitionDataList);

      // Test with Expression
      List<Pair<PartitionData, Long>> resultWithExpression =
          icebergScanExecutor.getPartitionLevelRecordCounts(mockTable, expression);

      // Verify
      assertThat(resultWithList).hasSize(2);
      assertThat(resultWithExpression).hasSize(2);

      List<Pair<Integer, Long>> expected = List.of(Pair.of(1, 100L), Pair.of(2, 200L));
      for (List<Pair<PartitionData, Long>> result :
          Arrays.asList(resultWithList, resultWithExpression)) {
        List<Pair<Integer, Long>> actual =
            result.stream()
                .map(r -> Pair.of(r.getLeft().getSpecId(), r.getRight()))
                .collect(Collectors.toList());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
      }
    }
  }

  @Test
  void testGetPartitionLevelRecordCountsWithDeleteFiles() throws IOException {
    ResolvedExpression expression = Expressions.resolveExpression(Expressions.alwaysTrue());
    FileScanTask taskWithDeletes = mock(FileScanTask.class);
    when(mockTable.newScan()).thenReturn(mockTableScan);
    when(mockTableScan.filter(expression.getExpression())).thenReturn(mockTableScan);
    when(taskWithDeletes.deletes()).thenReturn(List.of(mock(DeleteFile.class)));
    when(mockTableScan.planFiles())
        .thenReturn(CloseableIterable.withNoopClose(List.of(taskWithDeletes)));

    assertThatThrownBy(
            () -> icebergScanExecutor.getPartitionLevelRecordCounts(mockTable, expression))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Found delete file. 'merge-on-read' mode is not supported.");
  }

  @Test
  void testCheckTableScanLimits() throws IOException {
    Expression fileFilter = Expressions.alwaysTrue();

    DataFile file1 = mock(DataFile.class);
    when(file1.fileSizeInBytes()).thenReturn(1024L * 1024L);
    DataFile file2 = mock(DataFile.class);
    when(file2.fileSizeInBytes()).thenReturn(2048L * 1024L);
    FileScanTask task1 = mock(FileScanTask.class);
    when(task1.file()).thenReturn(file1);
    when(task1.deletes()).thenReturn(List.of());
    FileScanTask task2 = mock(FileScanTask.class);
    when(task2.file()).thenReturn(file2);
    when(task2.deletes()).thenReturn(List.of());

    when(mockConfig.isAllowFullTableScan()).thenReturn(true);
    when(mockTable.newScan()).thenReturn(mockTableScan);
    when(mockTableScan.filter(any())).thenReturn(mockTableScan);
    when(mockTableScan.planFiles())
        .thenReturn(CloseableIterable.withNoopClose(Arrays.asList(task1, task2)));
    when(mockConfig.getTotalFileSizePerScanLimitInMiB()).thenReturn(1);

    assertThatThrownBy(() -> icebergScanExecutor.executeTableScan(mockTable, fileFilter))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Total file size per scan exceeds limit");
  }

  private void configureTable(String tableName, Table table, Schema schema) {
    when(mockEngine.getTable(tableName)).thenReturn(table);
    if (schema != null) {
      when(table.schema()).thenReturn(schema);
    }
  }

  private Map<String, String> configureTableScan(
      Table table,
      TableScan tableScan,
      CloseableIterable<FileScanTask> fileScanTasks,
      List<DataFile> dataFiles,
      String selectSqlForDataFiles) {
    when(table.newScan()).thenReturn(tableScan);
    when(tableScan.filter(any())).thenReturn(tableScan);
    if (fileScanTasks == null) {
      when(tableScan.planFiles()).thenReturn(CloseableIterable.empty());
    } else {
      when(tableScan.planFiles()).thenReturn(fileScanTasks);
      when(tableScan.table()).thenReturn(table);
      when(table.io()).thenReturn(mockFileIO);
    }
    when(mockSchemaEvolution.getSelectSQLForDataFiles(
            eq(table), any(), any(), eq(false), eq(false)))
        .thenReturn(selectSqlForDataFiles);
    Map<String, String> downloadedFiles = new HashMap<>();
    if (dataFiles != null) {
      Map<String, DataFile> dataFileMap =
          dataFiles.stream().collect(Collectors.toMap(DataFile::location, Function.identity()));
      // Mock the newInputFiles method
      when(mockFileIO.newInputFiles(anyList()))
          .thenAnswer(
              invocation -> {
                List<String> fileNames = invocation.getArgument(0);
                return new DefaultInputFiles(
                    fileNames.stream()
                        .map(
                            name -> {
                              DataFile dataFile = dataFileMap.get(name);
                              String localFileName = UUID.randomUUID().toString();
                              downloadedFiles.put(name, localFileName);
                              return new DefaultInputFile(
                                  localFileName, name, dataFile.fileSizeInBytes());
                            })
                        .collect(Collectors.toList()));
              });
    }
    return downloadedFiles;
  }

  private Pair<CloseableIterable<FileScanTask>, List<DataFile>> createMockFileScanTasks() {
    // Create mock DataFiles
    DataFile file1 = mock(DataFile.class);
    when(file1.location()).thenReturn("remote://path/file1.parquet");
    when(file1.fileSizeInBytes()).thenReturn(1024L);

    DataFile file2 = mock(DataFile.class);
    when(file2.location()).thenReturn("remote://path/file2.parquet");
    when(file2.fileSizeInBytes()).thenReturn(2048L);

    DataFile file3 = mock(DataFile.class);
    when(file3.location()).thenReturn("remote://path/file3.parquet");
    when(file3.fileSizeInBytes()).thenReturn(4096L);

    DataFile file4 = mock(DataFile.class);
    when(file4.location()).thenReturn("remote://path/file4.parquet");
    when(file4.fileSizeInBytes()).thenReturn(8192L);

    // Create FileScanTasks
    FileScanTask task1 = mock(FileScanTask.class);
    when(task1.file()).thenReturn(file1);
    when(task1.deletes()).thenReturn(List.of());

    FileScanTask task2 = mock(FileScanTask.class);
    when(task2.file()).thenReturn(file2);
    when(task2.deletes()).thenReturn(List.of());

    FileScanTask task3 = mock(FileScanTask.class);
    when(task3.file()).thenReturn(file3);
    when(task3.deletes()).thenReturn(List.of());

    FileScanTask task4 = mock(FileScanTask.class);
    when(task4.file()).thenReturn(file4);
    when(task4.deletes()).thenReturn(List.of());

    return Pair.of(
        CloseableIterable.withNoopClose(List.of(task1, task2, task3, task4)),
        List.of(file1, file2, file3, file4));
  }

  private FileScanTask mockFileScanTask(int specId, long recordCount) {
    FileScanTask task = mock(FileScanTask.class);
    DataFile dataFile = mock(DataFile.class);
    StructLike partition = mock(StructLike.class);

    when(task.file()).thenReturn(dataFile);
    when(dataFile.specId()).thenReturn(specId);
    when(dataFile.partition()).thenReturn(partition);
    when(dataFile.recordCount()).thenReturn(recordCount);
    when(task.deletes()).thenReturn(List.of());

    return task;
  }
}
