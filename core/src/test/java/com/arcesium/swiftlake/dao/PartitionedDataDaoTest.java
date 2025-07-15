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
package com.arcesium.swiftlake.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PartitionedDataDaoTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private SwiftLakeSqlSessionFactory mockSqlSessionFactory;

  @Mock private SqlSession mockSqlSession;

  @Mock private Connection mockConnection;

  @Mock private Statement mockStatement;

  @Mock private Configuration mockConfiguration;

  @Mock private MappedStatement mockMappedStatement;

  @Mock private BoundSql mockBoundSql;

  @Mock private ResultSet mockResultSet;

  private PartitionedDataDao partitionedDataDao;

  @BeforeEach
  void setUp() throws Exception {
    partitionedDataDao = new PartitionedDataDao(mockSwiftLakeEngine);

    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
  }

  @Test
  void testGetDistinctPartitions() throws Exception {
    String table = "test_table";
    List<String> columns = Arrays.asList("col1", "col2");
    String currentFlagColumn = "current_flag";
    String effectiveEndColumn = "effective_end";
    Set<String> singleBucketColumns = new HashSet<>(Arrays.asList("col1"));

    List<Map<String, Object>> expectedResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("col1", "value1");
    row.put("col2", "value2");
    expectedResult.add(row);

    when(mockSqlSession.getConnection()).thenReturn(mockConnection);
    when(mockSqlSession.getConfiguration()).thenReturn(mockConfiguration);
    when(mockConfiguration.getMappedStatement(anyString())).thenReturn(mockMappedStatement);
    when(mockMappedStatement.getBoundSql(any())).thenReturn(mockBoundSql);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(any())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getMetaData()).thenReturn(mock(ResultSetMetaData.class));
    when(mockResultSet.getMetaData().getColumnCount()).thenReturn(2);
    when(mockResultSet.getMetaData().getColumnName(1)).thenReturn("col1");
    when(mockResultSet.getMetaData().getColumnName(2)).thenReturn("col2");
    when(mockResultSet.getObject("col1")).thenReturn("value1");
    when(mockResultSet.getObject("col2")).thenReturn("value2");

    List<Map<String, Object>> result =
        partitionedDataDao.getDistinctPartitions(
            table, columns, currentFlagColumn, effectiveEndColumn, singleBucketColumns);

    assertThat(result).isEqualTo(expectedResult);
    verify(mockSqlSession).close();
  }

  @Test
  void testGetDistinctPartitionsTransformed() throws Exception {
    String partitionsDataTable = "partitions_data";
    List<String> partitionColumns = Arrays.asList("col1", "col2");

    List<Map<String, Object>> expectedResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("col1", "value1");
    row.put("col2", "value2");
    expectedResult.add(row);

    when(mockSqlSession.getConnection()).thenReturn(mockConnection);
    when(mockSqlSession.getConfiguration()).thenReturn(mockConfiguration);
    when(mockConfiguration.getMappedStatement(anyString())).thenReturn(mockMappedStatement);
    when(mockMappedStatement.getBoundSql(any())).thenReturn(mockBoundSql);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(any())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getMetaData()).thenReturn(mock(ResultSetMetaData.class));
    when(mockResultSet.getMetaData().getColumnCount()).thenReturn(2);
    when(mockResultSet.getMetaData().getColumnName(1)).thenReturn("col1");
    when(mockResultSet.getMetaData().getColumnName(2)).thenReturn("col2");
    when(mockResultSet.getObject("col1")).thenReturn("value1");
    when(mockResultSet.getObject("col2")).thenReturn("value2");

    List<Map<String, Object>> result =
        partitionedDataDao.getDistinctPartitionsTransformed(partitionsDataTable, partitionColumns);

    assertThat(result).isEqualTo(expectedResult);
    verify(mockSqlSession).close();
  }

  @Test
  void testGetDistinctPartitionValuesFromTable() throws Exception {
    String table = "test_table";
    List<String> partitionColumns = Arrays.asList("col1", "col2");

    List<Map<String, Object>> expectedResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("col1", "value1");
    row.put("col2", "value2");
    expectedResult.add(row);

    when(mockSqlSession.getConnection()).thenReturn(mockConnection);
    when(mockSqlSession.getConfiguration()).thenReturn(mockConfiguration);
    when(mockConfiguration.getMappedStatement(anyString())).thenReturn(mockMappedStatement);
    when(mockMappedStatement.getBoundSql(any())).thenReturn(mockBoundSql);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(any())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getMetaData()).thenReturn(mock(ResultSetMetaData.class));
    when(mockResultSet.getMetaData().getColumnCount()).thenReturn(2);
    when(mockResultSet.getMetaData().getColumnName(1)).thenReturn("col1");
    when(mockResultSet.getMetaData().getColumnName(2)).thenReturn("col2");
    when(mockResultSet.getObject("col1")).thenReturn("value1");
    when(mockResultSet.getObject("col2")).thenReturn("value2");

    List<Map<String, Object>> result =
        partitionedDataDao.getDistinctPartitionValuesFromTable(table, partitionColumns);

    assertThat(result).isEqualTo(expectedResult);
    verify(mockSqlSession).close();
  }

  @Test
  void testWrite() {
    String dataSql = "SELECT * FROM test_table";
    String partitionsDataTable = "partitions_data";
    String outputPath = "/output/path";
    String compression = "snappy";
    List<String> projectedColumns = Arrays.asList("col1", "col2");
    List<String> partitionColumns = Arrays.asList("partition_col1", "partition_col2");
    String effectiveEndColumn = "effective_end";
    Set<String> singleBucketColumns = new HashSet<>(Arrays.asList("col1"));

    partitionedDataDao.write(
        dataSql,
        partitionsDataTable,
        outputPath,
        compression,
        projectedColumns,
        partitionColumns,
        effectiveEndColumn,
        singleBucketColumns);

    verify(mockSqlSession)
        .update(
            eq("PartitionedData.write"),
            argThat(
                p -> {
                  Map<String, Object> params = (Map<String, Object>) p;
                  return params.get("dataSql").equals(dataSql)
                      && params.get("partitionsDataTable").equals(partitionsDataTable)
                      && params.get("outputPath").equals(outputPath)
                      && params.get("compression").equals(compression)
                      && params.get("projectedColumns").equals(projectedColumns)
                      && params.get("partitionColumns").equals(partitionColumns)
                      && params.get("effectiveEndColumn").equals(effectiveEndColumn)
                      && params.get("singleBucketColumns").equals(singleBucketColumns);
                }));
    verify(mockSqlSession).close();
  }

  @Test
  void testSqlExceptionHandling() throws Exception {
    when(mockSqlSession.getConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenThrow(new SQLException("Test SQL Exception"));

    assertThatThrownBy(
            () ->
                partitionedDataDao.getDistinctPartitions(
                    "table",
                    Collections.emptyList(),
                    "current_flag",
                    "effective_end",
                    Collections.emptySet()))
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(SQLException.class)
        .hasMessageContaining("Test SQL Exception");
  }
}
