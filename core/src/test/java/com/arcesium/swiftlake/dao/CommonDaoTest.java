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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CommonDaoTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private SwiftLakeSqlSessionFactory mockSqlSessionFactory;

  @Mock private SqlSession mockSqlSession;

  private CommonDao commonDao;

  @BeforeEach
  void setUp() {
    commonDao = new CommonDao(mockSwiftLakeEngine);
  }

  @Test
  void testGetDistinctFileNames() {
    List<String> expectedFileNames = Arrays.asList("file1.txt", "file2.txt");
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList("Common.getDistinctFileNames", "testPath"))
        .thenReturn((List) expectedFileNames);

    List<String> result = commonDao.getDistinctFileNames("testPath");

    assertThat(result).isEqualTo(expectedFileNames);
    verify(mockSqlSession).selectList("Common.getDistinctFileNames", "testPath");
  }

  @Test
  void testMergeCardinalityCheck() {
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectOne("Common.mergeCardinalityCheck", "testTable")).thenReturn(true);

    Boolean result = commonDao.mergeCardinalityCheck("testTable");

    assertThat(result).isTrue();
    verify(mockSqlSession).selectOne("Common.mergeCardinalityCheck", "testTable");
  }

  @Test
  void testGetTotalRowCount() {
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectOne(eq("Common.getTotalRowCount"), any(Map.class))).thenReturn(100L);

    Long result = commonDao.getTotalRowCount("testTable");

    assertThat(result).isEqualTo(100L);
    verify(mockSqlSession).selectOne(eq("Common.getTotalRowCount"), any(Map.class));
  }

  @Test
  void testIsTableEmpty() {
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectOne("Common.isTableEmpty", "testTable")).thenReturn(true);

    Boolean result = commonDao.isTableEmpty("testTable");

    assertThat(result).isTrue();
    verify(mockSqlSession).selectOne("Common.isTableEmpty", "testTable");
  }

  @Test
  void testGetTotalRowCountWithCondition() {
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectOne(eq("Common.getTotalRowCount"), any(Map.class))).thenReturn(50L);

    Long result = commonDao.getTotalRowCount("testTable", "column = 'value'");

    assertThat(result).isEqualTo(50L);
    verify(mockSqlSession).selectOne(eq("Common.getTotalRowCount"), any(Map.class));
  }

  @Test
  void testGetDataSqlFromParquetFiles() {
    List<String> files = Arrays.asList("file1.parquet", "file2.parquet");
    List<String> columns = Arrays.asList("col1", "col2");
    String expectedSql = "SELECT col1, col2 FROM READ_PARQUET('file1.parquet', 'file2.parquet')";
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
    when(mockSqlSessionFactory.getSql(eq("Common.getDataFromParquetFiles"), any(Map.class)))
        .thenReturn(expectedSql);

    String result =
        commonDao.getDataSqlFromParquetFiles(files, columns, true, false, false, false, false);

    assertThat(result).isEqualTo(expectedSql);
    verify(mockSqlSessionFactory).getSql(eq("Common.getDataFromParquetFiles"), any(Map.class));
  }

  @Test
  void testGetDistinctValues() {
    List<String> columns = Arrays.asList("col1", "col2");
    List<Map<String, Object>> expectedResult =
        Arrays.asList(
            new HashMap<String, Object>() {
              {
                put("col1", "value1");
                put("col2", "value2");
              }
            },
            new HashMap<String, Object>() {
              {
                put("col1", "value3");
                put("col2", "value4");
              }
            });
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList(eq("Common.getDistinctValues"), any(Map.class)))
        .thenReturn((List) expectedResult);

    List<Map<String, Object>> result = commonDao.getDistinctValues("testTable", columns);

    assertThat(result).isEqualTo(expectedResult);
    verify(mockSqlSession).selectList(eq("Common.getDistinctValues"), any(Map.class));
  }

  @Test
  void testGetCopyToFileSql() {
    String expectedSql = "COPY (SELECT * FROM table) TO 'output.csv' (FORMAT CSV)";
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
    when(mockSqlSessionFactory.getSql(eq("Common.copyToFile"), any(Map.class)))
        .thenReturn(expectedSql);

    String result =
        commonDao.getCopyToFileSql("SELECT * FROM table", null, "output.csv", "FORMAT CSV");

    assertThat(result).isEqualTo(expectedSql);
    verify(mockSqlSessionFactory).getSql(eq("Common.copyToFile"), any(Map.class));
  }

  @Test
  void testGetDropTableSql() {
    String result = commonDao.getDropTableSql("testTable");

    assertThat(result).isEqualTo("DROP TABLE testTable");
  }

  @Test
  void testGetDuckDBRuntimeMetrics() {
    List<Map<String, Object>> expectedMetrics =
        Arrays.asList(
            new HashMap<String, Object>() {
              {
                put("metric", "value1");
              }
            },
            new HashMap<String, Object>() {
              {
                put("metric", "value2");
              }
            });
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList("Common.getDuckDBRuntimeMetrics"))
        .thenReturn((List) expectedMetrics);

    List<Map<String, Object>> result = commonDao.getDuckDBRuntimeMetrics();

    assertThat(result).isEqualTo(expectedMetrics);
    verify(mockSqlSession).selectList("Common.getDuckDBRuntimeMetrics");
  }
}
