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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.commands.SCD1MergeProperties;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import java.util.Arrays;
import java.util.List;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SCD1MergeDaoTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private SwiftLakeSqlSessionFactory mockSqlSessionFactory;

  @Mock private SqlSession mockSqlSession;

  private SCD1MergeDao scd1MergeDao;

  @BeforeEach
  void setUp() {
    scd1MergeDao = new SCD1MergeDao(mockSwiftLakeEngine);
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
  }

  @Test
  void testMergeFindDiffs() {
    SCD1MergeProperties properties = new SCD1MergeProperties();
    properties.setDestinationTableName("target_table");
    properties.setSourceTableName("source_table");
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    scd1MergeDao.mergeFindDiffs(properties);

    verify(mockSqlSession).update("SCD1Merge.mergeFindDiffs", properties);
    verify(mockSqlSession).close();
  }

  @Test
  void testGetMergeUpsertsSql() {
    SCD1MergeProperties properties = new SCD1MergeProperties();
    properties.setDestinationTableName("target_table");
    properties.setSourceTableName("source_table");

    String expectedSql = "testGetMergeUpsertsSql...";
    when(mockSqlSessionFactory.getSql("SCD1Merge.mergeUpserts", properties))
        .thenReturn(expectedSql);
    String result = scd1MergeDao.getMergeUpsertsSql(properties);

    assertThat(result).isEqualTo(expectedSql);
  }

  @Test
  void testGetFileNames() {
    String dataPath = "/data/path";
    List<String> expectedFileNames = Arrays.asList("file1.parquet", "file2.parquet");
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList("SCD1Merge.getFileNames", dataPath))
        .thenReturn((List) expectedFileNames);

    List<String> result = scd1MergeDao.getFileNames(dataPath);

    assertThat(result).isEqualTo(expectedFileNames);
    verify(mockSqlSession).close();
  }

  @Test
  void testSaveDistinctFileNames() {
    SCD1MergeProperties properties = new SCD1MergeProperties();
    properties.setDestinationTableName("target_table");
    properties.setSourceTableName("source_table");
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    scd1MergeDao.saveDistinctFileNames(properties);

    verify(mockSqlSession).update("SCD1Merge.saveDistinctFileNames", properties);
    verify(mockSqlSession).close();
  }

  @Test
  void testMergeFindDiffsWithException() {
    SCD1MergeProperties properties = new SCD1MergeProperties();
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    doThrow(new RuntimeException("Database error")).when(mockSqlSession).update(anyString(), any());
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    assertThatThrownBy(() -> scd1MergeDao.mergeFindDiffs(properties))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Database error");

    verify(mockSqlSession).close();
  }

  @Test
  void testGetMergeUpsertsSqlWithException() {
    SCD1MergeProperties properties = new SCD1MergeProperties();
    when(mockSqlSessionFactory.getSql(anyString(), any()))
        .thenThrow(new RuntimeException("SQL error"));

    assertThatThrownBy(() -> scd1MergeDao.getMergeUpsertsSql(properties))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("SQL error");
  }

  @Test
  void testGetFileNamesWithException() {
    String dataPath = "/data/path";
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList(anyString(), any()))
        .thenThrow(new RuntimeException("File error"));

    assertThatThrownBy(() -> scd1MergeDao.getFileNames(dataPath))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("File error");

    verify(mockSqlSession).close();
  }

  @Test
  void testSaveDistinctFileNamesWithException() {
    SCD1MergeProperties properties = new SCD1MergeProperties();
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    doThrow(new RuntimeException("Save error")).when(mockSqlSession).update(anyString(), any());

    assertThatThrownBy(() -> scd1MergeDao.saveDistinctFileNames(properties))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Save error");

    verify(mockSqlSession).close();
  }
}
