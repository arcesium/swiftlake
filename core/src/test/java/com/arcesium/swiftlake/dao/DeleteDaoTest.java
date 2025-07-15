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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DeleteDaoTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private SwiftLakeSqlSessionFactory mockSqlSessionFactory;

  @Mock private SqlSession mockSqlSession;

  private DeleteDao deleteDao;

  @BeforeEach
  void setUp() {
    deleteDao = new DeleteDao(mockSwiftLakeEngine);
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
  }

  @Test
  void testGetMatchingFiles() {
    String destinationTableName = "testTable";
    String condition = "column = 'value'";
    List<String> expectedFiles = Arrays.asList("file1.parquet", "file2.parquet");
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList(eq("Delete.getMatchingFiles"), any(Map.class)))
        .thenReturn((List) expectedFiles);

    List<String> result = deleteDao.getMatchingFiles(destinationTableName, condition);

    assertThat(result).isEqualTo(expectedFiles);
    verify(mockSqlSession).selectList(eq("Delete.getMatchingFiles"), any(Map.class));
  }

  @Test
  void testGetMatchingFilesWithEmptyResult() {
    String destinationTableName = "emptyTable";
    String condition = "column = 'nonexistent'";
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList(eq("Delete.getMatchingFiles"), any(Map.class)))
        .thenReturn(Arrays.asList());

    List<String> result = deleteDao.getMatchingFiles(destinationTableName, condition);

    assertThat(result).isEmpty();
    verify(mockSqlSession).selectList(eq("Delete.getMatchingFiles"), any(Map.class));
  }

  @Test
  void testGetDeleteSql() {
    String destinationTableWithMatchingFiles = "testTable";
    String condition = "column = 'value'";
    String expectedSql = "DELETE FROM testTable WHERE column = 'value'";

    when(mockSqlSessionFactory.getSql(eq("Delete.delete"), any(Map.class))).thenReturn(expectedSql);

    String result = deleteDao.getDeleteSql(destinationTableWithMatchingFiles, condition);

    assertThat(result).isEqualTo(expectedSql);
    verify(mockSqlSessionFactory).getSql(eq("Delete.delete"), any(Map.class));
  }

  @Test
  void testGetMatchingFilesWithNullCondition() {
    String destinationTableName = "testTable";
    String condition = null;
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList(eq("Delete.getMatchingFiles"), any(Map.class)))
        .thenReturn(Arrays.asList());

    List<String> result = deleteDao.getMatchingFiles(destinationTableName, condition);

    assertThat(result).isEmpty();
    verify(mockSqlSession).selectList(eq("Delete.getMatchingFiles"), any(Map.class));
  }

  @Test
  void testGetDeleteSqlWithNullCondition() {
    String destinationTableWithMatchingFiles = "testTable";
    String condition = null;
    String expectedSql = "DELETE FROM testTable";

    when(mockSqlSessionFactory.getSql(eq("Delete.delete"), any(Map.class))).thenReturn(expectedSql);

    String result = deleteDao.getDeleteSql(destinationTableWithMatchingFiles, condition);

    assertThat(result).isEqualTo(expectedSql);
    verify(mockSqlSessionFactory).getSql(eq("Delete.delete"), any(Map.class));
  }

  @Test
  void testGetMatchingFilesWithLongTableName() {
    String destinationTableName = "very_long_table_name_that_might_cause_issues_in_some_databases";
    String condition = "column = 'value'";
    List<String> expectedFiles = Arrays.asList("file1.parquet");
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList(eq("Delete.getMatchingFiles"), any(Map.class)))
        .thenReturn((List) expectedFiles);

    List<String> result = deleteDao.getMatchingFiles(destinationTableName, condition);

    assertThat(result).isEqualTo(expectedFiles);
    verify(mockSqlSession).selectList(eq("Delete.getMatchingFiles"), any(Map.class));
  }
}
