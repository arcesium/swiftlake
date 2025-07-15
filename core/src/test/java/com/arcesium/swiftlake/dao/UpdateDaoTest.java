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
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.argThat;
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
class UpdateDaoTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private SwiftLakeSqlSessionFactory mockSqlSessionFactory;

  @Mock private SqlSession mockSqlSession;

  private UpdateDao updateDao;

  @BeforeEach
  void setUp() {
    updateDao = new UpdateDao(mockSwiftLakeEngine);
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
  }

  @Test
  void testGetMatchingFiles() {
    String destinationTableName = "test_table";
    String condition = "id > 100";
    List<String> expectedFiles = Arrays.asList("file1.parquet", "file2.parquet");
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList(eq("Update.getMatchingFiles"), anyMap()))
        .thenReturn((List) expectedFiles);

    List<String> result = updateDao.getMatchingFiles(destinationTableName, condition);

    assertThat(result).isEqualTo(expectedFiles);
    verify(mockSqlSession).close();

    // Verify the correct parameters were passed
    verify(mockSqlSession)
        .selectList(
            eq("Update.getMatchingFiles"),
            argThat(
                map -> {
                  Map<String, Object> params = (Map<String, Object>) map;
                  return params.get("destinationTableName").equals(destinationTableName)
                      && params.get("condition").equals(condition);
                }));
  }

  @Test
  void testGetUpdateSql() {
    String destinationTableWithMatchingFiles = "test_table_with_matches";
    String condition = "id > 100";
    Map<String, String> updateDataMap = new HashMap<>();
    updateDataMap.put("status", "'UPDATED'");
    updateDataMap.put("modified_date", "CURRENT_TIMESTAMP");

    String expectedSql = "testGetUpdateSql...";
    when(mockSqlSessionFactory.getSql(eq("Update.update"), anyMap())).thenReturn(expectedSql);

    String result =
        updateDao.getUpdateSql(destinationTableWithMatchingFiles, condition, updateDataMap);

    assertThat(result).isEqualTo(expectedSql);

    // Verify the correct parameters were passed
    verify(mockSqlSessionFactory)
        .getSql(
            eq("Update.update"),
            argThat(
                map -> {
                  Map<String, Object> params = (Map<String, Object>) map;
                  return params
                          .get("destinationTableWithMatchingFiles")
                          .equals(destinationTableWithMatchingFiles)
                      && params.get("condition").equals(condition)
                      && params.get("updateDataMap").equals(updateDataMap);
                }));
  }

  @Test
  void testGetMatchingFilesWithNoResults() {
    String destinationTableName = "empty_table";
    String condition = "1=0";
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList(eq("Update.getMatchingFiles"), anyMap()))
        .thenReturn(Arrays.asList());

    List<String> result = updateDao.getMatchingFiles(destinationTableName, condition);

    assertThat(result).isEmpty();
    verify(mockSqlSession).close();
  }

  @Test
  void testGetUpdateSqlWithNoUpdateData() {
    String destinationTableWithMatchingFiles = "test_table";
    String condition = "id > 100";
    Map<String, String> updateDataMap = new HashMap<>();

    String expectedSql = "testGetUpdateSqlWithNoUpdateData...";

    when(mockSqlSessionFactory.getSql(eq("Update.update"), anyMap())).thenReturn(expectedSql);

    String result =
        updateDao.getUpdateSql(destinationTableWithMatchingFiles, condition, updateDataMap);

    assertThat(result).isEqualTo(expectedSql);
  }

  @Test
  void testGetMatchingFilesWithException() {
    String destinationTableName = "test_table";
    String condition = "id > 100";
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList(eq("Update.getMatchingFiles"), anyMap()))
        .thenThrow(new RuntimeException("Database error"));

    assertThatThrownBy(() -> updateDao.getMatchingFiles(destinationTableName, condition))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Database error");

    verify(mockSqlSession).close();
  }

  @Test
  void testGetUpdateSqlWithException() {
    String destinationTableWithMatchingFiles = "test_table";
    String condition = "id > 100";
    Map<String, String> updateDataMap = new HashMap<>();
    updateDataMap.put("status", "'UPDATED'");
    when(mockSqlSessionFactory.getSql(eq("Update.update"), anyMap()))
        .thenThrow(new RuntimeException("SQL generation error"));

    assertThatThrownBy(
            () ->
                updateDao.getUpdateSql(destinationTableWithMatchingFiles, condition, updateDataMap))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("SQL generation error");
  }
}
