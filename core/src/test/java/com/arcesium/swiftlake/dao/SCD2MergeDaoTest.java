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
import com.arcesium.swiftlake.commands.SCD2MergeProperties;
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
class SCD2MergeDaoTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private SwiftLakeSqlSessionFactory mockSqlSessionFactory;

  @Mock private SqlSession mockSqlSession;

  private SCD2MergeDao scd2MergeDao;

  @BeforeEach
  void setUp() {
    scd2MergeDao = new SCD2MergeDao(mockSwiftLakeEngine);
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
  }

  @Test
  void testVerifyNoOutOfOrderRecords() {
    SCD2MergeProperties properties = new SCD2MergeProperties();
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectOne("SCD2Merge.verifyNoOutOfOrderRecords", properties))
        .thenReturn(true);

    boolean result = scd2MergeDao.verifyNoOutOfOrderRecords(properties);

    assertThat(result).isTrue();
    verify(mockSqlSession).close();
  }

  @Test
  void testChangesBasedSCD2MergeFindDiffs() {
    SCD2MergeProperties properties = new SCD2MergeProperties();
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    scd2MergeDao.changesBasedSCD2MergeFindDiffs(properties);

    verify(mockSqlSession).update("SCD2Merge.changesBasedSCD2MergeFindDiffs", properties);
    verify(mockSqlSession).close();
  }

  @Test
  void testGetChangesBasedSCD2MergeUpsertsSql() {
    SCD2MergeProperties properties = new SCD2MergeProperties();

    String expectedSql = "testGetChangesBasedSCD2MergeUpsertsSql...";
    when(mockSqlSessionFactory.getSql("SCD2Merge.changesBasedSCD2MergeUpserts", properties))
        .thenReturn(expectedSql);

    String result = scd2MergeDao.getChangesBasedSCD2MergeUpsertsSql(properties);

    assertThat(result).isEqualTo(expectedSql);
  }

  @Test
  void testSnapshotBasedSCD2MergeFindDiffs() {
    SCD2MergeProperties properties = new SCD2MergeProperties();
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    scd2MergeDao.snapshotBasedSCD2MergeFindDiffs(properties);

    verify(mockSqlSession).update("SCD2Merge.snapshotBasedSCD2MergeFindDiffs", properties);
    verify(mockSqlSession).close();
  }

  @Test
  void testGetSnapshotBasedSCD2MergeUpsertsSql() {
    SCD2MergeProperties properties = new SCD2MergeProperties();

    String expectedSql = "testGetSnapshotBasedSCD2MergeUpsertsSql...";
    when(mockSqlSessionFactory.getSql("SCD2Merge.snapshotBasedSCD2MergeUpserts", properties))
        .thenReturn(expectedSql);

    String result = scd2MergeDao.getSnapshotBasedSCD2MergeUpsertsSql(properties);

    assertThat(result).isEqualTo(expectedSql);
  }

  @Test
  void testGetSnapshotBasedSCD2MergeAppendOnlySql() {
    SCD2MergeProperties properties = new SCD2MergeProperties();
    String expectedSql = "testGetSnapshotBasedSCD2MergeAppendOnlySql...";
    when(mockSqlSessionFactory.getSql("SCD2Merge.snapshotBasedSCD2MergeAppendOnly", properties))
        .thenReturn(expectedSql);

    String result = scd2MergeDao.getSnapshotBasedSCD2MergeAppendOnlySql(properties);

    assertThat(result).isEqualTo(expectedSql);
  }

  @Test
  void testGetFileNames() {
    String dataPath = "/data/path";
    List<String> expectedFileNames = Arrays.asList("file1.parquet", "file2.parquet");
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList("SCD2Merge.getFileNames", dataPath))
        .thenReturn((List) expectedFileNames);

    List<String> result = scd2MergeDao.getFileNames(dataPath);

    assertThat(result).isEqualTo(expectedFileNames);
    verify(mockSqlSession).close();
  }

  @Test
  void testSaveDistinctFileNamesForSnapshotMerge() {
    SCD2MergeProperties properties = new SCD2MergeProperties();
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    scd2MergeDao.saveDistinctFileNamesForSnapshotMerge(properties);

    verify(mockSqlSession).update("SCD2Merge.saveDistinctFileNamesForSnapshotMerge", properties);
    verify(mockSqlSession).close();
  }

  @Test
  void testSaveDistinctFileNamesForChangesMerge() {
    SCD2MergeProperties properties = new SCD2MergeProperties();
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    scd2MergeDao.saveDistinctFileNamesForChangesMerge(properties);

    verify(mockSqlSession).update("SCD2Merge.saveDistinctFileNamesForChangesMerge", properties);
    verify(mockSqlSession).close();
  }

  @Test
  void testVerifyNoOutOfOrderRecordsWithException() {
    SCD2MergeProperties properties = new SCD2MergeProperties();
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectOne(anyString(), any()))
        .thenThrow(new RuntimeException("Database error"));

    assertThatThrownBy(() -> scd2MergeDao.verifyNoOutOfOrderRecords(properties))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Database error");

    verify(mockSqlSession).close();
  }

  @Test
  void testChangesBasedSCD2MergeFindDiffsWithException() {
    SCD2MergeProperties properties = new SCD2MergeProperties();
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    doThrow(new RuntimeException("Merge error")).when(mockSqlSession).update(anyString(), any());

    assertThatThrownBy(() -> scd2MergeDao.changesBasedSCD2MergeFindDiffs(properties))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Merge error");

    verify(mockSqlSession).close();
  }

  @Test
  void testGetChangesBasedSCD2MergeUpsertsSqlWithException() {
    SCD2MergeProperties properties = new SCD2MergeProperties();

    when(mockSqlSessionFactory.getSql(anyString(), any()))
        .thenThrow(new RuntimeException("SQL error"));

    assertThatThrownBy(() -> scd2MergeDao.getChangesBasedSCD2MergeUpsertsSql(properties))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("SQL error");
  }

  @Test
  void testGetFileNamesWithException() {
    String dataPath = "/data/path";
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);
    when(mockSqlSession.selectList(anyString(), any()))
        .thenThrow(new RuntimeException("File error"));

    assertThatThrownBy(() -> scd2MergeDao.getFileNames(dataPath))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("File error");

    verify(mockSqlSession).close();
  }
}
