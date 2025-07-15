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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BaseDaoTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private SwiftLakeSqlSessionFactory mockSqlSessionFactory;

  @Mock private SqlSession mockSqlSession;

  @Mock private ResultSet mockResultSet;

  @Mock private ResultSetMetaData mockResultSetMetaData;

  private TestBaseDao testBaseDao;

  private static class TestBaseDao extends BaseDao {
    public TestBaseDao(SwiftLakeEngine swiftLakeEngine) {
      super(swiftLakeEngine);
    }

    public SqlSession getSessionPublic() {
      return super.getSession();
    }

    public String getSqlPublic(String id, Object params) {
      return super.getSql(id, params);
    }

    public List<Map<String, Object>> getDataInMapPublic(ResultSet resultSet) {
      return super.getDataInMap(resultSet);
    }
  }

  @BeforeEach
  void setUp() {
    testBaseDao = new TestBaseDao(mockSwiftLakeEngine);
  }

  @Test
  void testConstructor() {
    assertThat(testBaseDao).isNotNull();
  }

  @Test
  void testGetSession() {
    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
    when(mockSqlSessionFactory.openSession()).thenReturn(mockSqlSession);

    SqlSession result = testBaseDao.getSessionPublic();

    assertThat(result).isEqualTo(mockSqlSession);
    verify(mockSwiftLakeEngine).getInternalSqlSessionFactory();
    verify(mockSqlSessionFactory).openSession();
  }

  @Test
  void testGetSql() {
    String id = "testId";
    Object params = new Object();
    String expectedSql = "SELECT * FROM table";

    when(mockSwiftLakeEngine.getInternalSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
    when(mockSqlSessionFactory.getSql(id, params)).thenReturn(expectedSql);

    String result = testBaseDao.getSqlPublic(id, params);

    assertThat(result).isEqualTo(expectedSql);
    verify(mockSwiftLakeEngine).getInternalSqlSessionFactory();
    verify(mockSqlSessionFactory).getSql(id, params);
  }

  @Test
  void testGetDataInMap() throws SQLException {
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(2);
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("id");
    when(mockResultSetMetaData.getColumnName(2)).thenReturn("name");
    when(mockResultSet.getObject("id")).thenReturn(1, 2);
    when(mockResultSet.getObject("name")).thenReturn("John", "Jane");

    List<Map<String, Object>> result = testBaseDao.getDataInMapPublic(mockResultSet);

    assertThat(result).hasSize(2);
    assertThat(result.get(0)).containsEntry("id", 1).containsEntry("name", "John");
    assertThat(result.get(1)).containsEntry("id", 2).containsEntry("name", "Jane");
  }

  @Test
  void testGetDataInMapWithEmptyResultSet() throws SQLException {
    when(mockResultSet.next()).thenReturn(false);

    List<Map<String, Object>> result = testBaseDao.getDataInMapPublic(mockResultSet);

    assertThat(result).isEmpty();
  }

  @Test
  void testGetDataInMapWithSQLException() throws SQLException {
    when(mockResultSet.next()).thenThrow(new SQLException("Test exception"));

    assertThatThrownBy(() -> testBaseDao.getDataInMapPublic(mockResultSet))
        .isInstanceOf(SwiftLakeException.class)
        .hasMessageContaining("An error occurred while reading data from ResultSet")
        .hasCauseInstanceOf(SQLException.class);
  }

  @Test
  void testGetDataInMapWithNullValues() throws SQLException {
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(2);
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("id");
    when(mockResultSetMetaData.getColumnName(2)).thenReturn("name");
    when(mockResultSet.getObject("id")).thenReturn(null);
    when(mockResultSet.getObject("name")).thenReturn(null);

    List<Map<String, Object>> result = testBaseDao.getDataInMapPublic(mockResultSet);

    assertThat(result).hasSize(1);
    assertThat(result.get(0)).containsEntry("id", null).containsEntry("name", null);
  }
}
