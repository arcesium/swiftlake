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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBPreparedStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SwiftLakeConnectionTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private DuckDBConnection mockDuckDBConnection;

  private SwiftLakeConnection swiftLakeConnection;

  @BeforeEach
  void setUp() {
    swiftLakeConnection = new SwiftLakeConnection(mockSwiftLakeEngine, mockDuckDBConnection, true);
  }

  @Test
  void testPrepareStatement() throws SQLException {
    String sql = "SELECT * FROM table";

    PreparedStatement result = swiftLakeConnection.prepareStatement(sql);

    assertThat(result).isInstanceOf(SwiftLakePreparedStatement.class);
  }

  @Test
  void testPrepareStatementWithQueryTransformer() throws SQLException {
    String sql = "SELECT * FROM table";
    QueryTransformer transformer = s -> s + " WHERE id = 1";

    PreparedStatement result = swiftLakeConnection.prepareStatement(sql, transformer);

    assertThat(result).isInstanceOf(SwiftLakePreparedStatement.class);
  }

  @Test
  void testCreateStatement() throws SQLException {
    when(mockDuckDBConnection.createStatement()).thenReturn(mock(DuckDBPreparedStatement.class));

    Statement result = swiftLakeConnection.createStatement();

    assertThat(result).isInstanceOf(SwiftLakePreparedStatement.class);
  }

  @Test
  void testClose() throws SQLException {
    swiftLakeConnection.close();

    verify(mockSwiftLakeEngine).closeDuckDBConnection(mockDuckDBConnection);
  }

  @Test
  void testIsClosed() throws SQLException {
    when(mockDuckDBConnection.isClosed()).thenReturn(true);

    boolean result = swiftLakeConnection.isClosed();

    assertThat(result).isTrue();
  }

  @Test
  void testBeginAndEndRequest() throws SQLException {
    swiftLakeConnection.beginRequest();
    swiftLakeConnection.endRequest();

    // Verify that temporary objects are dropped
    verify(mockDuckDBConnection).createStatement();
  }

  @Test
  void testGetMetaData() throws SQLException {
    DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
    when(mockDuckDBConnection.getMetaData()).thenReturn(mockMetaData);

    DatabaseMetaData result = swiftLakeConnection.getMetaData();

    assertThat(result).isEqualTo(mockMetaData);
  }

  @Test
  void testSetAutoCommit() throws SQLException {
    swiftLakeConnection.setAutoCommit(true);

    verify(mockDuckDBConnection).setAutoCommit(true);
  }

  @Test
  void testGetAutoCommit() throws SQLException {
    when(mockDuckDBConnection.getAutoCommit()).thenReturn(true);

    boolean result = swiftLakeConnection.getAutoCommit();

    assertThat(result).isTrue();
  }

  @Test
  void testCommit() throws SQLException {
    swiftLakeConnection.commit();

    verify(mockDuckDBConnection).commit();
  }

  @Test
  void testRollback() throws SQLException {
    swiftLakeConnection.rollback();

    verify(mockDuckDBConnection).rollback();
  }

  @Test
  void testSetReadOnly() throws SQLException {
    swiftLakeConnection.setReadOnly(true);

    verify(mockDuckDBConnection).setReadOnly(true);
  }

  @Test
  void testIsReadOnly() throws SQLException {
    when(mockDuckDBConnection.isReadOnly()).thenReturn(true);

    boolean result = swiftLakeConnection.isReadOnly();

    assertThat(result).isTrue();
  }

  @Test
  void testSetTransactionIsolation() throws SQLException {
    swiftLakeConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    verify(mockDuckDBConnection).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

  @Test
  void testGetTransactionIsolation() throws SQLException {
    when(mockDuckDBConnection.getTransactionIsolation())
        .thenReturn(Connection.TRANSACTION_READ_COMMITTED);

    int result = swiftLakeConnection.getTransactionIsolation();

    assertThat(result).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
  }

  @Test
  void testSetSchema() throws SQLException {
    swiftLakeConnection.setSchema("test_schema");

    verify(mockDuckDBConnection).setSchema("test_schema");
  }

  @Test
  void testGetSchema() throws SQLException {
    when(mockDuckDBConnection.getSchema()).thenReturn("test_schema");

    String result = swiftLakeConnection.getSchema();

    assertThat(result).isEqualTo("test_schema");
  }

  @Test
  void testAbort() throws SQLException {
    Executor executor = Runnable::run;
    swiftLakeConnection.abort(executor);

    verify(mockDuckDBConnection).abort(executor);
  }

  @Test
  void testSetNetworkTimeout() throws SQLException {
    Executor executor = Runnable::run;
    swiftLakeConnection.setNetworkTimeout(executor, 1000);

    verify(mockDuckDBConnection).setNetworkTimeout(executor, 1000);
  }

  @Test
  void testGetNetworkTimeout() throws SQLException {
    when(mockDuckDBConnection.getNetworkTimeout()).thenReturn(1000);

    int result = swiftLakeConnection.getNetworkTimeout();

    assertThat(result).isEqualTo(1000);
  }

  @Test
  void testIsValid() throws SQLException {
    when(mockDuckDBConnection.isValid(1)).thenReturn(true);

    boolean result = swiftLakeConnection.isValid(1);

    assertThat(result).isTrue();
  }

  @Test
  void testSetClientInfo() throws SQLClientInfoException {
    Properties properties = new Properties();
    properties.setProperty("name", "value");

    swiftLakeConnection.setClientInfo(properties);

    verify(mockDuckDBConnection).setClientInfo(properties);
  }

  @Test
  void testGetClientInfo() throws SQLException {
    Properties properties = new Properties();
    when(mockDuckDBConnection.getClientInfo()).thenReturn(properties);

    Properties result = swiftLakeConnection.getClientInfo();

    assertThat(result).isEqualTo(properties);
  }
}
