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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Timer;
import org.apache.commons.lang3.tuple.Pair;
import org.duckdb.DuckDBPreparedStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SwiftLakePreparedStatementTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private SwiftLakeConnection mockConnection;

  @Mock private DuckDBPreparedStatement mockDuckDBStatement;

  @Mock private SqlQueryProcessor mockSqlQueryProcessor;
  @Mock private IcebergScanExecutor mockIcebergScanExecutor;

  private SwiftLakePreparedStatement statement;
  private SwiftLakePreparedStatement preparedStatement;

  @BeforeEach
  void setUp() throws SQLException {
    when(mockSwiftLakeEngine.getSqlQueryProcessor()).thenReturn(mockSqlQueryProcessor);
    statement =
        new SwiftLakePreparedStatement(
            mockSwiftLakeEngine, mockConnection, mockDuckDBStatement, true);
    preparedStatement =
        new SwiftLakePreparedStatement(
            mockSwiftLakeEngine, mockConnection, "?", true, (sql) -> mockDuckDBStatement);
  }

  @Test
  void testExecuteQuery() throws SQLException {
    String sql = "SELECT * FROM table";
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockSqlQueryProcessor.process(anyString(), any())).thenReturn(Pair.of(sql, null));
    when(mockDuckDBStatement.executeQuery(sql)).thenReturn(mockResultSet);

    ResultSet result = statement.executeQuery(sql);

    assertThat(result).isEqualTo(mockResultSet);
    verify(mockDuckDBStatement).executeQuery(sql);
  }

  @Test
  void testExecuteUpdate() throws SQLException {
    String sql = "SELECT * FROM table";
    when(mockSqlQueryProcessor.process(anyString(), any())).thenReturn(Pair.of(sql, null));
    when(mockDuckDBStatement.executeUpdate(sql)).thenReturn(1);

    int result = statement.executeUpdate(sql);

    assertThat(result).isEqualTo(1);
    verify(mockDuckDBStatement).executeUpdate(sql);
  }

  @Test
  void testExecute() throws SQLException {
    String sql = "SELECT * FROM table";

    when(mockSqlQueryProcessor.process(anyString(), any())).thenReturn(Pair.of(sql, null));
    when(mockDuckDBStatement.execute(sql)).thenReturn(true);

    boolean result = statement.execute(sql);

    assertThat(result).isTrue();
    verify(mockDuckDBStatement).execute(sql);
  }

  @Test
  void testSetParameter() throws SQLException {
    statement.setInt(1, 100);
    statement.setString(2, "test");

    verify(mockDuckDBStatement).setInt(1, 100);
    verify(mockDuckDBStatement).setString(2, "test");
  }

  @Test
  void testClearParameters() throws SQLException {
    statement.clearParameters();

    verify(mockDuckDBStatement).clearParameters();
  }

  @Test
  void testClose() throws SQLException {
    statement.close();

    verify(mockDuckDBStatement).close();
  }

  @Test
  void testSetQueryTimeout() throws SQLException {
    statement.setQueryTimeout(30);

    assertThat(statement.getQueryTimeout()).isEqualTo(30);
  }

  @Test
  void testSetMaxRows() throws SQLException {
    statement.setMaxRows(100);

    verify(mockDuckDBStatement).setMaxRows(100);
  }

  @Test
  void testGetMetaData() throws SQLException {
    ResultSetMetaData mockMetaData = mock(ResultSetMetaData.class);
    when(mockDuckDBStatement.getMetaData()).thenReturn(mockMetaData);

    ResultSetMetaData result = statement.getMetaData();

    assertThat(result).isEqualTo(mockMetaData);
  }

  @Test
  void testExecuteQueryWithTableFilters() throws SQLException {
    String sql = "SELECT * FROM table";
    List<TableFilter> tableFilters = new ArrayList<>();
    tableFilters.add(new TableFilter("table", null, "placeholder"));
    when(mockSwiftLakeEngine.getIcebergScanExecutor()).thenReturn(mockIcebergScanExecutor);
    when(mockIcebergScanExecutor.executeTableScansAndUpdateSql(any(), any(), any(), anyBoolean()))
        .thenReturn(Pair.of(sql, List.of()));
    when(mockSqlQueryProcessor.process(sql, mockConnection))
        .thenReturn(org.apache.commons.lang3.tuple.Pair.of(sql, tableFilters));

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockDuckDBStatement.executeQuery(anyString())).thenReturn(mockResultSet);

    ResultSet result = statement.executeQuery(sql);

    assertThat(result).isEqualTo(mockResultSet);
    verify(mockSqlQueryProcessor).process(sql, mockConnection);
    verify(mockDuckDBStatement).executeQuery(anyString());
  }

  @Test
  void testExecuteWithInvalidSyntax() {
    String invalidSql = "SELECT * FORM table"; // Intentional typo

    when(mockSqlQueryProcessor.process(invalidSql, mockConnection))
        .thenThrow(new SwiftLakeException(new RuntimeException(), "Invalid SQL syntax"));

    assertThatThrownBy(() -> statement.execute(invalidSql))
        .isInstanceOf(SwiftLakeException.class)
        .hasMessageContaining("Invalid SQL syntax");
  }

  @Test
  void testSetParameterWithUnsupportedType() {
    assertThatThrownBy(() -> statement.setRowId(1, null))
        .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  void testBatchOperationsNotSupported() {
    assertThatThrownBy(() -> statement.addBatch())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Batch mode is not supported");

    assertThatThrownBy(() -> statement.executeBatch())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Batch mode is not supported");
  }

  @Test
  void testGetConnection() throws SQLException {
    Connection result = statement.getConnection();

    assertThat(result).isEqualTo(mockConnection);
  }

  @Test
  void testIsClosed() throws SQLException {
    when(mockDuckDBStatement.isClosed()).thenReturn(false);
    assertThat(statement.isClosed()).isFalse();

    when(mockDuckDBStatement.isClosed()).thenReturn(true);
    assertThat(statement.isClosed()).isTrue();
  }

  @Test
  void testSetRowId() {
    assertThatThrownBy(() -> preparedStatement.setRowId(1, null))
        .isInstanceOf(SQLFeatureNotSupportedException.class)
        .hasMessage("setRowId");
  }

  @Test
  void testSetNCharacterStream() {
    assertThatThrownBy(() -> preparedStatement.setNCharacterStream(1, null, 0))
        .isInstanceOf(SQLFeatureNotSupportedException.class)
        .hasMessage("setNCharacterStream");
  }

  @Test
  void testSetNClob() {
    assertThatThrownBy(() -> preparedStatement.setNClob(1, (NClob) null))
        .isInstanceOf(SQLFeatureNotSupportedException.class)
        .hasMessage("setNClob");
  }

  @Test
  void testSetClob() {
    assertThatThrownBy(() -> preparedStatement.setClob(1, (Clob) null))
        .isInstanceOf(SQLFeatureNotSupportedException.class)
        .hasMessage("setClob");
  }

  @Test
  void testSetBlob() {
    assertThatThrownBy(() -> preparedStatement.setBlob(1, (Blob) null))
        .isInstanceOf(SQLFeatureNotSupportedException.class)
        .hasMessage("setBlob");
  }

  @Test
  void testSetSQLXML() {
    assertThatThrownBy(() -> preparedStatement.setSQLXML(1, null))
        .isInstanceOf(SQLFeatureNotSupportedException.class)
        .hasMessage("setSQLXML");
  }

  @Test
  void testSetRef() {
    assertThatThrownBy(() -> preparedStatement.setRef(1, null))
        .isInstanceOf(SQLFeatureNotSupportedException.class)
        .hasMessage("setRef");
  }

  @Test
  void testSetAsciiStream() {
    assertThatThrownBy(() -> preparedStatement.setAsciiStream(1, null))
        .isInstanceOf(SQLFeatureNotSupportedException.class)
        .hasMessage("setAsciiStream");
  }

  @Test
  void testCloseOnCompletion() {
    assertThatThrownBy(() -> statement.closeOnCompletion())
        .isInstanceOf(SQLFeatureNotSupportedException.class)
        .hasMessage("closeOnCompletion");
  }

  @Test
  void testSetNull() throws SQLException {
    statement.setNull(1, Types.INTEGER);
    verify(mockDuckDBStatement).setNull(1, Types.INTEGER);
  }

  @Test
  void testSetBoolean() throws SQLException {
    statement.setBoolean(1, true);
    verify(mockDuckDBStatement).setBoolean(1, true);
  }

  @Test
  void testSetByte() throws SQLException {
    statement.setByte(1, (byte) 5);
    verify(mockDuckDBStatement).setByte(1, (byte) 5);
  }

  @Test
  void testSetShort() throws SQLException {
    statement.setShort(1, (short) 10);
    verify(mockDuckDBStatement).setShort(1, (short) 10);
  }

  @Test
  void testSetInt() throws SQLException {
    statement.setInt(1, 100);
    verify(mockDuckDBStatement).setInt(1, 100);
  }

  @Test
  void testSetLong() throws SQLException {
    statement.setLong(1, 1000L);
    verify(mockDuckDBStatement).setLong(1, 1000L);
  }

  @Test
  void testSetFloat() throws SQLException {
    statement.setFloat(1, 10.5f);
    verify(mockDuckDBStatement).setFloat(1, 10.5f);
  }

  @Test
  void testSetDouble() throws SQLException {
    statement.setDouble(1, 20.5);
    verify(mockDuckDBStatement).setDouble(1, 20.5);
  }

  @Test
  void testSetBigDecimal() throws SQLException {
    BigDecimal bd = new BigDecimal("100.50");
    statement.setBigDecimal(1, bd);
    verify(mockDuckDBStatement).setBigDecimal(1, bd);
  }

  @Test
  void testSetString() throws SQLException {
    statement.setString(1, "test");
    verify(mockDuckDBStatement).setString(1, "test");
  }

  @Test
  void testSetBytes() throws SQLException {
    byte[] bytes = {1, 2, 3};
    statement.setBytes(1, bytes);
    verify(mockDuckDBStatement).setBytes(1, bytes);
  }

  @Test
  void testSetDate() throws SQLException {
    Date date = new Date(System.currentTimeMillis());
    statement.setDate(1, date);
    verify(mockDuckDBStatement).setDate(1, date);
  }

  @Test
  void testSetTime() throws SQLException {
    Time time = new Time(System.currentTimeMillis());
    statement.setTime(1, time);
    verify(mockDuckDBStatement).setTime(1, time);
  }

  @Test
  void testSetTimestamp() throws SQLException {
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    statement.setTimestamp(1, timestamp);
    verify(mockDuckDBStatement).setTimestamp(1, timestamp);
  }

  @Test
  void testSetUnicodeStream() throws SQLException {
    InputStream is = mock(InputStream.class);
    statement.setUnicodeStream(1, is, 10);
    verify(mockDuckDBStatement).setUnicodeStream(1, is, 10);
  }

  @Test
  void testSetBinaryStream() throws SQLException {
    InputStream is = mock(InputStream.class);
    statement.setBinaryStream(1, is, 10);
    verify(mockDuckDBStatement).setBinaryStream(1, is, 10);
  }

  @Test
  void testSetObject() throws SQLException {
    Object obj = new Object();
    statement.setObject(1, obj);
    verify(mockDuckDBStatement).setObject(1, obj);
  }

  @Test
  void testSetObjectWithTargetType() throws SQLException {
    Object obj = new Object();
    statement.setObject(1, obj, Types.VARCHAR);
    verify(mockDuckDBStatement).setObject(1, obj, Types.VARCHAR);
  }

  @Test
  void testSetCharacterStream() throws SQLException {
    Reader reader = mock(Reader.class);
    statement.setCharacterStream(1, reader, 10);
    verify(mockDuckDBStatement).setCharacterStream(1, reader, 10);
  }

  @Test
  void testSetArray() throws SQLException {
    Array array = mock(Array.class);
    statement.setArray(1, array);
    verify(mockDuckDBStatement).setArray(1, array);
  }

  @Test
  void testSetURL() throws Exception {
    URL url = new URL("http://example.com");
    statement.setURL(1, url);
    verify(mockDuckDBStatement).setURL(1, url);
  }

  @Test
  void testGetParameterMetaData() throws SQLException {
    ParameterMetaData metaData = mock(ParameterMetaData.class);
    when(mockDuckDBStatement.getParameterMetaData()).thenReturn(metaData);
    assertThat(statement.getParameterMetaData()).isEqualTo(metaData);
  }

  @Test
  void testSetNString() throws SQLException {
    statement.setNString(1, "test");
    verify(mockDuckDBStatement).setNString(1, "test");
  }

  @Test
  void testSetObjectWithTypeAndScaleOrLength() throws SQLException {
    Object obj = new Object();
    statement.setObject(1, obj, Types.NUMERIC, 2);
    verify(mockDuckDBStatement).setObject(1, obj, Types.NUMERIC, 2);
  }

  @Test
  void testExecuteQueryNoParameters() throws SQLException {
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockDuckDBStatement.executeQuery()).thenReturn(mockResultSet);
    assertThat(statement.executeQuery()).isEqualTo(mockResultSet);
  }

  @Test
  void testExecuteUpdateNoParameters() throws SQLException {
    when(mockDuckDBStatement.executeUpdate()).thenReturn(1);
    assertThat(statement.executeUpdate()).isEqualTo(1);
  }

  @Test
  void testExecuteNoParameters() throws SQLException {
    when(mockDuckDBStatement.execute()).thenReturn(true);
    assertThat(statement.execute()).isTrue();
  }

  @Test
  void testGetMaxFieldSize() throws SQLException {
    when(mockDuckDBStatement.getMaxFieldSize()).thenReturn(1000);
    assertThat(statement.getMaxFieldSize()).isEqualTo(1000);
  }

  @Test
  void testSetMaxFieldSize() throws SQLException {
    statement.setMaxFieldSize(2000);
    verify(mockDuckDBStatement).setMaxFieldSize(2000);
  }

  @Test
  void testGetMaxRows() throws SQLException {
    when(mockDuckDBStatement.getMaxRows()).thenReturn(100);
    assertThat(statement.getMaxRows()).isEqualTo(100);
  }

  @Test
  void testSetEscapeProcessing() throws SQLException {
    statement.setEscapeProcessing(true);
    verify(mockDuckDBStatement).setEscapeProcessing(true);
  }

  @Test
  void testGetQueryTimeout() throws SQLException {
    statement.setQueryTimeout(30);
    assertThat(statement.getQueryTimeout()).isEqualTo(30);
  }

  @Test
  void testCancel() throws SQLException {
    statement.cancel();
    verify(mockDuckDBStatement).cancel();
  }

  @Test
  void testGetWarnings() throws SQLException {
    SQLWarning warning = new SQLWarning("Test warning");
    when(mockDuckDBStatement.getWarnings()).thenReturn(warning);
    assertThat((Iterable<? extends Throwable>) statement.getWarnings()).isEqualTo(warning);
  }

  @Test
  void testClearWarnings() throws SQLException {
    statement.clearWarnings();
    verify(mockDuckDBStatement).clearWarnings();
  }

  @Test
  void testSetCursorName() throws SQLException {
    statement.setCursorName("cursor1");
    verify(mockDuckDBStatement).setCursorName("cursor1");
  }

  @Test
  void testGetResultSet() throws SQLException {
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockDuckDBStatement.getResultSet()).thenReturn(mockResultSet);
    assertThat(statement.getResultSet()).isEqualTo(mockResultSet);
  }

  @Test
  void testGetUpdateCount() throws SQLException {
    when(mockDuckDBStatement.getUpdateCount()).thenReturn(5);
    assertThat(statement.getUpdateCount()).isEqualTo(5);
  }

  @Test
  void testGetMoreResults() throws SQLException {
    when(mockDuckDBStatement.getMoreResults()).thenReturn(true);
    assertThat(statement.getMoreResults()).isTrue();
  }

  @Test
  void testSetFetchDirection() throws SQLException {
    statement.setFetchDirection(ResultSet.FETCH_FORWARD);
    verify(mockDuckDBStatement).setFetchDirection(ResultSet.FETCH_FORWARD);
  }

  @Test
  void testGetFetchDirection() throws SQLException {
    when(mockDuckDBStatement.getFetchDirection()).thenReturn(ResultSet.FETCH_FORWARD);
    assertThat(statement.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
  }

  @Test
  void testSetFetchSize() throws SQLException {
    statement.setFetchSize(100);
    verify(mockDuckDBStatement).setFetchSize(100);
  }

  @Test
  void testGetFetchSize() throws SQLException {
    when(mockDuckDBStatement.getFetchSize()).thenReturn(100);
    assertThat(statement.getFetchSize()).isEqualTo(100);
  }

  @Test
  void testGetResultSetConcurrency() throws SQLException {
    when(mockDuckDBStatement.getResultSetConcurrency()).thenReturn(ResultSet.CONCUR_READ_ONLY);
    assertThat(statement.getResultSetConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
  }

  @Test
  void testGetResultSetType() throws SQLException {
    when(mockDuckDBStatement.getResultSetType()).thenReturn(ResultSet.TYPE_FORWARD_ONLY);
    assertThat(statement.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
  }

  @Test
  void testGetMoreResultsWithCurrent() throws SQLException {
    when(mockDuckDBStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT)).thenReturn(true);
    assertThat(statement.getMoreResults(Statement.KEEP_CURRENT_RESULT)).isTrue();
  }

  @Test
  void testGetGeneratedKeys() throws SQLException {
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockDuckDBStatement.getGeneratedKeys()).thenReturn(mockResultSet);
    assertThat(statement.getGeneratedKeys()).isEqualTo(mockResultSet);
  }

  @Test
  void testExecuteUpdateWithAutoGeneratedKeys() throws SQLException {
    when(mockDuckDBStatement.executeUpdate("SQL", Statement.RETURN_GENERATED_KEYS)).thenReturn(1);
    assertThat(statement.executeUpdate("SQL", Statement.RETURN_GENERATED_KEYS)).isEqualTo(1);
  }

  @Test
  void testExecuteUpdateWithColumnIndexes() throws SQLException {
    int[] columnIndexes = {1, 2, 3};
    when(mockDuckDBStatement.executeUpdate("SQL", columnIndexes)).thenReturn(1);
    assertThat(statement.executeUpdate("SQL", columnIndexes)).isEqualTo(1);
  }

  @Test
  void testExecuteUpdateWithColumnNames() throws SQLException {
    String[] columnNames = {"col1", "col2", "col3"};
    when(mockDuckDBStatement.executeUpdate("SQL", columnNames)).thenReturn(1);
    assertThat(statement.executeUpdate("SQL", columnNames)).isEqualTo(1);
  }

  @Test
  void testExecuteWithAutoGeneratedKeys() throws SQLException {
    when(mockDuckDBStatement.execute("SQL", Statement.RETURN_GENERATED_KEYS)).thenReturn(true);
    assertThat(statement.execute("SQL", Statement.RETURN_GENERATED_KEYS)).isTrue();
  }

  @Test
  void testExecuteWithColumnIndexes() throws SQLException {
    int[] columnIndexes = {1, 2, 3};
    when(mockDuckDBStatement.execute("SQL", columnIndexes)).thenReturn(true);
    assertThat(statement.execute("SQL", columnIndexes)).isTrue();
  }

  @Test
  void testExecuteWithColumnNames() throws SQLException {
    String[] columnNames = {"col1", "col2", "col3"};
    when(mockDuckDBStatement.execute("SQL", columnNames)).thenReturn(true);
    assertThat(statement.execute("SQL", columnNames)).isTrue();
  }

  @Test
  void testGetResultSetHoldability() throws SQLException {
    when(mockDuckDBStatement.getResultSetHoldability())
        .thenReturn(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    assertThat(statement.getResultSetHoldability()).isEqualTo(ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  @Test
  void testSetPoolable() throws SQLException {
    statement.setPoolable(true);
    verify(mockDuckDBStatement).setPoolable(true);
  }

  @Test
  void testIsPoolable() throws SQLException {
    when(mockDuckDBStatement.isPoolable()).thenReturn(true);
    assertThat(statement.isPoolable()).isTrue();
  }

  @Test
  void testIsCloseOnCompletion() throws SQLException {
    when(mockDuckDBStatement.isCloseOnCompletion()).thenReturn(true);
    assertThat(statement.isCloseOnCompletion()).isTrue();
  }

  @Test
  void testUnwrap() throws SQLException {
    when(mockDuckDBStatement.unwrap(Statement.class)).thenReturn(mockDuckDBStatement);
    assertThat(statement.unwrap(Statement.class)).isEqualTo(mockDuckDBStatement);
  }

  @Test
  void testIsWrapperFor() throws SQLException {
    when(mockDuckDBStatement.isWrapperFor(Statement.class)).thenReturn(true);
    assertThat(statement.isWrapperFor(Statement.class)).isTrue();
  }

  @Test
  void testSetNullWithTypeName() throws SQLException {
    statement.setNull(1, Types.OTHER, "CustomType");
    verify(mockDuckDBStatement).setNull(1, Types.OTHER, "CustomType");
  }

  @Test
  void testSetDateWithCalendar() {
    assertThatThrownBy(() -> statement.setDate(1, null, Calendar.getInstance()))
        .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  void testSetTimeWithCalendar() {
    assertThatThrownBy(() -> statement.setTime(1, null, Calendar.getInstance()))
        .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  void testSetTimestampWithCalendar() {
    assertThatThrownBy(() -> statement.setTimestamp(1, null, Calendar.getInstance()))
        .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  void testSetClobWithReader() {
    assertThatThrownBy(() -> preparedStatement.setClob(1, (Reader) null))
        .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  void testSetBlobWithInputStream() {
    assertThatThrownBy(() -> preparedStatement.setBlob(1, (InputStream) null))
        .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  void testSetNClobWithReader() {
    assertThatThrownBy(() -> preparedStatement.setNClob(1, (Reader) null))
        .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  void testQueryTimeoutExceeded() throws SQLException {
    Timer timer = new Timer("test", true);
    when(mockSwiftLakeEngine.getTimer()).thenReturn(timer);
    statement.setQueryTimeout(1);
    when(mockSqlQueryProcessor.process(anyString(), any()))
        .thenAnswer(
            invocation -> {
              Thread.sleep(2000); // Sleep for 2 seconds
              return Pair.of("", null);
            });

    assertThatThrownBy(() -> statement.executeQuery("test"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Query timed out");
  }

  @Test
  void testExecuteQueryWithTableFiltersAndQueryTransformer() throws SQLException {
    String sql = "SELECT * FROM table";
    QueryTransformer mockTransformer = mock(QueryTransformer.class);
    when(mockTransformer.transform(anyString())).thenReturn("TRANSFORMED SQL");

    List<TableFilter> tableFilters = new ArrayList<>();
    tableFilters.add(new TableFilter("table", null, "placeholder"));
    when(mockSwiftLakeEngine.getIcebergScanExecutor()).thenReturn(mockIcebergScanExecutor);
    when(mockIcebergScanExecutor.executeTableScansAndUpdateSql(any(), any(), any(), anyBoolean()))
        .thenReturn(Pair.of(sql, List.of()));
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockDuckDBStatement.executeQuery(anyString())).thenReturn(mockResultSet);

    ResultSet result = statement.executeQuery(sql, tableFilters, mockTransformer);

    assertThat(result).isEqualTo(mockResultSet);
    verify(mockTransformer).transform(anyString());
    verify(mockDuckDBStatement).executeQuery("TRANSFORMED SQL");
  }

  @Test
  void testExecuteUpdateWithTableFiltersAndQueryTransformer() throws SQLException {
    String sql = "SELECT * FROM table";
    QueryTransformer mockTransformer = mock(QueryTransformer.class);
    when(mockTransformer.transform(anyString())).thenReturn("TRANSFORMED SQL");

    List<TableFilter> tableFilters = new ArrayList<>();
    tableFilters.add(new TableFilter("table", null, "placeholder"));
    when(mockSwiftLakeEngine.getIcebergScanExecutor()).thenReturn(mockIcebergScanExecutor);
    when(mockIcebergScanExecutor.executeTableScansAndUpdateSql(any(), any(), any(), anyBoolean()))
        .thenReturn(Pair.of(sql, List.of()));
    when(mockDuckDBStatement.executeUpdate(anyString())).thenReturn(1);

    int result = statement.executeUpdate(sql, tableFilters, mockTransformer);

    assertThat(result).isEqualTo(1);
    verify(mockTransformer).transform(anyString());
    verify(mockDuckDBStatement).executeUpdate("TRANSFORMED SQL");
  }

  @Test
  void testExecuteWithTableFiltersAndQueryTransformer() throws SQLException {
    String sql = "INSERT INTO table VALUES (1, 'test')";
    QueryTransformer mockTransformer = mock(QueryTransformer.class);
    when(mockTransformer.transform(anyString())).thenReturn("TRANSFORMED SQL");

    List<TableFilter> tableFilters = new ArrayList<>();
    tableFilters.add(new TableFilter("table", null, "placeholder"));
    when(mockSwiftLakeEngine.getIcebergScanExecutor()).thenReturn(mockIcebergScanExecutor);
    when(mockIcebergScanExecutor.executeTableScansAndUpdateSql(any(), any(), any(), anyBoolean()))
        .thenReturn(Pair.of(sql, List.of()));
    when(mockDuckDBStatement.execute(anyString())).thenReturn(true);

    boolean result = statement.execute(sql, tableFilters, mockTransformer);

    assertThat(result).isTrue();
    verify(mockTransformer).transform(anyString());
    verify(mockDuckDBStatement).execute("TRANSFORMED SQL");
  }
}
