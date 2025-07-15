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

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.InputFiles;
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
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.duckdb.DuckDBPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of PreparedStatement for SwiftLake. */
public class SwiftLakePreparedStatement implements PreparedStatement {
  private static final Logger LOGGER = LoggerFactory.getLogger(SwiftLakePreparedStatement.class);
  private SwiftLakeEngine swiftLakeEngine;
  private SwiftLakeConnection connection;
  private DuckDBPreparedStatement statement;
  private boolean processTables;
  private SqlQueryProcessor sqlQueryProcessor;
  private List<InputFiles> inputFiles;
  private String preparedStmtSql;
  private Map<Integer, Object> params;
  private Map<Integer, Pair<Integer, Integer>> paramsTargetSqlTypes;
  private Function<String, DuckDBPreparedStatement> stmtFunc;
  private long queryTimeoutInMillis = 0;

  // This field is updated via reflection using TIMEOUT_TASK_UPDATER
  @SuppressWarnings("UnusedVariable")
  private volatile TimerTask queryTimeoutTask = null;

  private static final AtomicReferenceFieldUpdater<SwiftLakePreparedStatement, TimerTask>
      TIMEOUT_TASK_UPDATER =
          AtomicReferenceFieldUpdater.newUpdater(
              SwiftLakePreparedStatement.class, TimerTask.class, "queryTimeoutTask");
  private boolean isQueryCancelled;

  /**
   * Constructs a SwiftLakePreparedStatement with a pre-existing DuckDBPreparedStatement.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param connection The SwiftLakeConnection instance.
   * @param stmt The DuckDBPreparedStatement to be wrapped.
   * @param processTables Flag indicating whether to process tables.
   * @throws SQLException If any of the parameters are null.
   */
  public SwiftLakePreparedStatement(
      SwiftLakeEngine swiftLakeEngine,
      SwiftLakeConnection connection,
      DuckDBPreparedStatement stmt,
      boolean processTables)
      throws SQLException {
    // Null checks for parameters
    if (swiftLakeEngine == null) {
      throw new SQLException("swiftLakeEngine parameter cannot be null");
    }
    if (connection == null) {
      throw new SQLException("connection parameter cannot be null");
    }
    if (stmt == null) {
      throw new SQLException("statement parameter cannot be null");
    }
    // Initialize instance variables
    this.swiftLakeEngine = swiftLakeEngine;
    this.connection = connection;
    this.statement = stmt;
    this.processTables = processTables;
    this.sqlQueryProcessor = swiftLakeEngine.getSqlQueryProcessor();
    // Set timeout if configured in SwiftLakeEngine
    if (swiftLakeEngine.getQueryTimeoutInSeconds() != null) {
      this.queryTimeoutInMillis = swiftLakeEngine.getQueryTimeoutInSeconds() * 1000L;
    }
  }

  /**
   * Constructs a SwiftLakePreparedStatement with an SQL query string.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param connection The SwiftLakeConnection instance.
   * @param sql The SQL query string.
   * @param processTables Flag indicating whether to process tables.
   * @param stmtFunc Function to create a DuckDBPreparedStatement from SQL.
   * @throws SQLException If any of the parameters are null.
   */
  public SwiftLakePreparedStatement(
      SwiftLakeEngine swiftLakeEngine,
      SwiftLakeConnection connection,
      String sql,
      boolean processTables,
      Function<String, DuckDBPreparedStatement> stmtFunc)
      throws SQLException {
    // Null checks for parameters
    if (swiftLakeEngine == null) {
      throw new SQLException("swiftLakeEngine parameter cannot be null");
    }
    if (connection == null) {
      throw new SQLException("connection parameter cannot be null");
    }
    if (sql == null) {
      throw new SQLException("sql query parameter cannot be null");
    }
    if (stmtFunc == null) {
      throw new SQLException("stmtFunc parameter cannot be null");
    }
    // Initialize instance variables
    this.swiftLakeEngine = swiftLakeEngine;
    this.connection = connection;
    this.sqlQueryProcessor = swiftLakeEngine.getSqlQueryProcessor();
    this.processTables = processTables;

    // Set up statement based on processTables flag
    if (processTables) {
      this.preparedStmtSql = sql;
      this.stmtFunc = stmtFunc;
      this.params = new HashMap<>();
      this.paramsTargetSqlTypes = new HashMap<>();
    } else {
      this.statement = stmtFunc.apply(sql);
    }
    // Set timeout if configured in SwiftLakeEngine
    if (swiftLakeEngine.getQueryTimeoutInSeconds() != null) {
      this.queryTimeoutInMillis = swiftLakeEngine.getQueryTimeoutInSeconds() * 1000L;
    }
  }

  /**
   * Executes a query with table filters and query transformer.
   *
   * @param sql The SQL query to execute.
   * @param tableFilters List of table filters to apply.
   * @param queryTransformer Transformer to modify the query.
   * @return ResultSet containing the query results.
   * @throws SQLException If an error occurs during query execution.
   */
  public ResultSet executeQuery(
      String sql, List<TableFilter> tableFilters, QueryTransformer queryTransformer)
      throws SQLException {
    return executeQuery(sql, tableFilters, queryTransformer, true);
  }

  /**
   * Private method to execute a query with additional options.
   *
   * @param sql The SQL query to execute.
   * @param tableFilters List of table filters to apply.
   * @param queryTransformer Transformer to modify the query.
   * @param startTimer Flag to indicate whether to start the query timer.
   * @return ResultSet containing the query results.
   * @throws SQLException If an error occurs during query execution.
   */
  private ResultSet executeQuery(
      String sql,
      List<TableFilter> tableFilters,
      QueryTransformer queryTransformer,
      boolean startTimer)
      throws SQLException {
    try {
      // Start timer if requested
      if (startTimer) {
        startQueryTimeoutTask();
      }

      // Apply table filters if present
      if (tableFilters != null && !tableFilters.isEmpty()) {
        Pair<String, List<InputFiles>> scanResult =
            swiftLakeEngine
                .getIcebergScanExecutor()
                .executeTableScansAndUpdateSql(sql, tableFilters, connection, true);
        sql = scanResult.getLeft();
        addToInputFiles(scanResult.getRight());
      }

      // Apply query transformer if present
      if (queryTransformer != null) {
        sql = queryTransformer.transform(sql);
      }
      // Check for query timeout
      checkQueryTimeout();
      // Execute the query
      return statement.executeQuery(sql);
    } finally {
      // Clean up timer if it was started
      if (startTimer) {
        clearQueryTimeoutTask();
      }
    }
  }

  private void checkQueryTimeout() {
    if (isQueryCancelled) {
      throw new ValidationException("Query timed out.");
    }
  }

  /**
   * Executes an SQL update statement with table filters and query transformation.
   *
   * @param sql The SQL update statement to execute
   * @param tableFilters List of table filters to apply
   * @param queryTransformer Transformer to modify the SQL query
   * @return The number of rows affected by the update
   * @throws SQLException If a database access error occurs
   */
  public int executeUpdate(
      String sql, List<TableFilter> tableFilters, QueryTransformer queryTransformer)
      throws SQLException {
    return executeUpdate(sql, tableFilters, queryTransformer, true);
  }

  /**
   * Private method to execute an SQL update statement with optional timer.
   *
   * @param sql The SQL update statement to execute
   * @param tableFilters List of table filters to apply
   * @param queryTransformer Transformer to modify the SQL query
   * @param startTimer Flag to indicate whether to start the timer
   * @return The number of rows affected by the update
   * @throws SQLException If a database access error occurs
   */
  private int executeUpdate(
      String sql,
      List<TableFilter> tableFilters,
      QueryTransformer queryTransformer,
      boolean startTimer)
      throws SQLException {
    try {
      // Start timer if requested
      if (startTimer) {
        startQueryTimeoutTask();
      }
      // Apply table filters if present
      if (tableFilters != null && !tableFilters.isEmpty()) {
        Pair<String, List<InputFiles>> scanResult =
            swiftLakeEngine
                .getIcebergScanExecutor()
                .executeTableScansAndUpdateSql(sql, tableFilters, connection, true);
        sql = scanResult.getLeft();
        addToInputFiles(scanResult.getRight());
      }
      // Apply query transformer if present
      if (queryTransformer != null) {
        sql = queryTransformer.transform(sql);
      }
      // Check for query timeout
      checkQueryTimeout();
      // Execute the query
      return statement.executeUpdate(sql);
    } finally {
      // Clean up timer if it was started
      if (startTimer) {
        clearQueryTimeoutTask();
      }
    }
  }

  /**
   * Executes an SQL statement with table filters and query transformation.
   *
   * @param sql The SQL statement to execute
   * @param tableFilters List of table filters to apply
   * @param queryTransformer Transformer to modify the SQL query
   * @return true if the first result is a ResultSet object; false if it is an update count or there
   *     are no results
   * @throws SQLException If a database access error occurs
   */
  public boolean execute(
      String sql, List<TableFilter> tableFilters, QueryTransformer queryTransformer)
      throws SQLException {
    return execute(sql, tableFilters, queryTransformer, true);
  }

  private boolean execute(
      String sql,
      List<TableFilter> tableFilters,
      QueryTransformer queryTransformer,
      boolean startTimer)
      throws SQLException {
    try {
      // Start timer if requested
      if (startTimer) {
        startQueryTimeoutTask();
      }
      // Apply table filters if present
      if (tableFilters != null && !tableFilters.isEmpty()) {
        Pair<String, List<InputFiles>> scanResult =
            swiftLakeEngine
                .getIcebergScanExecutor()
                .executeTableScansAndUpdateSql(sql, tableFilters, connection, true);
        sql = scanResult.getLeft();
        addToInputFiles(scanResult.getRight());
      }
      // Apply query transformer if present
      if (queryTransformer != null) {
        sql = queryTransformer.transform(sql);
      }
      // Check for query timeout
      checkQueryTimeout();
      // Execute the query
      return statement.execute(sql);
    } finally {
      // Clean up timer if it was started
      if (startTimer) {
        clearQueryTimeoutTask();
      }
    }
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return executeQuery(sql, null);
  }

  /**
   * Executes the given SQL query with an optional query transformer.
   *
   * @param sql The SQL query to execute
   * @param queryTransformer An optional transformer to modify the query
   * @return A ResultSet containing the query results
   * @throws SQLException If a database access error occurs
   */
  public ResultSet executeQuery(String sql, QueryTransformer queryTransformer) throws SQLException {
    try {
      startQueryTimeoutTask();
      List<TableFilter> tableFilters = null;
      if (processTables) {
        // Process the SQL query and get table filters if needed
        Pair<String, List<TableFilter>> result = sqlQueryProcessor.process(sql, connection);
        sql = result.getLeft();
        tableFilters = result.getRight();
      }

      return this.executeQuery(sql, tableFilters, queryTransformer, false);
    } finally {
      clearQueryTimeoutTask();
    }
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    return executeUpdate(sql, (QueryTransformer) null);
  }

  /**
   * Executes the given SQL update statement with an optional query transformer.
   *
   * @param sql The SQL update statement to execute
   * @param queryTransformer An optional transformer to modify the query
   * @return The number of rows affected
   * @throws SQLException If a database access error occurs
   */
  public int executeUpdate(String sql, QueryTransformer queryTransformer) throws SQLException {
    try {
      startQueryTimeoutTask();
      List<TableFilter> tableFilters = null;
      if (processTables) {
        // Process the SQL query and get table filters if needed
        Pair<String, List<TableFilter>> result = sqlQueryProcessor.process(sql, connection);
        sql = result.getLeft();
        tableFilters = result.getRight();
      }
      return this.executeUpdate(sql, tableFilters, queryTransformer, false);
    } finally {
      clearQueryTimeoutTask();
    }
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    return execute(sql, (QueryTransformer) null);
  }

  /**
   * Executes the given SQL statement with an optional query transformer.
   *
   * @param sql The SQL statement to execute
   * @param queryTransformer An optional transformer to modify the query
   * @return true if the first result is a ResultSet object; false if it is an update count or there
   *     are no results
   * @throws SQLException If a database access error occurs
   */
  public boolean execute(String sql, QueryTransformer queryTransformer) throws SQLException {
    try {
      startQueryTimeoutTask();
      List<TableFilter> tableFilters = null;
      if (processTables) {
        // Process the SQL query and get table filters if needed
        Pair<String, List<TableFilter>> result = sqlQueryProcessor.process(sql, connection);
        sql = result.getLeft();
        tableFilters = result.getRight();
      }

      return this.execute(sql, tableFilters, queryTransformer, false);
    } finally {
      clearQueryTimeoutTask();
    }
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    try {
      startQueryTimeoutTask();
      processSqlAndCreatePreparedStatement();
      checkQueryTimeout();
      return statement.executeQuery();
    } finally {
      clearQueryTimeoutTask();
    }
  }

  @Override
  public int executeUpdate() throws SQLException {
    try {
      startQueryTimeoutTask();
      processSqlAndCreatePreparedStatement();
      checkQueryTimeout();
      return statement.executeUpdate();
    } finally {
      clearQueryTimeoutTask();
    }
  }

  @Override
  public boolean execute() throws SQLException {
    try {
      startQueryTimeoutTask();
      processSqlAndCreatePreparedStatement();
      checkQueryTimeout();
      return statement.execute();
    } finally {
      clearQueryTimeoutTask();
    }
  }

  /**
   * Processes the SQL query and creates a PreparedStatement. This method handles SQL processing,
   * table filtering, and parameter setting.
   */
  private void processSqlAndCreatePreparedStatement() {
    if (preparedStmtSql == null) {
      return;
    }

    // Process the SQL query
    Pair<String, List<TableFilter>> result =
        sqlQueryProcessor.process(preparedStmtSql, params, connection, null);
    String sql = result.getLeft();
    List<TableFilter> tableFilters = result.getRight();

    // Apply table filters if present
    if (tableFilters != null && !tableFilters.isEmpty()) {
      Pair<String, List<InputFiles>> scanResult =
          swiftLakeEngine
              .getIcebergScanExecutor()
              .executeTableScansAndUpdateSql(sql, tableFilters, connection, true);
      sql = scanResult.getLeft();
      addToInputFiles(scanResult.getRight());
    } else {
      sql = preparedStmtSql;
    }

    // Create the PreparedStatement
    this.statement = stmtFunc.apply(sql);

    // Set parameters for the PreparedStatement
    for (Map.Entry<Integer, Object> entry : params.entrySet()) {
      try {
        if (!paramsTargetSqlTypes.containsKey(entry.getKey())) {
          this.statement.setObject(entry.getKey(), entry.getValue());
        } else {
          Pair<Integer, Integer> targtSqlTypePair = paramsTargetSqlTypes.get(entry.getKey());
          if (targtSqlTypePair.getRight() != null) {
            this.statement.setObject(
                entry.getKey(),
                entry.getValue(),
                targtSqlTypePair.getLeft(),
                targtSqlTypePair.getRight());
          } else {
            this.statement.setObject(entry.getKey(), entry.getValue(), targtSqlTypePair.getLeft());
          }
        }
      } catch (SQLException e) {
        throw new SwiftLakeException(
            e, "An error occurred while setting parameters to PreparedStatement");
      }
    }
  }

  private void addToInputFiles(List<InputFiles> files) {
    if (inputFiles == null) inputFiles = new ArrayList<>();

    inputFiles.addAll(files);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, null);
    } else {
      statement.setNull(parameterIndex, sqlType);
    }
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setBoolean(parameterIndex, x);
    }
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setByte(parameterIndex, x);
    }
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setShort(parameterIndex, x);
    }
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setInt(parameterIndex, x);
    }
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setLong(parameterIndex, x);
    }
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setFloat(parameterIndex, x);
    }
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setDouble(parameterIndex, x);
    }
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setBigDecimal(parameterIndex, x);
    }
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setString(parameterIndex, x);
    }
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setBytes(parameterIndex, x);
    }
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setDate(parameterIndex, x);
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setTime(parameterIndex, x);
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setTimestamp(parameterIndex, x);
    }
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setAsciiStream(parameterIndex, x, length);
    }
  }

  @Override
  @Deprecated
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setUnicodeStream(parameterIndex, x, length);
    }
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setBinaryStream(parameterIndex, x, length);
    }
  }

  @Override
  public void clearParameters() throws SQLException {
    params = null;
    statement.clearParameters();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
      paramsTargetSqlTypes.put(parameterIndex, Pair.of(targetSqlType, null));
    } else {
      statement.setObject(parameterIndex, x, targetSqlType);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setObject(parameterIndex, x);
    }
  }

  @Override
  public void addBatch() throws SQLException {
    ValidationException.check(!processTables, "Batch mode is not supported");
    statement.addBatch();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    if (preparedStmtSql != null) {
      throw new SQLFeatureNotSupportedException("setCharacterStream");
    } else {
      statement.setCharacterStream(parameterIndex, reader, length);
    }
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    if (preparedStmtSql != null) {
      throw new SQLFeatureNotSupportedException("setRef");
    } else {
      statement.setRef(parameterIndex, x);
    }
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    if (preparedStmtSql != null) {
      throw new SQLFeatureNotSupportedException("setBlob");
    } else {
      statement.setBlob(parameterIndex, x);
    }
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    if (preparedStmtSql != null) {
      throw new SQLFeatureNotSupportedException("setClob");
    } else {
      statement.setClob(parameterIndex, x);
    }
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setArray(parameterIndex, x);
    }
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return statement.getMetaData();
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("setDate");
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("setDate");
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("setDate");
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, null);
    } else {
      statement.setNull(parameterIndex, sqlType, typeName);
    }
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
    } else {
      statement.setURL(parameterIndex, x);
    }
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return statement.getParameterMetaData();
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setRowId");
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, value);
    } else {
      statement.setNString(parameterIndex, value);
    }
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("setNCharacterStream");
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new SQLFeatureNotSupportedException("setNClob");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("setClob");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("setBlob");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("setNClob");
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("setSQLXML");
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    if (preparedStmtSql != null) {
      params.put(parameterIndex, x);
      paramsTargetSqlTypes.put(parameterIndex, Pair.of(targetSqlType, scaleOrLength));
    } else {
      statement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("setAsciiStream");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("setBinaryStream");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("setCharacterStream");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setAsciiStream");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    if (preparedStmtSql != null) {
      throw new SQLFeatureNotSupportedException("setAsciiStream");
    } else {
      statement.setBinaryStream(parameterIndex, x);
    }
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    if (preparedStmtSql != null) {
      throw new SQLFeatureNotSupportedException("setCharacterStream");
    } else {
      statement.setCharacterStream(parameterIndex, reader);
    }
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    if (preparedStmtSql != null) {
      throw new SQLFeatureNotSupportedException("setNCharacterStream");
    } else {
      statement.setNCharacterStream(parameterIndex, value);
    }
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    if (preparedStmtSql != null) {
      throw new SQLFeatureNotSupportedException("setClob");
    } else {
      statement.setClob(parameterIndex, reader);
    }
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    if (preparedStmtSql != null) {
      throw new SQLFeatureNotSupportedException("setBlob");
    } else {
      statement.setBlob(parameterIndex, inputStream);
    }
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    if (preparedStmtSql != null) {
      throw new SQLFeatureNotSupportedException("setNClob");
    } else {
      statement.setNClob(parameterIndex, reader);
    }
  }

  @Override
  public void close() throws SQLException {
    try {
      if (statement != null) {
        statement.close();
      }
      connection = null;
    } finally {
      if (inputFiles != null) {
        inputFiles.forEach(
            f -> {
              try {
                f.close();
              } catch (Exception e) {
                LOGGER.error("An error occurred while closing resources.");
              }
            });

        inputFiles = null;
      }
    }
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return statement.getMaxFieldSize();
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    statement.setMaxFieldSize(max);
  }

  @Override
  public int getMaxRows() throws SQLException {
    return statement.getMaxRows();
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    statement.setMaxRows(max);
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    statement.setEscapeProcessing(enable);
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    long seconds = queryTimeoutInMillis / 1000;
    if (seconds >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) seconds;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    setQueryTimeoutMs(seconds * 1000L);
  }

  private void setQueryTimeoutMs(long millis) throws SQLException {
    checkClosed();
    ValidationException.check(
        millis >= 0, "Query timeout must be a value greater than or equals to 0.");
    queryTimeoutInMillis = millis;
  }

  private void checkClosed() throws SQLException {
    ValidationException.check(!isClosed(), "This statement has been closed.");
  }

  @Override
  public void cancel() throws SQLException {
    statement.cancel();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return statement.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    statement.clearWarnings();
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    statement.setCursorName(name);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return statement.getResultSet();
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return statement.getUpdateCount();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return statement.getMoreResults();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    statement.setFetchDirection(direction);
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return statement.getFetchDirection();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    statement.setFetchSize(rows);
  }

  @Override
  public int getFetchSize() throws SQLException {
    return statement.getFetchSize();
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return statement.getResultSetConcurrency();
  }

  @Override
  public int getResultSetType() throws SQLException {
    return statement.getResultSetType();
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    ValidationException.check(!processTables, "Batch mode is not supported");
    statement.addBatch(sql);
  }

  @Override
  public void clearBatch() throws SQLException {
    ValidationException.check(!processTables, "Batch mode is not supported");
    statement.clearBatch();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    ValidationException.check(!processTables, "Batch mode is not supported");
    try {
      startQueryTimeoutTask();
      return statement.executeBatch();
    } finally {
      clearQueryTimeoutTask();
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    if (isClosed()) {
      throw new SQLException("Statement was closed");
    }
    return connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return statement.getMoreResults(current);
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return statement.getGeneratedKeys();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    try {
      startQueryTimeoutTask();
      return statement.executeUpdate(sql, autoGeneratedKeys);
    } finally {
      clearQueryTimeoutTask();
    }
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    try {
      startQueryTimeoutTask();
      return statement.executeUpdate(sql, columnIndexes);
    } finally {
      clearQueryTimeoutTask();
    }
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    try {
      startQueryTimeoutTask();
      return statement.executeUpdate(sql, columnNames);
    } finally {
      clearQueryTimeoutTask();
    }
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    try {
      startQueryTimeoutTask();
      return statement.execute(sql, autoGeneratedKeys);
    } finally {
      clearQueryTimeoutTask();
    }
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    try {
      startQueryTimeoutTask();
      return statement.execute(sql, columnIndexes);
    } finally {
      clearQueryTimeoutTask();
    }
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    try {
      startQueryTimeoutTask();
      return statement.execute(sql, columnNames);
    } finally {
      clearQueryTimeoutTask();
    }
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return statement.getResultSetHoldability();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return statement == null || statement.isClosed();
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    statement.setPoolable(poolable);
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return statement.isPoolable();
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException("closeOnCompletion");
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return statement.isCloseOnCompletion();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return statement.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return statement.isWrapperFor(iface);
  }

  /** Starts the query execution timer. */
  private void startQueryTimeoutTask() {
    clearQueryTimeoutTask();

    if (queryTimeoutInMillis == 0) {
      return;
    }

    TimerTask timeoutTask =
        new TimerTask() {
          @Override
          public void run() {
            try {
              if (!TIMEOUT_TASK_UPDATER.compareAndSet(
                  SwiftLakePreparedStatement.this, this, null)) {
                return;
              }
              SwiftLakePreparedStatement.this.isQueryCancelled = true;
              SwiftLakePreparedStatement.this.cancel();
            } catch (Exception e) {
              LOGGER.debug("An error occurred while cancelling the query", e);
            }
          }
        };

    TIMEOUT_TASK_UPDATER.set(this, timeoutTask);
    swiftLakeEngine.getTimer().schedule(timeoutTask, queryTimeoutInMillis);
  }

  /**
   * Clear the query execution timer.
   *
   * @return true if the timer was successfully cleared or if no timeout was set, false otherwise
   */
  private boolean clearQueryTimeoutTask() {
    isQueryCancelled = false;
    TimerTask timeoutTask = TIMEOUT_TASK_UPDATER.get(this);
    if (timeoutTask == null) {
      return queryTimeoutInMillis == 0;
    }
    if (!TIMEOUT_TASK_UPDATER.compareAndSet(this, timeoutTask, null)) {
      return false;
    }
    timeoutTask.cancel();
    swiftLakeEngine.getTimer().purge();
    return true;
  }
}
