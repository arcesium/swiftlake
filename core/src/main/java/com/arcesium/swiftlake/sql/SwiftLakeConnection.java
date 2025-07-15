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
import com.arcesium.swiftlake.common.SwiftLakeException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SwiftLakeConnection class that wraps a DuckDB connection and provides additional functionality.
 */
public class SwiftLakeConnection implements java.sql.Connection {
  private static final Logger LOGGER = LoggerFactory.getLogger(SwiftLakeConnection.class);
  private final SwiftLakeEngine swiftLakeEngine;
  private final DuckDBConnection connection;
  private final boolean processTables;
  private boolean requestStarted = false;

  /**
   * Constructs a new SwiftLakeConnection.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param connection The DuckDBConnection instance.
   * @param processTables A boolean flag indicating whether to process tables.
   */
  public SwiftLakeConnection(
      SwiftLakeEngine swiftLakeEngine, DuckDBConnection connection, boolean processTables) {
    this.swiftLakeEngine = swiftLakeEngine;
    this.connection = connection;
    this.processTables = processTables;
  }

  /**
   * Creates a PreparedStatement for the given SQL.
   *
   * @param sql The SQL query
   * @return A PreparedStatement object
   * @throws SQLException If a database access error occurs
   */
  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return prepareStatement(sql, (QueryTransformer) null);
  }

  /**
   * Creates a PreparedStatement with control over table processing.
   *
   * @param sql The SQL query
   * @param processTables Whether to process tables
   * @return A PreparedStatement object
   * @throws SQLException If a database access error occurs
   */
  public PreparedStatement prepareStatement(String sql, boolean processTables) throws SQLException {
    return prepareStatement(sql, null, processTables);
  }

  /**
   * Creates a PreparedStatement with an optional query transformer.
   *
   * @param sql The SQL query
   * @param queryTransformer Transformer to modify the SQL query (can be null)
   * @return A PreparedStatement object
   * @throws SQLException If a database access error occurs
   */
  public PreparedStatement prepareStatement(String sql, QueryTransformer queryTransformer)
      throws SQLException {
    return prepareStatement(sql, queryTransformer, processTables);
  }

  /**
   * Creates a PreparedStatement with query transformation and table processing options.
   *
   * @param sql The SQL query
   * @param queryTransformer Transformer to modify the SQL query (can be null)
   * @param processTables Whether to process tables
   * @return A PreparedStatement object
   * @throws SQLException If a database access error occurs
   */
  public PreparedStatement prepareStatement(
      String sql, QueryTransformer queryTransformer, boolean processTables) throws SQLException {
    if (!processTables) {
      if (queryTransformer != null) {
        sql = queryTransformer.transform(sql);
      }
      return new SwiftLakePreparedStatement(
          swiftLakeEngine,
          this,
          (DuckDBPreparedStatement) connection.prepareStatement(sql),
          processTables);
    } else {
      try {
        return new SwiftLakePreparedStatement(
            swiftLakeEngine,
            this,
            sql,
            processTables,
            (updatedSql) -> {
              try {
                if (queryTransformer != null) {
                  updatedSql = queryTransformer.transform(updatedSql);
                }
                return (DuckDBPreparedStatement) connection.prepareStatement(updatedSql);
              } catch (SQLException ex) {
                throw new SwiftLakeException(ex, "An error occurred while creating statement.");
              }
            });
      } catch (SwiftLakeException ex) {
        throw new SQLException(ex.getCause());
      }
    }
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    if (!processTables) {
      return new SwiftLakePreparedStatement(
          swiftLakeEngine,
          this,
          (DuckDBPreparedStatement)
              connection.prepareStatement(
                  sql, resultSetType, resultSetConcurrency, resultSetHoldability),
          processTables);
    } else {
      try {
        return new SwiftLakePreparedStatement(
            swiftLakeEngine,
            this,
            sql,
            processTables,
            (updatedSql) -> {
              try {
                return (DuckDBPreparedStatement)
                    connection.prepareStatement(
                        updatedSql, resultSetType, resultSetConcurrency, resultSetHoldability);
              } catch (SQLException ex) {
                throw new SwiftLakeException(ex, "An error occurred while creating statement.");
              }
            });
      } catch (SwiftLakeException ex) {
        throw new SQLException(ex.getCause());
      }
    }
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    if (!processTables) {
      return new SwiftLakePreparedStatement(
          swiftLakeEngine,
          this,
          (DuckDBPreparedStatement)
              connection.prepareStatement(sql, resultSetType, resultSetConcurrency),
          processTables);
    } else {
      try {
        return new SwiftLakePreparedStatement(
            swiftLakeEngine,
            this,
            sql,
            processTables,
            (updatedSql) -> {
              try {
                return (DuckDBPreparedStatement)
                    connection.prepareStatement(updatedSql, resultSetType, resultSetConcurrency);
              } catch (SQLException ex) {
                throw new SwiftLakeException(ex, "An error occurred while creating statement.");
              }
            });
      } catch (SwiftLakeException ex) {
        throw new SQLException(ex.getCause());
      }
    }
  }

  @Override
  public Statement createStatement() throws SQLException {
    return createStatement(processTables);
  }

  public Statement createStatement(boolean processTables) throws SQLException {
    return new SwiftLakePreparedStatement(
        swiftLakeEngine,
        this,
        (DuckDBPreparedStatement) connection.createStatement(),
        processTables);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return new SwiftLakePreparedStatement(
        swiftLakeEngine,
        this,
        (DuckDBPreparedStatement) connection.createStatement(resultSetType, resultSetConcurrency),
        processTables);
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return new SwiftLakePreparedStatement(
        swiftLakeEngine,
        this,
        (DuckDBPreparedStatement)
            connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability),
        processTables);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return connection.prepareCall(sql);
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return connection.nativeSQL(sql);
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    connection.setAutoCommit(autoCommit);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return connection.getAutoCommit();
  }

  @Override
  public void commit() throws SQLException {
    connection.commit();
  }

  @Override
  public void rollback() throws SQLException {
    connection.rollback();
  }

  @Override
  public void close() throws SQLException {
    swiftLakeEngine.closeDuckDBConnection(connection);
  }

  @Override
  public boolean isClosed() throws SQLException {
    return connection.isClosed();
  }

  @Override
  public void beginRequest() throws SQLException {
    requestStarted = true;
  }

  @Override
  public void endRequest() throws SQLException {
    if (requestStarted) {
      dropTempDBObjects();
      requestStarted = false;
    }
  }

  /**
   * Drops all temporary database objects in the DuckDB 'temp' database. This includes tables,
   * views, sequences, macros, and macro tables.
   *
   * @throws SQLException if there's an error executing SQL statements
   */
  private void dropTempDBObjects() throws SQLException {
    try (var stmt = connection.createStatement()) {
      StringBuilder query = new StringBuilder();
      query.append(
          "SELECT 'TABLE' AS type, table_name AS name, schema_name FROM duckdb_tables() WHERE temporary=true AND database_name='temp'");
      query.append(" UNION ALL ");
      query.append(
          "SELECT 'VIEW' AS type, view_name AS name, schema_name FROM duckdb_views() WHERE internal=false AND temporary=true AND database_name='temp'");
      query.append(" UNION ALL ");
      query.append(
          "SELECT 'SEQUENCE' AS type, sequence_name AS name, schema_name FROM duckdb_sequences() WHERE temporary=true AND database_name='temp'");
      query.append(" UNION ALL ");
      query.append(
          "SELECT 'MACRO' AS type, function_name AS name, schema_name FROM duckdb_functions() WHERE internal=false AND database_name='temp' AND function_type='macro'");
      query.append(" UNION ALL ");
      query.append(
          "SELECT 'MACRO TABLE' AS type, function_name AS name, schema_name FROM duckdb_functions() WHERE internal=false AND database_name='temp' AND function_type='table_macro'");

      try {
        List<List<String>> names = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(query.toString())) {
          while (rs.next()) {
            names.add(
                Arrays.asList(
                    rs.getString("type"), rs.getString("schema_name"), rs.getString("name")));
          }
        }
        if (!names.isEmpty()) {
          StringBuilder dropQuery = new StringBuilder();
          for (var name : names) {
            dropQuery.append("DROP ");
            dropQuery.append(name.get(0));
            dropQuery.append(" temp.");
            dropQuery.append(name.get(1));
            dropQuery.append(".");
            dropQuery.append(name.get(2));
            dropQuery.append(";");
          }
          stmt.executeUpdate(dropQuery.toString());
        }
      } catch (Exception ex) {
        LOGGER.warn("An exception occurred while dropping tempdb objects", ex);
      }
    }
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return connection.getMetaData();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    connection.setReadOnly(readOnly);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return connection.isReadOnly();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    connection.setCatalog(catalog);
  }

  @Override
  public String getCatalog() throws SQLException {
    return connection.getCatalog();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    connection.setTransactionIsolation(level);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return connection.getTransactionIsolation();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return connection.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    connection.clearWarnings();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return connection.getTypeMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    connection.setTypeMap(map);
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    connection.setHoldability(holdability);
  }

  @Override
  public int getHoldability() throws SQLException {
    return connection.getHoldability();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return connection.setSavepoint();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    return connection.setSavepoint(name);
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    connection.rollback(savepoint);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    connection.releaseSavepoint(savepoint);
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return connection.prepareStatement(sql, autoGeneratedKeys);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return connection.prepareStatement(sql, columnIndexes);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return connection.prepareStatement(sql, columnNames);
  }

  @Override
  public Clob createClob() throws SQLException {
    return connection.createClob();
  }

  @Override
  public Blob createBlob() throws SQLException {
    return connection.createBlob();
  }

  @Override
  public NClob createNClob() throws SQLException {
    return connection.createNClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return connection.createSQLXML();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return connection.isValid(timeout);
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    connection.setClientInfo(name, value);
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    connection.setClientInfo(properties);
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return connection.getClientInfo(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return connection.getClientInfo();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    return connection.createArrayOf(typeName, elements);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return connection.createStruct(typeName, attributes);
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    connection.setSchema(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    return connection.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    connection.abort(executor);
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    connection.setNetworkTimeout(executor, milliseconds);
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return connection.getNetworkTimeout();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return connection.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return connection.isWrapperFor(iface);
  }
}
