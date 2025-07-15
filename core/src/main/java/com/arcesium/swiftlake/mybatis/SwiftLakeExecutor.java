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
package com.arcesium.swiftlake.mybatis;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.sql.QueryTransformer;
import com.arcesium.swiftlake.sql.TableFilter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.SimpleExecutor;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SwiftLakeExecutor extends SimpleExecutor to provide custom query execution with SwiftLake
 * functionality.
 */
public class SwiftLakeExecutor extends SimpleExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(SwiftLakeExecutor.class);
  private final SwiftLakeEngine swiftLakeEngine;

  /**
   * Constructor for SwiftLakeExecutor.
   *
   * @param configuration The MyBatis configuration
   * @param transaction The transaction object
   * @param swiftLakeEngine The SwiftLakeEngine instance
   */
  public SwiftLakeExecutor(
      Configuration configuration, Transaction transaction, SwiftLakeEngine swiftLakeEngine) {
    super(configuration, transaction);
    this.swiftLakeEngine = swiftLakeEngine;
  }

  /**
   * Executes a query with SwiftLake functionality.
   *
   * @param ms The MappedStatement
   * @param parameter The parameter object
   * @param rowBounds The row bounds
   * @param resultHandler The result handler
   * @param <E> The type of the result
   * @return List of query results
   * @throws SQLException if a database access error occurs
   */
  @Override
  public <E> List<E> query(
      MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler)
      throws SQLException {
    List<AutoCloseable> resources = null;
    try {
      Pair<BoundSql, List<AutoCloseable>> result = processStatement(ms, parameter);
      resources = result.getRight();
      BoundSql boundSql = result.getLeft();
      parameter = boundSql.getParameterObject();
      CacheKey key = createCacheKey(ms, parameter, rowBounds, boundSql);
      return query(ms, parameter, rowBounds, resultHandler, key, boundSql);
    } finally {
      closeResources(resources);
    }
  }

  /**
   * Executes an update operation with SwiftLake functionality.
   *
   * @param ms The MappedStatement
   * @param parameter The parameter object
   * @return The number of rows affected
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int doUpdate(MappedStatement ms, Object parameter) throws SQLException {
    Statement stmt = null;
    List<AutoCloseable> resources = null;
    try {
      Pair<BoundSql, List<AutoCloseable>> result = processStatement(ms, parameter);
      resources = result.getRight();
      BoundSql boundSql = result.getLeft();
      parameter = boundSql.getParameterObject();

      Configuration configuration = ms.getConfiguration();
      StatementHandler handler =
          configuration.newStatementHandler(this, ms, parameter, RowBounds.DEFAULT, null, boundSql);
      stmt = prepareStatement(handler, ms.getStatementLog());
      return handler.update(stmt);
    } finally {
      closeStatement(stmt);
      closeResources(resources);
    }
  }

  private Statement prepareStatement(StatementHandler handler, Log statementLog)
      throws SQLException {
    Statement stmt;
    Connection connection = getConnection(statementLog);
    stmt = handler.prepare(connection, transaction.getTimeout());
    handler.parameterize(stmt);
    return stmt;
  }

  /**
   * Processes the statement, applying table filters and query transformations if necessary.
   *
   * @param ms The MappedStatement
   * @param parameter The parameter object
   * @return A Pair containing the modified BoundSql and a list of resources to be closed
   */
  private Pair<BoundSql, List<AutoCloseable>> processStatement(
      MappedStatement ms, Object parameter) {
    List<TableFilter> tableFilters = new ArrayList<>();
    QueryTransformer queryTransformer = null;
    if (parameter instanceof QueryProperties<?> queryProperties) {
      parameter = queryProperties.getParameter();
      if (queryProperties.getTableFilters() != null) {
        tableFilters.addAll(queryProperties.getTableFilters());
      }
      queryTransformer = queryProperties.getQueryTransformer();
    }

    BoundSql boundSql = ms.getBoundSql(parameter);
    // If no filters or transformers, return the original BoundSql
    if (tableFilters.isEmpty() && queryTransformer == null) {
      return Pair.of(boundSql, null);
    }

    List<AutoCloseable> resources = new ArrayList<>();
    try {
      String sql = boundSql.getSql();
      // Run table scans and replace the table name with the data file paths
      if (!tableFilters.isEmpty()) {
        Connection connection = null;
        try {
          connection = getTransaction().getConnection();
        } catch (SQLException e) {
          throw new SwiftLakeException(e, "Failed to get database connection.");
        }
        Pair<String, List<InputFiles>> scanResult =
            swiftLakeEngine
                .getIcebergScanExecutor()
                .executeTableScansAndUpdateSql(sql, tableFilters, connection, true);
        sql = scanResult.getLeft();
        resources.addAll(scanResult.getRight());
      }
      // Apply query transformation if a transformer is provided
      if (queryTransformer != null) {
        sql = queryTransformer.transform(sql);
      }

      BoundSql newBoundSql = new SwiftLakeBoundSql(configuration, boundSql, sql);

      return Pair.of(newBoundSql, resources);
    } catch (Throwable t) {
      closeResources(resources);
      if (t instanceof ValidationException || t instanceof SwiftLakeException) {
        throw t;
      } else if (t instanceof Error error) {
        throw error;
      } else {
        throw new SwiftLakeException(t, "An error occurred while processing statement.");
      }
    }
  }

  /**
   * Closes the provided resources.
   *
   * @param resources The list of AutoCloseable resources to be closed
   */
  private void closeResources(List<AutoCloseable> resources) {
    if (resources != null) {
      resources.forEach(
          f -> {
            try {
              f.close();
            } catch (Exception e) {
              LOGGER.error("An error occurred while closing resources.");
            }
          });
    }
  }
}
