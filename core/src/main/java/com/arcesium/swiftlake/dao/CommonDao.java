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

import com.arcesium.swiftlake.SwiftLakeEngine;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.session.SqlSession;

/**
 * Data Access Object for common database operations in the SwiftLake system. Extends BaseDao to
 * leverage common DAO functionality.
 */
public class CommonDao extends BaseDao {
  private static final String namespace = "Common";

  /**
   * Constructs a CommonDao with the given SwiftLakeEngine.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance to use for database operations.
   */
  public CommonDao(SwiftLakeEngine swiftLakeEngine) {
    super(swiftLakeEngine);
  }

  /**
   * Retrieves distinct file names from a given data path.
   *
   * @param dataPath The path to search for file names.
   * @return A list of distinct file names.
   */
  public List<String> getDistinctFileNames(String dataPath) {
    try (SqlSession session = getSession(); ) {
      return session.selectList(namespace + ".getDistinctFileNames", dataPath);
    }
  }

  /**
   * Performs a merge cardinality check on a given table.
   *
   * @param table The name of the table to check.
   * @return A Boolean indicating the result of the cardinality check.
   */
  public Boolean mergeCardinalityCheck(String table) {
    try (SqlSession session = getSession()) {
      return session.selectOne(namespace + ".mergeCardinalityCheck", table);
    }
  }

  /**
   * Gets the total row count of a table.
   *
   * @param table The name of the table.
   * @return The total number of rows in the table.
   */
  public Long getTotalRowCount(String table) {
    return getTotalRowCount(table, "1=1");
  }

  /**
   * Checks if a table is empty.
   *
   * @param table The name of the table to check.
   * @return A Boolean indicating whether the table is empty.
   */
  public Boolean isTableEmpty(String table) {
    try (SqlSession session = getSession()) {
      return session.selectOne(namespace + ".isTableEmpty", table);
    }
  }

  /**
   * Gets the total row count of a table with a specified condition.
   *
   * @param table The name of the table.
   * @param condition The condition to apply when counting rows.
   * @return The total number of rows that match the condition.
   */
  public Long getTotalRowCount(String table, String condition) {
    try (SqlSession session = getSession()) {
      Map<String, Object> params = new HashMap<>();
      params.put("table", table);
      params.put("condition", condition);
      return session.selectOne(namespace + ".getTotalRowCount", params);
    }
  }

  /**
   * Generates SQL to read data from Parquet files.
   *
   * @param files List of Parquet file paths.
   * @param columns List of columns to select.
   * @param unionByName Whether to union by name.
   * @param hivePartitioning Whether Hive partitioning is used.
   * @param addFileRowNumberColumn Whether to add a file row number column.
   * @param addFileNameColumn Whether to add a file name column.
   * @param wrap Whether to wrap the SQL.
   * @return The generated SQL string.
   */
  public String getDataSqlFromParquetFiles(
      List<String> files,
      List<String> columns,
      boolean unionByName,
      boolean hivePartitioning,
      boolean addFileRowNumberColumn,
      boolean addFileNameColumn,
      boolean wrap) {
    Map<String, Object> params = new HashMap<>();
    params.put("files", files);
    params.put("columns", columns);
    params.put("hivePartitioning", hivePartitioning);
    params.put("unionByName", unionByName);
    params.put("addFileRowNumberColumn", addFileRowNumberColumn);
    params.put("addFileNameColumn", addFileNameColumn);
    params.put("wrap", wrap);
    return getSql(namespace + ".getDataFromParquetFiles", params);
  }

  /**
   * Retrieves distinct values for specified columns in a table.
   *
   * @param table The name of the table.
   * @param columns The list of columns to get distinct values for.
   * @return A list of maps containing the distinct values.
   */
  public List<Map<String, Object>> getDistinctValues(String table, List<String> columns) {
    try (SqlSession session = getSession()) {
      Map<String, Object> params = new HashMap<>();
      params.put("table", table);
      params.put("columns", columns);
      return session.selectList(namespace + ".getDistinctValues", params);
    }
  }

  /**
   * Generates SQL to copy data to a file.
   *
   * @param sql The SQL query to execute.
   * @param orderBy The ORDER BY clause.
   * @param outputPath The path where the output file will be saved.
   * @param options Additional options for the copy operation.
   * @return The generated SQL string for the copy operation.
   */
  public String getCopyToFileSql(String sql, String orderBy, String outputPath, String options) {
    Map<String, Object> params = new HashMap<>();
    params.put("sql", sql);
    params.put("orderBy", orderBy);
    params.put("outputPath", outputPath);
    params.put("options", options);
    return getSql(namespace + ".copyToFile", params);
  }

  /**
   * Generates SQL to drop a table.
   *
   * @param tableName The name of the table to drop.
   * @return The SQL string to drop the table.
   */
  public String getDropTableSql(String tableName) {
    StringBuilder sb = new StringBuilder();
    sb.append("DROP TABLE ");
    sb.append(tableName);
    return sb.toString();
  }

  /**
   * Retrieves DuckDB runtime metrics.
   *
   * @return A list of maps containing DuckDB runtime metrics.
   */
  public List<Map<String, Object>> getDuckDBRuntimeMetrics() {
    try (SqlSession session = getSession()) {
      return session.selectList(namespace + ".getDuckDBRuntimeMetrics");
    }
  }
}
