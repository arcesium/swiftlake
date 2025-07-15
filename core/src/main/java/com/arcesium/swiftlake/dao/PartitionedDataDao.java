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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data Access Object for handling partitioned data operations in the SwiftLake system. Extends
 * BaseDao to leverage common DAO functionality.
 */
public class PartitionedDataDao extends BaseDao {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedDataDao.class);
  private static final String namespace = "PartitionedData";

  /**
   * Constructs a PartitionedDataDao with the given SwiftLakeEngine.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance to use for database operations.
   */
  public PartitionedDataDao(SwiftLakeEngine swiftLakeEngine) {
    super(swiftLakeEngine);
  }

  /**
   * Retrieves distinct partitions from a table based on specified columns and conditions.
   *
   * @param table The name of the table to query.
   * @param columns List of column names to consider for partitions.
   * @param currentFlagColumn Name of the column indicating current records.
   * @param effectiveEndColumn Name of the column for effective end.
   * @param singleBucketColumns Set of columns that should have a single bucket.
   * @return List of maps representing distinct partitions.
   */
  public List<Map<String, Object>> getDistinctPartitions(
      String table,
      List<String> columns,
      String currentFlagColumn,
      String effectiveEndColumn,
      Set<String> singleBucketColumns) {
    try (SqlSession session = getSession(); ) {
      Map<String, Object> params = new HashMap<>();
      params.put("table", table);
      params.put("columns", columns);
      params.put("currentFlagColumn", currentFlagColumn);
      params.put("effectiveEndColumn", effectiveEndColumn);
      params.put("singleBucketColumns", singleBucketColumns);
      return getDataInMap(session, namespace + ".getDistinctPartitions", params);
    }
  }

  /**
   * Retrieves transformed distinct partitions from a partitions data table.
   *
   * @param partitionsDataTable The name of the partitions data table.
   * @param partitionColumns List of partition column names.
   * @return List of maps representing transformed distinct partitions.
   */
  public List<Map<String, Object>> getDistinctPartitionsTransformed(
      String partitionsDataTable, List<String> partitionColumns) {
    try (SqlSession session = getSession()) {
      Map<String, Object> params = new HashMap<>();
      params.put("partitionsDataTable", partitionsDataTable);
      params.put("partitionColumns", partitionColumns);
      return getDataInMap(session, namespace + ".getDistinctPartitionsTransformed", params);
    }
  }

  /**
   * Retrieves distinct partition values from a specified table.
   *
   * @param table The name of the table to query.
   * @param partitionColumns List of partition column names.
   * @return List of maps representing distinct partition values.
   */
  public List<Map<String, Object>> getDistinctPartitionValuesFromTable(
      String table, List<String> partitionColumns) {
    try (SqlSession session = getSession()) {
      Map<String, Object> params = new HashMap<>();
      params.put("table", table);
      params.put("partitionColumns", partitionColumns);
      return getDataInMap(session, namespace + ".getDistinctPartitionValuesFromTable", params);
    }
  }

  /**
   * Executes a SQL query and returns the result as a list of maps.
   *
   * @param session The SqlSession to use for the query.
   * @param mybatisStatementId The MyBatis statement ID to execute.
   * @param parameter The parameters for the SQL query.
   * @return List of maps representing the query results.
   */
  private List<Map<String, Object>> getDataInMap(
      SqlSession session, String mybatisStatementId, Object parameter) {
    try (Statement stmt = session.getConnection().createStatement()) {
      MappedStatement ms = session.getConfiguration().getMappedStatement(mybatisStatementId);
      BoundSql boundSql = ms.getBoundSql(parameter);
      String sql = boundSql.getSql();
      LOGGER.debug(sql);

      try (ResultSet resultSet = stmt.executeQuery(sql)) {
        return getDataInMap(resultSet);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Writes partitioned data to the specified output path.
   *
   * @param dataSql SQL to retrieve the data.
   * @param partitionsDataTable Name of the partitions data table.
   * @param outputPath Path where the output will be written.
   * @param compression Compression method to use.
   * @param projectedColumns List of columns to project.
   * @param partitionColumns List of partition columns.
   * @param effectiveEndColumn Name of the effective end column.
   * @param singleBucketColumns Set of columns that should have a single bucket.
   */
  public void write(
      String dataSql,
      String partitionsDataTable,
      String outputPath,
      String compression,
      List<String> projectedColumns,
      List<String> partitionColumns,
      String effectiveEndColumn,
      Set<String> singleBucketColumns) {
    try (SqlSession session = getSession()) {
      Map<String, Object> params = new HashMap<>();
      params.put("projectedColumns", projectedColumns);
      params.put("effectiveEndColumn", effectiveEndColumn);
      params.put("partitionColumns", partitionColumns);
      params.put("dataSql", dataSql);
      params.put("partitionsDataTable", partitionsDataTable);
      params.put("outputPath", outputPath);
      params.put("compression", compression);
      params.put("singleBucketColumns", singleBucketColumns);
      session.update(namespace + ".write", params);
    }
  }
}
