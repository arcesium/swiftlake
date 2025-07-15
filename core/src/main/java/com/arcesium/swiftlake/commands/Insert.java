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
package com.arcesium.swiftlake.commands;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.dao.CommonDao;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.metrics.CommitMetrics;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import com.arcesium.swiftlake.writer.PartitionedDataFileWriter;
import com.arcesium.swiftlake.writer.TableBatchTransaction;
import com.arcesium.swiftlake.writer.Transaction;
import com.arcesium.swiftlake.writer.UnpartitionedDataFileWriter;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class represents an Insert operation for SwiftLake tables. */
public class Insert {
  private static final Logger LOGGER = LoggerFactory.getLogger(Insert.class);
  private final SwiftLakeEngine swiftLakeEngine;
  private final Table table;
  private final CommonDao commonDao;
  private final String sql;
  private final String mybatisStatementId;
  private final Object mybatisStatementParameter;
  private boolean processSourceTables;
  private final SwiftLakeSqlSessionFactory sqlSessionFactory;
  private final boolean executeSqlOnceOnly;
  private final TableBatchTransaction tableBatchTransaction;
  private final boolean skipDataSorting;
  private final List<String> columns;
  private final Expression overwriteByFilter;
  private final String overwriteByFilterSql;
  private final List<String> overwriteByFilterColumns;
  private final String branch;
  private final Map<String, String> snapshotMetadata;
  private final IsolationLevel isolationLevel;

  /**
   * Constructor for the Insert class.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param table The table to insert into
   * @param sql The SQL statement for the insert operation
   * @param mybatisStatementId The MyBatis statement ID
   * @param mybatisStatementParameter The MyBatis statement parameter
   * @param sqlSessionFactory The SQL session factory
   * @param executeSqlOnceOnly Whether to execute the SQL only once
   * @param tableBatchTransaction The table batch transaction
   * @param skipDataSorting Whether to skip data sorting
   * @param columns The columns to insert
   * @param overwriteByFilter The overwrite filter expression
   * @param overwriteByFilterSql The overwrite filter SQL
   * @param overwriteByFilterColumns The columns for overwrite filter
   * @param branch The branch name
   * @param snapshotMetadata The snapshot metadata
   * @param isolationLevel The isolation level
   * @param processSourceTables Whether to process source tables
   */
  private Insert(
      SwiftLakeEngine swiftLakeEngine,
      Table table,
      String sql,
      String mybatisStatementId,
      Object mybatisStatementParameter,
      SwiftLakeSqlSessionFactory sqlSessionFactory,
      boolean executeSqlOnceOnly,
      TableBatchTransaction tableBatchTransaction,
      boolean skipDataSorting,
      List<String> columns,
      Expression overwriteByFilter,
      String overwriteByFilterSql,
      List<String> overwriteByFilterColumns,
      String branch,
      Map<String, String> snapshotMetadata,
      IsolationLevel isolationLevel,
      boolean processSourceTables) {
    this.swiftLakeEngine = swiftLakeEngine;
    this.table = table;
    this.sql = sql;
    this.mybatisStatementId = mybatisStatementId;
    this.mybatisStatementParameter = mybatisStatementParameter;
    this.processSourceTables = processSourceTables;
    this.sqlSessionFactory = sqlSessionFactory;
    this.executeSqlOnceOnly = executeSqlOnceOnly;
    this.commonDao = swiftLakeEngine.getCommonDao();
    this.tableBatchTransaction = tableBatchTransaction;
    this.skipDataSorting = skipDataSorting;
    this.columns = columns;
    this.overwriteByFilter = overwriteByFilter;
    this.overwriteByFilterSql = overwriteByFilterSql;
    this.overwriteByFilterColumns = overwriteByFilterColumns;
    this.branch = branch;
    this.snapshotMetadata = snapshotMetadata;
    this.isolationLevel = isolationLevel;
  }

  /**
   * Executes the insert operation and returns the commit metrics.
   *
   * @return The commit metrics for the insert operation
   */
  public CommitMetrics execute() {
    Transaction tx = executeWithoutCommit();
    if (tx == null) return new CommitMetrics(table.name());

    if (tableBatchTransaction != null) {
      tableBatchTransaction.add(tx);
      return null;
    } else {
      return tx.commit();
    }
  }

  /**
   * Executes the insert operation without committing the transaction. This method handles both
   * partitioned and unpartitioned tables, processes source tables if required, and creates a new
   * transaction based on the insert operation.
   *
   * @return A Transaction object representing the insert operation, or null if no new files were
   *     created.
   * @throws RuntimeException if an error occurs during the execution.
   */
  private Transaction executeWithoutCommit() {
    // Validate columns against the table schema
    WriteUtil.validateColumns(table, columns);

    // Create a temporary directory
    String tmpDir = swiftLakeEngine.getLocalDir() + "/insert/" + UUID.randomUUID().toString();
    new File(tmpDir).mkdirs();
    List<InputFiles> sourceSqlTmpFiles = null;
    try {
      String inputDataSql = this.getSql();
      if (processSourceTables) {
        // Process source tables if required
        Pair<String, List<InputFiles>> result =
            WriteUtil.processSourceTables(swiftLakeEngine, inputDataSql, table);
        inputDataSql = result.getLeft();
        sourceSqlTmpFiles = result.getRight();
      }
      List<com.arcesium.swiftlake.common.DataFile> newFiles;
      boolean isPartitionedTable = table.spec().isPartitioned();
      String insertDataSql = null;

      if (isPartitionedTable) {
        String partitionDataSql;

        if (executeSqlOnceOnly) {
          // Execute SQL and write to local Parquet files
          List<String> inputFilePaths =
              UnpartitionedDataFileWriter.builderFor(swiftLakeEngine, table, inputDataSql)
                  .columns(columns)
                  .tmpDir(tmpDir)
                  .skipDataSorting(true)
                  .targetFileSizeBytes(null)
                  .writeToPerThreadParquetFile(true)
                  .build()
                  .createLocalDataFiles();

          WriteUtil.closeInputFiles(sourceSqlTmpFiles);
          sourceSqlTmpFiles = null;
          // Generate SQL from the created Parquet files
          insertDataSql =
              commonDao.getDataSqlFromParquetFiles(
                  inputFilePaths, null, true, false, false, false, false);
          partitionDataSql = insertDataSql;
        } else {
          // Prepare SQL for partitioned table insert
          inputDataSql =
              WriteUtil.getSqlWithProjection(
                  swiftLakeEngine, table.schema(), inputDataSql, null, true, columns);
          partitionDataSql = inputDataSql;
          insertDataSql = inputDataSql;
        }
        // Write data to partitioned table
        newFiles =
            PartitionedDataFileWriter.builderFor(
                    swiftLakeEngine, table, partitionDataSql, insertDataSql)
                .skipDataSorting(skipDataSorting)
                .build()
                .write();
      } else {
        // Write data to unpartitioned table
        newFiles =
            UnpartitionedDataFileWriter.builderFor(swiftLakeEngine, table, inputDataSql)
                .columns(columns)
                .skipDataSorting(skipDataSorting)
                .build()
                .write();
        insertDataSql = inputDataSql;
      }
      // Return null if no new files were created
      if (newFiles == null || newFiles.isEmpty()) {
        return null;
      }

      // Check if this is an overwrite operation
      Expression overwriteByFilter = getOverwriteByFilter(insertDataSql);
      if (overwriteByFilter != null) {
        // Create an overwrite transaction
        return Transaction.builderForOverwrite(swiftLakeEngine, table, overwriteByFilter)
            .newDataFiles(newFiles)
            .branch(branch)
            .snapshotMetadata(snapshotMetadata)
            .isolationLevel(isolationLevel)
            .build();
      } else {
        // Create a regular insert transaction
        return Transaction.builderFor(swiftLakeEngine, table)
            .newDataFiles(newFiles)
            .branch(branch)
            .snapshotMetadata(snapshotMetadata)
            .isolationLevel(isolationLevel)
            .build();
      }
    } finally {
      try {
        // Clean up temporary files and directories
        WriteUtil.closeInputFiles(sourceSqlTmpFiles);
        FileUtils.deleteDirectory(new File(tmpDir));
      } catch (IOException e) {
        LOGGER.warn("Unable to delete the tmp directory {}", tmpDir);
      }
    }
  }

  /**
   * Retrieves the SQL statement for the insert operation.
   *
   * @return The SQL statement
   */
  private String getSql() {
    if (sql != null && !sql.isEmpty()) {
      return sql;
    }
    return WriteUtil.getSql(sqlSessionFactory, mybatisStatementId, mybatisStatementParameter);
  }

  /**
   * Retrieves the overwrite filter expression.
   *
   * @param sourceTable The source table name
   * @return The overwrite filter expression
   */
  private Expression getOverwriteByFilter(String sourceTable) {
    if (overwriteByFilter != null) {
      return overwriteByFilter;
    }

    if (overwriteByFilterSql != null) {
      return swiftLakeEngine
          .getSqlQueryProcessor()
          .parseConditionExpression(overwriteByFilterSql, table.schema(), null);
    } else if (overwriteByFilterColumns != null && !overwriteByFilterColumns.isEmpty()) {
      return WriteUtil.getEqualityConditionExpression(
          commonDao, sourceTable, table, overwriteByFilterColumns);
    }

    return null;
  }

  /**
   * Creates a new SetInputDataSql instance for inserting into the specified table.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param tableName The name of the table to insert into
   * @return A new SetInputDataSql instance
   */
  public static SetInputDataSql into(SwiftLakeEngine swiftLakeEngine, String tableName) {
    return new BuilderImpl(swiftLakeEngine, swiftLakeEngine.getTable(tableName, true));
  }

  /**
   * Creates a new SetInputDataSql instance for inserting into the specified table.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param table The table to insert into
   * @return A new SetInputDataSql instance
   */
  public static SetInputDataSql into(SwiftLakeEngine swiftLakeEngine, Table table) {
    return new BuilderImpl(swiftLakeEngine, table);
  }

  /**
   * Creates a new SetInputDataSql instance for inserting into the specified table batch
   * transaction.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param tableBatchTransaction The table batch transaction
   * @return A new SetInputDataSql instance
   */
  public static SetInputDataSql into(
      SwiftLakeEngine swiftLakeEngine, TableBatchTransaction tableBatchTransaction) {
    return new BuilderImpl(swiftLakeEngine, tableBatchTransaction);
  }

  /**
   * Creates a new SetOverwriteByFilter instance for overwriting the specified table.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param tableName The name of the table to overwrite
   * @return A new SetOverwriteByFilter instance
   */
  public static SetOverwriteByFilter overwrite(SwiftLakeEngine swiftLakeEngine, String tableName) {
    return new BuilderImpl(swiftLakeEngine, swiftLakeEngine.getTable(tableName, true));
  }

  /**
   * Creates a new SetOverwriteByFilter instance for overwriting the specified table.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param table The table to overwrite
   * @return A new SetOverwriteByFilter instance
   */
  public static SetOverwriteByFilter overwrite(SwiftLakeEngine swiftLakeEngine, Table table) {
    return new BuilderImpl(swiftLakeEngine, table);
  }

  public interface SetOverwriteByFilter {
    /**
     * Sets an overwrite condition using an Expression.
     *
     * @param condition The Expression to use as the overwrite condition
     * @return SetInputDataSql instance for further configuration
     */
    SetInputDataSql overwriteByFilter(Expression condition);

    /**
     * Sets an overwrite condition using a SQL string.
     *
     * @param conditionSql The SQL string to use as the overwrite condition
     * @return SetInputDataSql instance for further configuration
     */
    SetInputDataSql overwriteByFilterSql(String conditionSql);

    /**
     * Sets columns to create an overwrite filter based on distinct values from input data.
     *
     * @param columns List of column names to be used for overwrite filtering
     * @return SetInputDataSql instance for further configuration
     */
    SetInputDataSql overwriteByFilterColumns(List<String> columns);
  }

  public interface SetInputDataSql {
    /**
     * Sets the SQL query to be executed.
     *
     * @param sql The SQL query string
     * @return Builder instance for further configuration
     */
    Builder sql(String sql);

    /**
     * Sets a MyBatis statement to be executed.
     *
     * @param id The ID of the MyBatis statement
     * @return Builder instance for further configuration
     */
    Builder mybatisStatement(String id);

    /**
     * Sets a MyBatis statement with parameters to be executed.
     *
     * @param id The ID of the MyBatis statement
     * @param parameter The parameter object for the MyBatis statement
     * @return Builder instance for further configuration
     */
    Builder mybatisStatement(String id, Object parameter);
  }

  public interface Builder {
    /**
     * Sets the columns to be inserted.
     *
     * @param columns List of column names
     * @return Builder instance for further configuration
     */
    Builder columns(List<String> columns);

    /**
     * Sets the SwiftLakeSqlSessionFactory to be used.
     *
     * @param sqlSessionFactory The SwiftLakeSqlSessionFactory instance
     * @return Builder instance for further configuration
     */
    Builder sqlSessionFactory(SwiftLakeSqlSessionFactory sqlSessionFactory);

    /**
     * Sets whether to execute the SQL once only.
     *
     * @param executeSqlOnceOnly True to execute SQL once only, false otherwise
     * @return Builder instance for further configuration
     */
    Builder executeSqlOnceOnly(boolean executeSqlOnceOnly);

    /**
     * Sets whether to skip data sorting.
     *
     * @param skipDataSorting True to skip data sorting, false otherwise
     * @return Builder instance for further configuration
     */
    Builder skipDataSorting(boolean skipDataSorting);

    /**
     * Sets the branch to be used.
     *
     * @param branch The branch name
     * @return Builder instance for further configuration
     */
    Builder branch(String branch);

    /**
     * Sets the snapshot metadata.
     *
     * @param snapshotMetadata Map of snapshot metadata
     * @return Builder instance for further configuration
     */
    Builder snapshotMetadata(Map<String, String> snapshotMetadata);

    /**
     * Sets the isolation level.
     *
     * @param isolationLevel The IsolationLevel to be used
     * @return Builder instance for further configuration
     */
    Builder isolationLevel(IsolationLevel isolationLevel);

    /**
     * Sets whether to process source tables.
     *
     * @param processSourceTables True to process source tables, false otherwise
     * @return Builder instance for further configuration
     */
    Builder processSourceTables(boolean processSourceTables);

    /**
     * Executes the insert operation and returns commit metrics.
     *
     * @return CommitMetrics object containing metrics of the operation
     */
    CommitMetrics execute();
  }

  /** Implementation of the Builder interface for constructing Insert operations. */
  public static class BuilderImpl implements SetInputDataSql, Builder, SetOverwriteByFilter {
    private final SwiftLakeEngine swiftLakeEngine;
    private final Table table;
    private String sql;
    private String mybatisStatementId;
    private Object mybatisStatementParameter;
    private boolean processSourceTables;
    private SwiftLakeSqlSessionFactory sqlSessionFactory;
    private boolean executeSqlOnceOnly;
    private TableBatchTransaction tableBatchTransaction;
    private boolean skipDataSorting;
    private List<String> columns;
    private Expression overwriteByFilter;
    private String overwriteByFilterSql;
    private List<String> overwriteByFilterColumns;
    private String branch;
    private Map<String, String> snapshotMetadata;
    private IsolationLevel isolationLevel;

    /**
     * Constructor for BuilderImpl.
     *
     * @param swiftLakeEngine The SwiftLakeEngine instance
     * @param table The table to insert into
     */
    private BuilderImpl(SwiftLakeEngine swiftLakeEngine, Table table) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = table;
      this.sqlSessionFactory = swiftLakeEngine.getSqlSessionFactory();
      this.processSourceTables = swiftLakeEngine.getProcessTablesDefaultValue();
    }

    /**
     * Constructor for BuilderImpl with TableBatchTransaction.
     *
     * @param swiftLakeEngine The SwiftLakeEngine instance
     * @param tableBatchTransaction The table batch transaction
     */
    private BuilderImpl(
        SwiftLakeEngine swiftLakeEngine, TableBatchTransaction tableBatchTransaction) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = tableBatchTransaction.getTable();
      this.sqlSessionFactory = swiftLakeEngine.getSqlSessionFactory();
      this.tableBatchTransaction = tableBatchTransaction;
      this.processSourceTables = swiftLakeEngine.getProcessTablesDefaultValue();
    }

    @Override
    public Builder sql(String sql) {
      this.sql = sql;
      return this;
    }

    @Override
    public Builder mybatisStatement(String id) {
      this.mybatisStatementId = id;
      return this;
    }

    @Override
    public Builder mybatisStatement(String id, Object parameter) {
      this.mybatisStatementId = id;
      this.mybatisStatementParameter = parameter;
      return this;
    }

    @Override
    public Builder processSourceTables(boolean processSourceTables) {
      this.processSourceTables = processSourceTables;
      return this;
    }

    @Override
    public Builder columns(List<String> columns) {
      this.columns =
          columns == null ? null : columns.stream().distinct().collect(Collectors.toList());
      return this;
    }

    @Override
    public Builder sqlSessionFactory(SwiftLakeSqlSessionFactory sqlSessionFactory) {
      this.sqlSessionFactory = sqlSessionFactory;
      return this;
    }

    @Override
    public Builder executeSqlOnceOnly(boolean executeSqlOnceOnly) {
      this.executeSqlOnceOnly = executeSqlOnceOnly;
      return this;
    }

    @Override
    public Builder skipDataSorting(boolean skipDataSorting) {
      this.skipDataSorting = skipDataSorting;
      return this;
    }

    @Override
    public SetInputDataSql overwriteByFilter(Expression condition) {
      this.overwriteByFilter = condition;
      return this;
    }

    @Override
    public SetInputDataSql overwriteByFilterSql(String conditionSql) {
      this.overwriteByFilterSql = conditionSql;
      return this;
    }

    @Override
    public SetInputDataSql overwriteByFilterColumns(List<String> columns) {
      this.overwriteByFilterColumns =
          columns == null ? null : columns.stream().distinct().collect(Collectors.toList());
      return this;
    }

    @Override
    public Builder branch(String branch) {
      ValidationException.check(
          branch == null || tableBatchTransaction == null,
          "Set branch name on the batch transaction.");
      this.branch = branch;
      return this;
    }

    @Override
    public Builder snapshotMetadata(Map<String, String> snapshotMetadata) {
      ValidationException.check(
          snapshotMetadata == null || tableBatchTransaction == null,
          "Set snapshot metadata on the batch transaction.");
      this.snapshotMetadata =
          snapshotMetadata == null ? null : ImmutableMap.copyOf(snapshotMetadata);
      return this;
    }

    @Override
    public Builder isolationLevel(IsolationLevel isolationLevel) {
      ValidationException.check(
          isolationLevel == null || tableBatchTransaction == null,
          "Set isolation level on the batch transaction.");
      this.isolationLevel = isolationLevel;
      return this;
    }

    @Override
    public CommitMetrics execute() {
      Insert insert =
          new Insert(
              swiftLakeEngine,
              table,
              sql,
              mybatisStatementId,
              mybatisStatementParameter,
              sqlSessionFactory,
              executeSqlOnceOnly,
              tableBatchTransaction,
              skipDataSorting,
              columns,
              overwriteByFilter,
              overwriteByFilterSql,
              overwriteByFilterColumns,
              branch,
              snapshotMetadata,
              isolationLevel,
              processSourceTables);
      return insert.execute();
    }
  }
}
