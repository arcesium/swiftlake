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
import com.arcesium.swiftlake.common.FileUtil;
import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.dao.CommonDao;
import com.arcesium.swiftlake.dao.SCD1MergeDao;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.metrics.CommitMetrics;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import com.arcesium.swiftlake.sql.SqlQueryProcessor;
import com.arcesium.swiftlake.sql.TableScanResult;
import com.arcesium.swiftlake.writer.BaseDataFileWriter;
import com.arcesium.swiftlake.writer.PartitionedDataFileWriter;
import com.arcesium.swiftlake.writer.TableBatchTransaction;
import com.arcesium.swiftlake.writer.Transaction;
import com.arcesium.swiftlake.writer.UnpartitionedDataFileWriter;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements Slowly Changing Dimension Type 1 (SCD1) merge operation for data warehousing. */
public class SCD1Merge {
  private static final Logger LOGGER = LoggerFactory.getLogger(SCD1Merge.class);
  private final SwiftLakeEngine swiftLakeEngine;
  private final Table table;
  private final SCD1MergeDao scd1MergeDao;
  private final CommonDao commonDao;
  private final SCD1MergeProperties properties;
  private final TableBatchTransaction tableBatchTransaction;
  private final SqlQueryProcessor sqlQueryProcessor;
  private final String branch;
  private final Map<String, String> snapshotMetadata;
  private final IsolationLevel isolationLevel;

  /**
   * Constructs a new SCD1Merge instance.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param table The Table to perform the SCD1 merge on.
   * @param properties The SCD1MergeProperties containing merge configuration.
   * @param tableBatchTransaction The TableBatchTransaction for managing the merge operation.
   * @param branch The branch name for the merge operation.
   * @param snapshotMetadata A map containing snapshot metadata.
   * @param isolationLevel The IsolationLevel for the merge operation. If null, it will be
   *     determined from table properties.
   */
  private SCD1Merge(
      SwiftLakeEngine swiftLakeEngine,
      Table table,
      SCD1MergeProperties properties,
      TableBatchTransaction tableBatchTransaction,
      String branch,
      Map<String, String> snapshotMetadata,
      IsolationLevel isolationLevel) {
    this.swiftLakeEngine = swiftLakeEngine;
    this.table = table;
    this.commonDao = swiftLakeEngine.getCommonDao();
    this.scd1MergeDao = new SCD1MergeDao(swiftLakeEngine);
    this.properties = properties;
    this.tableBatchTransaction = tableBatchTransaction;
    this.properties.setCompression(BaseDataFileWriter.PARQUET_COMPRESSION_DEFAULT);
    this.sqlQueryProcessor = swiftLakeEngine.getSqlQueryProcessor();
    this.branch = branch;
    this.snapshotMetadata = snapshotMetadata;
    if (isolationLevel == null) {
      isolationLevel =
          IsolationLevel.fromName(
              PropertyUtil.propertyAsString(
                  table.properties(),
                  TableProperties.MERGE_ISOLATION_LEVEL,
                  TableProperties.MERGE_ISOLATION_LEVEL_DEFAULT));
    }
    this.isolationLevel = isolationLevel;
  }

  /**
   * Executes the SCD1 merge operation.
   *
   * @return CommitMetrics containing information about the merge operation
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
   * Executes the SCD1 merge operation without committing the transaction.
   *
   * @return A Transaction object representing the merge operation, or null if no changes were made
   */
  private Transaction executeWithoutCommit() {
    // Validate columns and set up properties
    WriteUtil.validateColumns(table, properties.getColumns());
    properties.setAllColumns(WriteUtil.getColumns(table));
    WriteUtil.validateColumns(properties.getAllColumns(), properties.getKeyColumns(), null, null);
    WriteUtil.validateTableFilterColumns(
        properties.getTableFilterColumns(), properties.getKeyColumns());

    // Create temporary directory for merge operation
    String tmpDir = swiftLakeEngine.getLocalDir() + "/scd1_merge/" + UUID.randomUUID().toString();
    new File(tmpDir).mkdirs();

    TableScanResult swiftLakeTableScanResult = null;
    List<InputFiles> sourceSqlTmpFiles = null;
    try {
      // Set up extra columns if operation type column is specified
      List<String> extraColumnsInInput = null;
      if (properties.getOperationTypeColumn() != null) {
        extraColumnsInInput = Arrays.asList(properties.getOperationTypeColumn());
      }

      // Process source tables if required
      String inputDataSql = this.getInputDataSql();
      if (properties.isProcessSourceTables()) {
        Pair<String, List<InputFiles>> result =
            WriteUtil.processSourceTables(swiftLakeEngine, inputDataSql, table);
        inputDataSql = result.getLeft();
        sourceSqlTmpFiles = result.getRight();
      }
      List<String> inputFilePaths = null;
      String sourceTableName = null;

      // Handle source SQL execution
      if (properties.isExecuteSourceSqlOnceOnly()) {
        // Write source data to local files
        inputFilePaths =
            UnpartitionedDataFileWriter.builderFor(swiftLakeEngine, table, inputDataSql)
                .columns(properties.getColumns())
                .tmpDir(tmpDir)
                .skipDataSorting(true)
                .targetFileSizeBytes(null)
                .additionalColumns(extraColumnsInInput)
                .writeToPerThreadParquetFile(true)
                .build()
                .createLocalDataFiles();

        WriteUtil.closeInputFiles(sourceSqlTmpFiles);
        sourceSqlTmpFiles = null;

        sourceTableName =
            commonDao.getDataSqlFromParquetFiles(
                inputFilePaths, null, true, false, true, false, true);
      } else {
        // Create a subquery for source data
        sourceTableName =
            "(SELECT *, 1 AS file_row_number FROM "
                + WriteUtil.getSqlWithProjection(
                    swiftLakeEngine,
                    table.schema(),
                    inputDataSql,
                    extraColumnsInInput,
                    true,
                    properties.getColumns())
                + ")";
      }

      // Check if source is empty
      if (commonDao.isTableEmpty(sourceTableName)) {
        return null;
      }

      sourceTableName = this.createTableFilter(sourceTableName);
      Expression tableFilter = properties.getTableFilter();
      // Scan target table
      swiftLakeTableScanResult =
          swiftLakeEngine
              .getIcebergScanExecutor()
              .executeTableScan(table, tableFilter, true, true, null, branch, null, false);
      List<DataFile> matchedDataFiles = swiftLakeTableScanResult.getScanResult().getRight();
      boolean isAppendOnly = matchedDataFiles.isEmpty();
      String destinationTableName = swiftLakeTableScanResult.getSql();

      PartitionSpec partitionSpec = table.spec();
      boolean isPartitionedTable = partitionSpec.isPartitioned();

      List<String> modifiedFiles = null;
      properties.setSourceTableName(sourceTableName);
      properties.setAppendOnly(isAppendOnly);

      // Handle non-append-only scenario
      if (!isAppendOnly) {
        String diffsFilePath = tmpDir + "/diffs_" + UUID.randomUUID().toString();
        String modifiedFileNamesFilePath =
            tmpDir + "/modified_files_" + UUID.randomUUID().toString() + ".parquet";
        properties.setDestinationTableName(destinationTableName);
        properties.setDiffsFilePath("'" + diffsFilePath + "'");
        String boundaryCondition =
            swiftLakeEngine.getSchemaEvolution().getDuckDBFilterSql(table, tableFilter);
        properties.setBoundaryCondition(boundaryCondition);

        scd1MergeDao.mergeFindDiffs(properties);

        boolean emptyDiffs = FileUtil.isEmptyFolder(diffsFilePath);
        if (!emptyDiffs) {
          properties.setDiffsFilePath(getDiffsTableName(diffsFilePath));
          properties.setModifiedFileNamesFilePath(modifiedFileNamesFilePath);
          scd1MergeDao.saveDistinctFileNames(properties);

          WriteUtil.checkMergeCardinality(commonDao, properties.getDiffsFilePath());

          modifiedFiles = scd1MergeDao.getFileNames(modifiedFileNamesFilePath);
        } else {
          properties.setDiffsFilePath(null);
        }
      }

      // Generate upserts SQL
      String upsertsSql = scd1MergeDao.getMergeUpsertsSql(properties);

      List<com.arcesium.swiftlake.common.DataFile> newFiles;
      if (isPartitionedTable) {
        // Handle partitioned table
        String modifiedFilesSql =
            swiftLakeEngine
                .getSchemaEvolution()
                .getSelectSQLForDataFiles(table, modifiedFiles, false, false);

        String partitionDataSql =
            "(SELECT * FROM "
                + sourceTableName
                + " UNION ALL BY NAME SELECT * FROM "
                + modifiedFilesSql
                + ")";

        newFiles =
            PartitionedDataFileWriter.builderFor(
                    swiftLakeEngine, table, partitionDataSql, upsertsSql)
                .skipDataSorting(properties.isSkipDataSorting())
                .build()
                .write();
      } else {
        // Handle unpartitioned table
        newFiles =
            UnpartitionedDataFileWriter.builderFor(swiftLakeEngine, table, upsertsSql)
                .skipDataSorting(properties.isSkipDataSorting())
                .build()
                .write();
      }

      // Get overwritten files
      List<DataFile> overwrittenFiles =
          WriteUtil.getModifiedIcebergDataFiles(swiftLakeTableScanResult, modifiedFiles);

      // Return null if no changes were made
      if (overwrittenFiles.isEmpty() && newFiles.isEmpty()) return null;

      // Build and return the transaction
      return Transaction.builderForOverwrite(
              swiftLakeEngine, swiftLakeTableScanResult.getScanResult().getLeft())
          .deletedDataFiles(overwrittenFiles)
          .newDataFiles(newFiles)
          .branch(branch)
          .snapshotMetadata(snapshotMetadata)
          .isolationLevel(isolationLevel)
          .build();

    } finally {
      // Clean up resources
      if (swiftLakeTableScanResult != null) {
        swiftLakeTableScanResult.close();
      }
      WriteUtil.closeInputFiles(sourceSqlTmpFiles);
      try {
        FileUtils.deleteDirectory(new File(tmpDir));
      } catch (IOException e) {
        LOGGER.warn("Unable to delete the tmp directory {}", tmpDir);
      }
    }
  }

  private String getDiffsTableName(String diffsFilePath) {
    return "read_parquet(['"
        + diffsFilePath
        + "/*/*.parquet"
        + "'], hive_partitioning=1, union_by_name=True)";
  }

  /**
   * Creates a file filter for the source table based on the provided properties.
   *
   * @param sourceTable The name of the source table
   * @return The filtered source table name
   */
  private String createTableFilter(String sourceTable) {
    boolean addFilterToSource = true;
    if (properties.getTableFilter() == null) {
      if (properties.getTableFilterSql() != null) {
        properties.setTableFilter(
            sqlQueryProcessor.parseConditionExpression(
                properties.getTableFilterSql(), table.schema(), null));
      } else if (properties.getTableFilterColumns() != null
          && !properties.getTableFilterColumns().isEmpty()) {
        com.arcesium.swiftlake.expressions.Expression expr =
            WriteUtil.getEqualityConditionExpression(
                commonDao, sourceTable, table, properties.getTableFilterColumns());
        properties.setTableFilter(expr);
        addFilterToSource = false;
      } else {
        throw new ValidationException("Table filter is mandatory.");
      }
    }

    properties.setTableFilter(Expressions.resolveExpression(properties.getTableFilter()));

    if (addFilterToSource) {
      String boundaryCondition =
          swiftLakeEngine
              .getSchemaEvolution()
              .getDuckDBFilterSql(table, properties.getTableFilter());

      sourceTable = "(SELECT * FROM " + sourceTable + " WHERE " + boundaryCondition + ")";
    }

    return sourceTable;
  }

  /**
   * Retrieves the input data SQL based on the provided properties.
   *
   * @return The SQL string for input data
   */
  private String getInputDataSql() {
    if (properties.getSql() != null && !properties.getSql().isEmpty()) {
      return properties.getSql();
    }
    return WriteUtil.getSql(
        properties.getSqlSessionFactory(),
        properties.getMybatisStatementId(),
        properties.getMybatisStatementParameter());
  }

  /**
   * Applies changes as SCD1 (Slowly Changing Dimension Type 1) merge operation.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param tableName The name of the table to apply changes to
   * @return A SetTableFilter instance for further configuration
   */
  public static SetTableFilter applyChanges(SwiftLakeEngine swiftLakeEngine, String tableName) {
    return new BuilderImpl(swiftLakeEngine, swiftLakeEngine.getTable(tableName, true));
  }

  /**
   * Applies changes as SCD1 (Slowly Changing Dimension Type 1) merge operation.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param table The Table instance to apply changes to
   * @return A SetTableFilter instance for further configuration
   */
  public static SetTableFilter applyChanges(SwiftLakeEngine swiftLakeEngine, Table table) {
    return new BuilderImpl(swiftLakeEngine, table);
  }

  /**
   * Applies changes as SCD1 (Slowly Changing Dimension Type 1) merge operation.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param tableBatchTransaction The TableBatchTransaction instance to apply changes to
   * @return A SetTableFilter instance for further configuration
   */
  public static SetTableFilter applyChanges(
      SwiftLakeEngine swiftLakeEngine, TableBatchTransaction tableBatchTransaction) {
    return new BuilderImpl(swiftLakeEngine, tableBatchTransaction);
  }

  public interface SetTableFilter {
    /**
     * Sets a table filter using an Expression condition.
     *
     * @param condition The Expression to use as a filter condition
     * @return SetSourceData interface for further configuration
     */
    SetSourceData tableFilter(Expression condition);

    /**
     * Sets a table filter using a SQL condition string.
     *
     * @param conditionSql The SQL string to use as a filter condition
     * @return SetSourceData interface for further configuration
     */
    SetSourceData tableFilterSql(String conditionSql);

    /**
     * Sets columns to create a table filter based on distinct values from input data.
     *
     * @param columns The list of column names to use for the filter creation
     * @return SetSourceData interface for further configuration
     */
    SetSourceData tableFilterColumns(List<String> columns);
  }

  public interface SetSourceData {
    /**
     * Sets the source SQL for the merge operation.
     *
     * @param sql The SQL string to use as the source
     * @return SetKeyColumns interface for further configuration
     */
    SetKeyColumns sourceSql(String sql);

    /**
     * Sets the source using a MyBatis statement ID.
     *
     * @param id The ID of the MyBatis statement to use as the source
     * @return SetKeyColumns interface for further configuration
     */
    SetKeyColumns sourceMybatisStatement(String id);

    /**
     * Sets the source using a MyBatis statement ID and a parameter object.
     *
     * @param id The ID of the MyBatis statement to use as the source
     * @param parameter The parameter object to pass to the MyBatis statement
     * @return SetKeyColumns interface for further configuration
     */
    SetKeyColumns sourceMybatisStatement(String id, Object parameter);
  }

  public interface SetKeyColumns {
    /**
     * Sets the key columns for the merge operation.
     *
     * @param keyColumns The list of column names to use as key columns
     * @return Builder interface for further configuration
     */
    Builder keyColumns(List<String> keyColumns);
  }

  public interface Builder {
    /**
     * Sets the columns to be included in the merge operation.
     *
     * @param columns The list of column names to include
     * @return This Builder instance for method chaining
     */
    Builder columns(List<String> columns);

    /**
     * Sets the operation type column and delete operation value.
     *
     * @param column The name of the column indicating the operation type
     * @param deleteOperationValue The value indicating a delete operation
     * @return This Builder instance for method chaining
     */
    Builder operationTypeColumn(String column, String deleteOperationValue);

    /**
     * Sets the SwiftLakeSqlSessionFactory to be used.
     *
     * @param sqlSessionFactory The SwiftLakeSqlSessionFactory instance
     * @return This Builder instance for method chaining
     */
    Builder sqlSessionFactory(SwiftLakeSqlSessionFactory sqlSessionFactory);

    /**
     * Sets whether to skip data sorting.
     *
     * @param skipDataSorting true to skip data sorting, false otherwise
     * @return This Builder instance for method chaining
     */
    Builder skipDataSorting(boolean skipDataSorting);

    /**
     * Sets whether to execute the source SQL only once.
     *
     * @param executeSourceSqlOnceOnly true to execute source SQL once only, false otherwise
     * @return This Builder instance for method chaining
     */
    Builder executeSourceSqlOnceOnly(boolean executeSourceSqlOnceOnly);

    /**
     * Sets the branch name.
     *
     * @param branch The name of the branch
     * @return This Builder instance for method chaining
     */
    Builder branch(String branch);

    /**
     * Sets the snapshot metadata.
     *
     * @param snapshotMetadata A map containing snapshot metadata
     * @return This Builder instance for method chaining
     */
    Builder snapshotMetadata(Map<String, String> snapshotMetadata);

    /**
     * Sets the isolation level for the merge operation.
     *
     * @param isolationLevel The IsolationLevel to be used
     * @return This Builder instance for method chaining
     */
    Builder isolationLevel(IsolationLevel isolationLevel);

    /**
     * Sets whether to process source tables.
     *
     * @param processSourceTables true to process source tables, false otherwise
     * @return This Builder instance for method chaining
     */
    Builder processSourceTables(boolean processSourceTables);

    /**
     * Executes the SCD1Merge operation with the configured settings.
     *
     * @return CommitMetrics containing metrics about the executed merge operation
     */
    CommitMetrics execute();
  }

  /** Implementation of the Builder pattern for SCD1Merge. */
  public static class BuilderImpl implements SetSourceData, SetTableFilter, SetKeyColumns, Builder {
    private final SwiftLakeEngine swiftLakeEngine;
    private final Table table;
    private final SCD1MergeProperties properties;
    private TableBatchTransaction tableBatchTransaction;
    private String branch;
    private Map<String, String> snapshotMetadata;
    private IsolationLevel isolationLevel;

    /**
     * Constructs a new BuilderImpl instance.
     *
     * @param swiftLakeEngine The SwiftLakeEngine instance to be used.
     * @param table The Table instance to be associated with this builder.
     */
    private BuilderImpl(SwiftLakeEngine swiftLakeEngine, Table table) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = table;
      this.properties = new SCD1MergeProperties();
      this.properties.setSqlSessionFactory(swiftLakeEngine.getSqlSessionFactory());
      this.properties.setProcessSourceTables(swiftLakeEngine.getProcessTablesDefaultValue());
    }

    /**
     * Constructs a new BuilderImpl instance with a TableBatchTransaction.
     *
     * @param swiftLakeEngine The SwiftLakeEngine instance to be used.
     * @param tableBatchTransaction The TableBatchTransaction instance to be associated with this
     *     builder.
     */
    private BuilderImpl(
        SwiftLakeEngine swiftLakeEngine, TableBatchTransaction tableBatchTransaction) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = tableBatchTransaction.getTable();
      this.properties = new SCD1MergeProperties();
      this.properties.setSqlSessionFactory(swiftLakeEngine.getSqlSessionFactory());
      this.tableBatchTransaction = tableBatchTransaction;
      this.properties.setProcessSourceTables(swiftLakeEngine.getProcessTablesDefaultValue());
    }

    @Override
    public SetSourceData tableFilter(Expression filter) {
      properties.setTableFilter(filter);
      return this;
    }

    @Override
    public SetSourceData tableFilterSql(String conditionSql) {
      properties.setTableFilterSql(conditionSql);
      return this;
    }

    @Override
    public SetSourceData tableFilterColumns(List<String> columns) {
      properties.setTableFilterColumns(
          columns == null ? null : columns.stream().distinct().collect(Collectors.toList()));
      return this;
    }

    @Override
    public SetKeyColumns sourceSql(String sql) {
      properties.setSql(sql);
      return this;
    }

    @Override
    public SetKeyColumns sourceMybatisStatement(String id) {
      properties.setMybatisStatementId(id);
      return this;
    }

    @Override
    public SetKeyColumns sourceMybatisStatement(String id, Object parameter) {
      properties.setMybatisStatementId(id);
      properties.setMybatisStatementParameter(parameter);
      return this;
    }

    @Override
    public Builder processSourceTables(boolean processSourceTables) {
      properties.setProcessSourceTables(processSourceTables);
      return this;
    }

    @Override
    public Builder sqlSessionFactory(SwiftLakeSqlSessionFactory sqlSessionFactory) {
      properties.setSqlSessionFactory(sqlSessionFactory);
      return this;
    }

    @Override
    public Builder keyColumns(List<String> keyColumns) {
      properties.setKeyColumns(keyColumns.stream().distinct().collect(Collectors.toList()));
      return this;
    }

    @Override
    public Builder columns(List<String> columns) {
      properties.setColumns(
          columns == null ? null : columns.stream().distinct().collect(Collectors.toList()));
      return this;
    }

    @Override
    public Builder operationTypeColumn(String column, String deleteOperationValue) {
      properties.setOperationTypeColumn(column);
      properties.setDeleteOperationValue(deleteOperationValue);
      return this;
    }

    @Override
    public Builder skipDataSorting(boolean skipDataSorting) {
      properties.setSkipDataSorting(skipDataSorting);
      return this;
    }

    @Override
    public Builder executeSourceSqlOnceOnly(boolean executeSourceSqlOnceOnly) {
      properties.setExecuteSourceSqlOnceOnly(executeSourceSqlOnceOnly);
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
      SCD1Merge merge =
          new SCD1Merge(
              swiftLakeEngine,
              table,
              properties,
              tableBatchTransaction,
              branch,
              snapshotMetadata,
              isolationLevel);
      return merge.execute();
    }
  }
}
