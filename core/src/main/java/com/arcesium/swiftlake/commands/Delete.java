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
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.dao.DeleteDao;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.metrics.CommitMetrics;
import com.arcesium.swiftlake.sql.SqlQueryProcessor;
import com.arcesium.swiftlake.sql.TableScanResult;
import com.arcesium.swiftlake.writer.PartitionedDataFileWriter;
import com.arcesium.swiftlake.writer.TableBatchTransaction;
import com.arcesium.swiftlake.writer.Transaction;
import com.arcesium.swiftlake.writer.UnpartitionedDataFileWriter;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class represents Delete operation for SwiftLake tables. */
public class Delete {
  private static final Logger LOGGER = LoggerFactory.getLogger(Delete.class);
  private final SwiftLakeEngine swiftLakeEngine;
  private final Table table;
  private Expression condition;
  private String conditionSql;
  private final DeleteDao deleteDao;
  private final TableBatchTransaction tableBatchTransaction;
  private final SqlQueryProcessor sqlQueryProcessor;
  private final boolean skipDataSorting;
  private final String branch;
  private final Map<String, String> snapshotMetadata;
  private final IsolationLevel isolationLevel;

  /**
   * Constructs a new Delete operation.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param table The table to perform the delete operation on
   * @param conditionSql The SQL condition for the delete operation
   * @param condition The Expression condition for the delete operation
   * @param tableBatchTransaction The TableBatchTransaction, if part of a batch operation
   * @param skipDataSorting Whether to skip data sorting
   * @param branch The branch name for the operation
   * @param snapshotMetadata Metadata for the snapshot
   * @param isolationLevel The isolation level for the delete operation
   */
  private Delete(
      SwiftLakeEngine swiftLakeEngine,
      Table table,
      String conditionSql,
      Expression condition,
      TableBatchTransaction tableBatchTransaction,
      boolean skipDataSorting,
      String branch,
      Map<String, String> snapshotMetadata,
      IsolationLevel isolationLevel) {
    this.swiftLakeEngine = swiftLakeEngine;
    this.table = table;
    this.condition = condition;
    this.conditionSql = conditionSql;
    this.deleteDao = new DeleteDao(swiftLakeEngine);
    this.tableBatchTransaction = tableBatchTransaction;
    this.skipDataSorting = skipDataSorting;
    this.sqlQueryProcessor = swiftLakeEngine.getSqlQueryProcessor();
    this.branch = branch;
    this.snapshotMetadata = snapshotMetadata;
    if (isolationLevel == null) {
      isolationLevel =
          IsolationLevel.fromName(
              PropertyUtil.propertyAsString(
                  table.properties(),
                  TableProperties.DELETE_ISOLATION_LEVEL,
                  TableProperties.DELETE_ISOLATION_LEVEL_DEFAULT));
    }
    this.isolationLevel = isolationLevel;
  }

  /**
   * Executes the delete operation and commits the changes.
   *
   * @return CommitMetrics for the delete operation, or null if part of a batch transaction
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
   * Executes the delete operation without committing the changes.
   *
   * @return Transaction object representing the delete operation, or null if no changes
   */
  private Transaction executeWithoutCommit() {
    TableScanResult swiftLakeTableScanResult = null;
    try {
      // Create and execute table scan
      this.createCondition();
      swiftLakeTableScanResult =
          swiftLakeEngine
              .getIcebergScanExecutor()
              .executeTableScan(table, condition, true, false, null, branch, null, false);
      List<DataFile> matchedDataFiles = swiftLakeTableScanResult.getScanResult().getRight();
      String conditionSql =
          swiftLakeEngine.getSchemaEvolution().getDuckDBFilterSql(table, condition);

      // Check if any files match the condition
      if (matchedDataFiles.isEmpty()) {
        LOGGER.info("Nothing matched for the condition (" + conditionSql + ")");
        return null;
      }

      String destinationTableName = swiftLakeTableScanResult.getSql();

      // Get exactly matching files
      List<String> exactlyMatchingFiles =
          deleteDao.getMatchingFiles(destinationTableName, conditionSql);
      if (exactlyMatchingFiles.isEmpty()) {
        LOGGER.info("Nothing matched for the condition (" + conditionSql + ")");
        return null;
      }

      // Prepare SQL with the matching files
      String destinationTableWithMatchingFiles =
          swiftLakeEngine
              .getSchemaEvolution()
              .getSelectSQLForDataFiles(table, exactlyMatchingFiles, false, false);

      boolean isPartitionedTable = table.spec().isPartitioned();

      // Generate delete SQL
      String deleteSql = deleteDao.getDeleteSql(destinationTableWithMatchingFiles, conditionSql);

      // Write new files based on table partitioning
      List<com.arcesium.swiftlake.common.DataFile> newFiles;
      if (isPartitionedTable) {
        newFiles =
            PartitionedDataFileWriter.builderFor(swiftLakeEngine, table, deleteSql, deleteSql)
                .skipDataSorting(skipDataSorting)
                .build()
                .write();
      } else {
        newFiles =
            UnpartitionedDataFileWriter.builderFor(swiftLakeEngine, table, deleteSql)
                .skipDataSorting(skipDataSorting)
                .build()
                .write();
      }
      // Get list of overwritten files
      List<DataFile> overwrittenFiles =
          WriteUtil.getModifiedIcebergDataFiles(swiftLakeTableScanResult, exactlyMatchingFiles);

      // Get list of overwritten files
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
      // Ensure proper cleanup of resourc
      if (swiftLakeTableScanResult != null) {
        swiftLakeTableScanResult.close();
      }
    }
  }

  /** Creates the condition for the delete operation. */
  private void createCondition() {
    Pair<String, Expression> conditionPair =
        WriteUtil.getCondition(swiftLakeEngine, sqlQueryProcessor, table, condition, conditionSql);
    this.conditionSql = conditionPair.getLeft();
    this.condition = conditionPair.getRight();
  }

  /**
   * Creates a new Delete operation for the specified table.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param tableName The name of the table to delete from
   * @return A SetCondition object to set the delete condition
   */
  public static SetCondition from(SwiftLakeEngine swiftLakeEngine, String tableName) {
    return new BuilderImpl(swiftLakeEngine, swiftLakeEngine.getTable(tableName, true));
  }

  /**
   * Creates a new Delete operation for the specified table.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param table The Table object to delete from
   * @return A SetCondition object to set the delete condition
   */
  public static SetCondition from(SwiftLakeEngine swiftLakeEngine, Table table) {
    return new BuilderImpl(swiftLakeEngine, table);
  }

  /**
   * Creates a new Delete operation as part of a batch transaction.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param tableBatchTransaction The TableBatchTransaction to add this delete operation to
   * @return A SetCondition object to set the delete condition
   */
  public static SetCondition from(
      SwiftLakeEngine swiftLakeEngine, TableBatchTransaction tableBatchTransaction) {
    return new BuilderImpl(swiftLakeEngine, tableBatchTransaction);
  }

  public interface SetCondition {
    /**
     * Sets the SQL condition for the delete operation.
     *
     * @param condition The SQL condition string
     * @return The Builder instance for method chaining
     */
    Builder conditionSql(String condition);

    /**
     * Sets the condition expression for the delete operation.
     *
     * @param expression The condition Expression object
     * @return The Builder instance for method chaining
     */
    Builder condition(Expression expression);
  }

  public interface Builder {
    /**
     * Sets whether to skip data sorting during the delete operation.
     *
     * @param skipDataSorting True to skip data sorting, false otherwise
     * @return The Builder instance for method chaining
     */
    Builder skipDataSorting(boolean skipDataSorting);

    /**
     * Sets the branch for the delete operation.
     *
     * @param branch The branch name
     * @return The Builder instance for method chaining
     */
    Builder branch(String branch);

    /**
     * Sets the snapshot metadata for the delete operation.
     *
     * @param snapshotMetadata A map containing snapshot metadata
     * @return The Builder instance for method chaining
     */
    Builder snapshotMetadata(Map<String, String> snapshotMetadata);

    /**
     * Sets the isolation level for the delete operation.
     *
     * @param isolationLevel The IsolationLevel to be used
     * @return The Builder instance for method chaining
     */
    Builder isolationLevel(IsolationLevel isolationLevel);

    /**
     * Executes the delete operation with the configured settings.
     *
     * @return CommitMetrics object containing metrics about the delete operation
     */
    CommitMetrics execute();
  }

  public static class BuilderImpl implements SetCondition, Builder {
    private final SwiftLakeEngine swiftLakeEngine;
    private final Table table;
    private Expression condition;
    private String conditionSql;
    private TableBatchTransaction tableBatchTransaction;
    private boolean skipDataSorting;
    private String branch;
    private Map<String, String> snapshotMetadata;
    private IsolationLevel isolationLevel;

    /**
     * Constructs a BuilderImpl with a SwiftLakeEngine and Table.
     *
     * @param swiftLakeEngine The SwiftLakeEngine to use
     * @param table The Table to perform delete operations on
     */
    private BuilderImpl(SwiftLakeEngine swiftLakeEngine, Table table) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = table;
    }

    /**
     * Constructs a BuilderImpl with a SwiftLakeEngine and TableBatchTransaction.
     *
     * @param swiftLakeEngine The SwiftLakeEngine to use
     * @param tableBatchTransaction The TableBatchTransaction to use
     */
    private BuilderImpl(
        SwiftLakeEngine swiftLakeEngine, TableBatchTransaction tableBatchTransaction) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = tableBatchTransaction.getTable();
      this.tableBatchTransaction = tableBatchTransaction;
    }

    @Override
    public Builder conditionSql(String condition) {
      this.conditionSql = condition;
      return this;
    }

    @Override
    public Builder condition(Expression expression) {
      this.condition = expression;
      return this;
    }

    @Override
    public Builder skipDataSorting(boolean skipDataSorting) {
      this.skipDataSorting = skipDataSorting;
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
      Delete delete =
          new Delete(
              swiftLakeEngine,
              table,
              conditionSql,
              condition,
              tableBatchTransaction,
              skipDataSorting,
              branch,
              snapshotMetadata,
              isolationLevel);
      return delete.execute();
    }
  }
}
