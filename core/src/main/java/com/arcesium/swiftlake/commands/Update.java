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
import com.arcesium.swiftlake.dao.UpdateDao;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.metrics.CommitMetrics;
import com.arcesium.swiftlake.sql.SqlQueryProcessor;
import com.arcesium.swiftlake.sql.TableScanResult;
import com.arcesium.swiftlake.writer.PartitionedDataFileWriter;
import com.arcesium.swiftlake.writer.TableBatchTransaction;
import com.arcesium.swiftlake.writer.Transaction;
import com.arcesium.swiftlake.writer.UnpartitionedDataFileWriter;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents an update operation for SwiftLake tables. */
public class Update {
  private static final Logger LOGGER = LoggerFactory.getLogger(Update.class);
  private final SwiftLakeEngine swiftLakeEngine;
  private final Table table;
  private Expression condition;
  private String conditionSql;
  private final Map<String, Object> updateSets;
  private final UpdateDao updateDao;
  private final TableBatchTransaction tableBatchTransaction;
  private final SqlQueryProcessor sqlQueryProcessor;
  private final boolean skipDataSorting;
  private final String branch;
  private final Map<String, String> snapshotMetadata;
  private final IsolationLevel isolationLevel;

  /**
   * Constructs an Update object with the given parameters.
   *
   * @param swiftLakeEngine The SwiftLake engine instance
   * @param table The table to be updated
   * @param conditionSql The SQL condition for the update
   * @param condition The Expression condition for the update
   * @param updateSets The map of column names to update values
   * @param tableBatchTransaction The batch transaction, if any
   * @param skipDataSorting Whether to skip data sorting
   * @param branch The branch name, if any
   * @param snapshotMetadata The snapshot metadata, if any
   * @param isolationLevel The isolation level for the update
   */
  private Update(
      SwiftLakeEngine swiftLakeEngine,
      Table table,
      String conditionSql,
      Expression condition,
      Map<String, Object> updateSets,
      TableBatchTransaction tableBatchTransaction,
      boolean skipDataSorting,
      String branch,
      Map<String, String> snapshotMetadata,
      IsolationLevel isolationLevel) {
    this.swiftLakeEngine = swiftLakeEngine;
    this.table = table;
    this.condition = condition;
    this.conditionSql = conditionSql;
    this.updateSets = updateSets;
    this.updateDao = new UpdateDao(swiftLakeEngine);
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
                  TableProperties.UPDATE_ISOLATION_LEVEL,
                  TableProperties.UPDATE_ISOLATION_LEVEL_DEFAULT));
    }
    this.isolationLevel = isolationLevel;
  }

  /**
   * Executes the update operation and commits the changes.
   *
   * @return CommitMetrics for the update operation, or null if part of a batch transaction
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
   * Executes the update operation without committing the changes.
   *
   * @return Transaction object representing the update operation, or null if no changes
   */
  private Transaction executeWithoutCommit() {
    TableScanResult swiftLakeTableScanResult = null;
    try {
      // Create condition and execute table scan
      this.createCondition();
      swiftLakeTableScanResult =
          swiftLakeEngine
              .getIcebergScanExecutor()
              .executeTableScan(table, condition, true, false, null, branch, null, false);
      List<DataFile> matchedDataFiles = swiftLakeTableScanResult.getScanResult().getRight();

      // Check if any files match the condition
      if (matchedDataFiles.isEmpty()) {
        LOGGER.info("Nothing matched for the condition ({})", conditionSql);
        return null;
      }

      String destinationTableName = swiftLakeTableScanResult.getSql();

      // Get exactly matching files based on the condition
      List<String> exactlyMatchingFiles =
          updateDao.getMatchingFiles(destinationTableName, conditionSql);
      if (exactlyMatchingFiles.isEmpty()) {
        LOGGER.info("Nothing matched for the condition ({})", conditionSql);
        return null;
      }

      // Generate SQL for selecting data from matching files
      String destinationTableWithMatchingFiles =
          swiftLakeEngine
              .getSchemaEvolution()
              .getSelectSQLForDataFiles(table, exactlyMatchingFiles, false, false);

      boolean isPartitionedTable = table.spec().isPartitioned();

      // Generate update SQL
      String updateSql =
          updateDao.getUpdateSql(
              destinationTableWithMatchingFiles, conditionSql, getUpdateSetsForSql());

      // Write new files based on whether the table is partitioned or not
      List<com.arcesium.swiftlake.common.DataFile> newFiles;
      if (isPartitionedTable) {
        newFiles =
            PartitionedDataFileWriter.builderFor(swiftLakeEngine, table, updateSql, updateSql)
                .skipDataSorting(skipDataSorting)
                .build()
                .write();
      } else {
        newFiles =
            UnpartitionedDataFileWriter.builderFor(swiftLakeEngine, table, updateSql)
                .skipDataSorting(skipDataSorting)
                .build()
                .write();
      }

      // Get list of files to be overwritten
      List<DataFile> overwrittenFiles =
          WriteUtil.getModifiedIcebergDataFiles(swiftLakeTableScanResult, exactlyMatchingFiles);

      // If no files are overwritten or created, return null
      if (overwrittenFiles.isEmpty() && newFiles.isEmpty()) return null;

      // Build and return the transaction for overwriting
      return Transaction.builderForOverwrite(
              swiftLakeEngine, swiftLakeTableScanResult.getScanResult().getLeft())
          .deletedDataFiles(overwrittenFiles)
          .newDataFiles(newFiles)
          .branch(branch)
          .snapshotMetadata(snapshotMetadata)
          .isolationLevel(isolationLevel)
          .build();

    } finally {
      // Close the table scan result if it exists
      if (swiftLakeTableScanResult != null) {
        swiftLakeTableScanResult.close();
      }
    }
  }

  /** Creates the condition for the update operation. */
  private void createCondition() {
    Pair<String, Expression> conditionPair =
        WriteUtil.getCondition(swiftLakeEngine, sqlQueryProcessor, table, condition, conditionSql);
    this.conditionSql = conditionPair.getLeft();
    this.condition = conditionPair.getRight();
  }

  /**
   * Converts update sets to SQL-compatible format.
   *
   * @return Map of column names to their SQL-formatted update values
   */
  private Map<String, String> getUpdateSetsForSql() {
    Schema schema = table.schema();
    Map<String, String> updates =
        updateSets.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> e.getKey(),
                    e -> {
                      Types.NestedField f = schema.findField(e.getKey());
                      ValidationException.check(f != null, "Could not find column %s", e.getKey());
                      return swiftLakeEngine
                          .getSchemaEvolution()
                          .getPrimitiveTypeValueForSql(f.type().asPrimitiveType(), e.getValue());
                    }));
    return updates;
  }

  /**
   * Initiates an update operation on the specified table.
   *
   * @param swiftLakeEngine SwiftLakeEngine instance
   * @param tableName Name of the table to update
   * @return SetCondition object to continue building the update operation
   */
  public static SetCondition on(SwiftLakeEngine swiftLakeEngine, String tableName) {
    return new BuilderImpl(swiftLakeEngine, swiftLakeEngine.getTable(tableName, true));
  }

  /**
   * Initiates an update operation on the specified table.
   *
   * @param swiftLakeEngine SwiftLakeEngine instance
   * @param table Table object to update
   * @return SetCondition object to continue building the update operation
   */
  public static SetCondition on(SwiftLakeEngine swiftLakeEngine, Table table) {
    return new BuilderImpl(swiftLakeEngine, table);
  }

  /**
   * Initiates an update operation as part of a batch transaction.
   *
   * @param swiftLakeEngine SwiftLakeEngine instance
   * @param tableBatchTransaction TableBatchTransaction object
   * @return SetCondition object to continue building the update operation
   */
  public static SetCondition on(
      SwiftLakeEngine swiftLakeEngine, TableBatchTransaction tableBatchTransaction) {
    return new BuilderImpl(swiftLakeEngine, tableBatchTransaction);
  }

  /** Defines methods for setting update conditions. */
  public interface SetCondition {
    /**
     * Sets the condition SQL for the update operation.
     *
     * @param condition The SQL condition string
     * @return The SetUpdateSets interface for further configuration
     */
    SetUpdateSets conditionSql(String condition);

    /**
     * Sets the condition expression for the update operation.
     *
     * @param expression The condition expression
     * @return The SetUpdateSets interface for further configuration
     */
    SetUpdateSets condition(Expression expression);
  }

  /** Defines a method for setting update sets. */
  public interface SetUpdateSets {
    /**
     * Sets the update sets for the update operation.
     *
     * @param updateSets A map of column names to their new values
     * @return The Builder interface for further configuration
     */
    Builder updateSets(Map<String, Object> updateSets);
  }

  /** Defines methods for configuring and executing the update operation further. */
  public interface Builder {
    /**
     * Sets whether to skip data sorting.
     *
     * @param skipDataSorting True to skip data sorting, false otherwise
     * @return This Builder instance
     */
    Builder skipDataSorting(boolean skipDataSorting);

    /**
     * Sets the branch for the update operation.
     *
     * @param branch The branch name
     * @return This Builder instance
     */
    Builder branch(String branch);

    /**
     * Sets the snapshot metadata for the update operation.
     *
     * @param snapshotMetadata A map of snapshot metadata
     * @return This Builder instance
     */
    Builder snapshotMetadata(Map<String, String> snapshotMetadata);

    /**
     * Sets the isolation level for the update operation.
     *
     * @param isolationLevel The isolation level
     * @return This Builder instance
     */
    Builder isolationLevel(IsolationLevel isolationLevel);

    /**
     * Executes the update operation.
     *
     * @return CommitMetrics for the executed update operation
     */
    CommitMetrics execute();
  }

  /** Implementation of the Builder interface for constructing Update operations. */
  public static class BuilderImpl implements SetCondition, SetUpdateSets, Builder {
    private final SwiftLakeEngine swiftLakeEngine;
    private final Table table;
    private Expression condition;
    private String conditionSql;
    private Map<String, Object> updateSets;
    private TableBatchTransaction tableBatchTransaction;
    private boolean skipDataSorting;
    private String branch;
    private Map<String, String> snapshotMetadata;
    private IsolationLevel isolationLevel;

    /**
     * Constructs a BuilderImpl with SwiftLakeEngine and Table.
     *
     * @param swiftLakeEngine The SwiftLake engine instance
     * @param table The table to be updated
     */
    private BuilderImpl(SwiftLakeEngine swiftLakeEngine, Table table) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = table;
    }

    /**
     * Constructs a BuilderImpl with SwiftLakeEngine and TableBatchTransaction.
     *
     * @param swiftLakeEngine The SwiftLake engine instance
     * @param tableBatchTransaction The batch transaction
     */
    private BuilderImpl(
        SwiftLakeEngine swiftLakeEngine, TableBatchTransaction tableBatchTransaction) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = tableBatchTransaction.getTable();
      this.tableBatchTransaction = tableBatchTransaction;
    }

    @Override
    public SetUpdateSets conditionSql(String condition) {
      this.conditionSql = condition;
      return this;
    }

    @Override
    public SetUpdateSets condition(Expression expression) {
      this.condition = expression;
      return this;
    }

    @Override
    public Builder updateSets(Map<String, Object> updateSets) {
      ValidationException.check(
          updateSets != null && !updateSets.isEmpty(), "Update sets cannot be null or empty.");
      this.updateSets = new HashMap<>(updateSets);
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
      Update update =
          new Update(
              swiftLakeEngine,
              table,
              conditionSql,
              condition,
              updateSets,
              tableBatchTransaction,
              skipDataSorting,
              branch,
              snapshotMetadata,
              isolationLevel);
      return update.execute();
    }
  }
}
