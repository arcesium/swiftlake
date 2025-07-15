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
package com.arcesium.swiftlake.writer;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.DataFile;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.metrics.CommitMetrics;
import com.arcesium.swiftlake.metrics.MetricCollector;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages a batch of transactions for a single Iceberg table. */
public class TableBatchTransaction {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableBatchTransaction.class);
  private final Table table;
  private final String applicationId;
  private final String branch;
  private final Map<String, String> snapshotMetadata;
  private final IsolationLevel isolationLevel;
  private Boolean overwrite;
  private List<Transaction> transactions;
  private Set<String> deletedFiles;
  private Long fromSnapshotId;
  private final MetricCollector metricCollector;

  /**
   * Constructs a new TableBatchTransaction.
   *
   * @param table The Table object associated with this transaction.
   * @param applicationId The ID of the application initiating this transaction.
   * @param branch The branch name for this transaction.
   * @param snapshotMetadata A map containing metadata for the snapshot.
   * @param isolationLevel The isolation level for this transaction.
   * @param metricCollector The MetricCollector used for collecting metrics during the transaction.
   */
  private TableBatchTransaction(
      Table table,
      String applicationId,
      String branch,
      Map<String, String> snapshotMetadata,
      IsolationLevel isolationLevel,
      MetricCollector metricCollector) {
    this.table = table;
    this.applicationId = applicationId;
    this.branch = branch;
    this.snapshotMetadata = snapshotMetadata;
    this.isolationLevel = isolationLevel;
    this.metricCollector = metricCollector;
  }

  /**
   * Gets the Iceberg table associated with this batch transaction.
   *
   * @return The Iceberg table
   */
  public Table getTable() {
    return table;
  }

  /**
   * Adds a transaction to the batch.
   *
   * @param transaction The transaction to add
   * @throws ValidationException if the transaction is incompatible with the batch
   */
  public synchronized void add(Transaction transaction) {
    // Validate that the transaction is for the same table
    ValidationException.check(
        transaction.getTable().location().equals(table.location()),
        "Cannot batch transactions of different tables (%s, %s).",
        table.location(),
        transaction.getTable().location());
    // Initialize batch properties if this is the first transaction
    if (transactions == null) {
      transactions = new ArrayList<>();
      overwrite = transaction.isOverwrite();
      deletedFiles = new HashSet<>();
      fromSnapshotId = transaction.getFromSnapshotId();
    } else {
      // Ensure all transactions in the batch are of the same type (append or overwrite)
      ValidationException.check(
          overwrite.equals(transaction.isOverwrite()),
          "Cannot mix the append transactions and overwrite transactions in one batch transaction.");

      if (overwrite) {
        validateOverwriteTransaction(transaction);
      }
    }

    // Add deleted files to the set
    if (transaction.getDeletedDataFiles() != null) {
      deletedFiles.addAll(
          transaction.getDeletedDataFiles().stream()
              .map(d -> d.location().toString())
              .collect(Collectors.toList()));
    }

    transactions.add(transaction);
  }

  /**
   * Commits all transactions in the batch.
   *
   * @return CommitMetrics for the batch commit
   */
  public CommitMetrics commit() {
    if (transactions == null || transactions.isEmpty()) return new CommitMetrics(table.name());

    if (overwrite) {
      return commitOverwriteTransactions(transactions);
    } else {
      return commitAppendTransactions(transactions);
    }
  }

  /**
   * Validates an overwrite transaction before adding it to the batch.
   *
   * @param transaction The transaction to validate
   * @throws ValidationException if the transaction is invalid
   */
  private void validateOverwriteTransaction(Transaction transaction) {
    // Check if fromSnapshotId is consistent across transactions
    ValidationException.check(
        (fromSnapshotId == null && transaction.getFromSnapshotId() == null)
            || (fromSnapshotId != null
                && transaction.getFromSnapshotId() != null
                && fromSnapshotId.equals(transaction.getFromSnapshotId())),
        "fromSnapshotId is not same %s:%s",
        fromSnapshotId,
        transaction.getFromSnapshotId());

    // Ensure no duplicate conflict detection filters
    transactions.forEach(
        tx ->
            ValidationException.check(
                !ExpressionUtil.equivalent(
                    tx.getConflictDetectionFilter().getExpression(),
                    transaction.getConflictDetectionFilter().getExpression(),
                    tx.getTable().schema().asStruct(),
                    true),
                "A transaction with same conflict detection filter exists already: %s",
                transaction.getConflictDetectionFilter()));

    // Check for conflicting files
    PartitionSpec spec = transaction.getTable().spec();
    InclusiveMetricsEvaluator metricsEvaluator =
        new InclusiveMetricsEvaluator(
            transaction.getTable().schema(),
            transaction.getConflictDetectionFilter().getExpression());

    Evaluator evaluator =
        new Evaluator(
            spec.partitionType(),
            Projections.inclusive(spec)
                .project(transaction.getConflictDetectionFilter().getExpression()));

    transactions.forEach(
        tx -> {
          if (tx.getNewDataFiles() != null && !tx.getNewDataFiles().isEmpty()) {
            tx.getNewDataFiles()
                .forEach(
                    d ->
                        ValidationException.check(
                            !(evaluator.eval(d.getIcebergDataFile().partition())
                                && metricsEvaluator.eval(d.getIcebergDataFile())),
                            "Found conflicting files that can contain records matching %s: %s",
                            transaction.getConflictDetectionFilter(),
                            d.getIcebergDataFile().location()));
          }
        });

    // Check for duplicate file deletions
    if (transaction.getDeletedDataFiles() != null
        && !transaction.getDeletedDataFiles().isEmpty()
        && deletedFiles != null
        && !deletedFiles.isEmpty()) {
      transaction
          .getDeletedDataFiles()
          .forEach(
              d ->
                  ValidationException.check(
                      !deletedFiles.contains(d.location().toString()),
                      "Data file %s is already deleted as part of another transaction",
                      d.location()));
    }
  }

  /**
   * Commits a batch of append transactions.
   *
   * @param transactions List of transactions to commit
   * @return CommitMetrics for the batch commit
   */
  private CommitMetrics commitAppendTransactions(List<Transaction> transactions) {
    if (transactions.isEmpty()) {
      return new CommitMetrics(table.name());
    }

    // Collect all new data files from transactions
    List<DataFile> newDataFiles =
        transactions.stream()
            .filter(tx -> tx.getNewDataFiles() != null)
            .map(tx -> tx.getNewDataFiles())
            .flatMap(List::stream)
            .collect(Collectors.toList());

    // Create and commit a single transaction for all new data files
    CommitMetrics metrics =
        Transaction.builderFor(table, false, null, null, applicationId, metricCollector)
            .newDataFiles(newDataFiles)
            .branch(branch)
            .snapshotMetadata(snapshotMetadata)
            .isolationLevel(isolationLevel)
            .build()
            .commit();
    LOGGER.info("BatchCommit: Committed {} transactions in one batch.", transactions.size());
    return metrics;
  }

  /**
   * Commits a batch of overwrite transactions.
   *
   * @param transactions List of transactions to commit
   * @return CommitMetrics for the batch commit
   */
  private CommitMetrics commitOverwriteTransactions(List<Transaction> transactions) {
    if (transactions.isEmpty()) {
      return new CommitMetrics(table.name());
    }

    // Collect all deleted data files from transactions
    List<org.apache.iceberg.DataFile> deletedDataFiles =
        transactions.stream()
            .filter(tx -> tx.getDeletedDataFiles() != null)
            .map(tx -> tx.getDeletedDataFiles())
            .flatMap(List::stream)
            .collect(Collectors.toList());

    // Collect all new data files from transactions
    List<DataFile> newDataFiles =
        transactions.stream()
            .filter(tx -> tx.getNewDataFiles() != null)
            .map(tx -> tx.getNewDataFiles())
            .flatMap(List::stream)
            .collect(Collectors.toList());

    // Combine all conflict detection filters using OR expression
    Expression conflictDetectionFilter =
        Expressions.resolved(
            transactions.stream()
                .map(tx -> tx.getConflictDetectionFilter().getExpression())
                .reduce(
                    org.apache.iceberg.expressions.Expressions.alwaysFalse(),
                    (left, right) -> org.apache.iceberg.expressions.Expressions.or(left, right)));

    // Create and commit a single transaction for all changes
    CommitMetrics metrics =
        Transaction.builderFor(
                table,
                true,
                fromSnapshotId,
                conflictDetectionFilter,
                applicationId,
                metricCollector)
            .newDataFiles(newDataFiles)
            .deletedDataFiles(deletedDataFiles)
            .branch(branch)
            .snapshotMetadata(snapshotMetadata)
            .isolationLevel(isolationLevel)
            .build()
            .commit();

    LOGGER.info("BatchCommit: Committed {} transactions in one batch.", transactions.size());
    return metrics;
  }

  /**
   * Returns a string representation of this TableBatchTransaction.
   *
   * @return A string containing the table, applicationId, branch, and snapshotMetadata.
   */
  @Override
  public String toString() {
    return "TableBatchTransaction{"
        + "table="
        + table
        + ", applicationId='"
        + applicationId
        + '\''
        + ", branch='"
        + branch
        + '\''
        + ", snapshotMetadata="
        + snapshotMetadata
        + '}';
  }

  /**
   * Creates a new Builder for a TableBatchTransaction with the specified SwiftLakeEngine and table
   * name.
   *
   * @param swiftLakeEngine The SwiftLakeEngine to use.
   * @param tableName The name of the table.
   * @return A new Builder instance.
   */
  public static Builder builderFor(SwiftLakeEngine swiftLakeEngine, String tableName) {
    return new Builder(swiftLakeEngine, swiftLakeEngine.getTable(tableName, true));
  }

  /**
   * Creates a new Builder for a TableBatchTransaction with the specified SwiftLakeEngine and Table.
   *
   * @param swiftLakeEngine The SwiftLakeEngine to use.
   * @param table The Table instance.
   * @return A new Builder instance.
   */
  public static Builder builderFor(SwiftLakeEngine swiftLakeEngine, Table table) {
    return new Builder(swiftLakeEngine, table);
  }

  /** Builder class for creating TableBatchTransaction instances. */
  public static class Builder {
    private final Table table;
    private String applicationId;
    private String branch;
    private Map<String, String> snapshotMetadata;
    private IsolationLevel isolationLevel;
    private final MetricCollector metricCollector;

    /**
     * Constructs a new Builder instance.
     *
     * @param swiftLakeEngine The SwiftLakeEngine instance.
     * @param table The Table instance.
     */
    private Builder(SwiftLakeEngine swiftLakeEngine, Table table) {
      this.table = table;
      this.applicationId = swiftLakeEngine.getApplicationId();
      this.metricCollector = swiftLakeEngine.getMetricCollector();
    }

    /**
     * Sets the branch for the TableBatchTransaction.
     *
     * @param branch The branch name.
     * @return This Builder instance.
     */
    public Builder branch(String branch) {
      this.branch = branch;
      return this;
    }

    /**
     * Sets the snapshot metadata for the TableBatchTransaction.
     *
     * @param snapshotMetadata A map containing snapshot metadata.
     * @return This Builder instance.
     */
    public Builder snapshotMetadata(Map<String, String> snapshotMetadata) {
      this.snapshotMetadata =
          snapshotMetadata == null ? null : ImmutableMap.copyOf(snapshotMetadata);
      return this;
    }

    /**
     * Sets the isolation level for the TableBatchTransaction.
     *
     * @param isolationLevel The IsolationLevel to be set.
     * @return This Builder instance.
     */
    public Builder isolationLevel(IsolationLevel isolationLevel) {
      this.isolationLevel = isolationLevel;
      return this;
    }

    /**
     * Builds and returns a new TableBatchTransaction instance.
     *
     * @return A new TableBatchTransaction instance.
     */
    public TableBatchTransaction build() {
      return new TableBatchTransaction(
          table, applicationId, branch, snapshotMetadata, isolationLevel, metricCollector);
    }
  }
}
