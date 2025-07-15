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
import com.arcesium.swiftlake.common.ArrayStructLike;
import com.arcesium.swiftlake.common.DataFile;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.expressions.ResolvedExpression;
import com.arcesium.swiftlake.metrics.CommitMetrics;
import com.arcesium.swiftlake.metrics.MetricCollector;
import com.arcesium.swiftlake.metrics.PartitionCommitMetrics;
import com.arcesium.swiftlake.metrics.PartitionCommitMetricsCounter;
import com.arcesium.swiftlake.metrics.PartitionData;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents a transaction for table operations in SwiftLake. */
public class Transaction {
  private static final Logger LOGGER = LoggerFactory.getLogger(Transaction.class);
  private final Table table;
  private final boolean overwrite;
  private final Long fromSnapshotId;
  private final ResolvedExpression conflictDetectionFilter;
  private final List<DataFile> newDataFiles;
  private final List<org.apache.iceberg.DataFile> deletedDataFiles;
  private final ResolvedExpression overwriteByRowFilter;
  private final String applicationId;
  private final String branch;
  private final Map<String, String> snapshotMetadata;
  private final IsolationLevel isolationLevel;
  private final MetricCollector metricCollector;

  private Transaction(
      Table table,
      boolean overwrite,
      Long fromSnapshotId,
      ResolvedExpression conflictDetectionFilter,
      List<DataFile> newDataFiles,
      List<org.apache.iceberg.DataFile> deletedDataFiles,
      ResolvedExpression overwriteByRowFilter,
      String applicationId,
      String branch,
      Map<String, String> snapshotMetadata,
      IsolationLevel isolationLevel,
      MetricCollector metricCollector) {
    this.table = table;
    this.overwrite = overwrite;
    this.fromSnapshotId = fromSnapshotId;
    this.conflictDetectionFilter = conflictDetectionFilter;
    this.newDataFiles =
        newDataFiles == null
            ? null
            : newDataFiles.stream()
                .filter(d -> d.getIcebergDataFile().recordCount() > 0)
                .collect(Collectors.toList());
    this.deletedDataFiles = deletedDataFiles;
    this.overwriteByRowFilter = overwriteByRowFilter;
    this.applicationId = applicationId;
    this.branch = branch;
    this.snapshotMetadata = snapshotMetadata;
    if (isolationLevel != null) {
      this.isolationLevel = isolationLevel;
    } else {
      this.isolationLevel = IsolationLevel.SERIALIZABLE;
    }
    this.metricCollector = metricCollector;
  }

  /**
   * Gets the table associated with this transaction.
   *
   * @return The table object.
   */
  public Table getTable() {
    return table;
  }

  /**
   * Checks if this transaction is an overwrite operation.
   *
   * @return true if it's an overwrite operation, false otherwise.
   */
  public boolean isOverwrite() {
    return overwrite;
  }

  /**
   * Gets the snapshot ID from which this transaction started.
   *
   * @return The starting snapshot ID.
   */
  public Long getFromSnapshotId() {
    return fromSnapshotId;
  }

  /**
   * Gets the conflict detection filter expression.
   *
   * @return The conflict detection filter expression.
   */
  public ResolvedExpression getConflictDetectionFilter() {
    return conflictDetectionFilter;
  }

  /**
   * Gets the list of new data files to be added in this transaction.
   *
   * @return The list of new data files.
   */
  public List<DataFile> getNewDataFiles() {
    return newDataFiles;
  }

  /**
   * Gets the list of data files to be deleted in this transaction.
   *
   * @return The list of data files to be deleted.
   */
  public List<org.apache.iceberg.DataFile> getDeletedDataFiles() {
    return deletedDataFiles;
  }

  /**
   * Gets the filter expression for overwriting by row.
   *
   * @return The overwrite by row filter expression.
   */
  public ResolvedExpression getOverwriteByRowFilter() {
    return overwriteByRowFilter;
  }

  /**
   * Gets the application ID associated with this transaction.
   *
   * @return The application ID.
   */
  public String getApplicationId() {
    return applicationId;
  }

  /**
   * Gets the branch name associated with this transaction.
   *
   * @return The branch name.
   */
  public String getBranch() {
    return branch;
  }

  /**
   * Gets the snapshot metadata associated with this transaction.
   *
   * @return A map of snapshot metadata.
   */
  public Map<String, String> getSnapshotMetadata() {
    return snapshotMetadata;
  }

  /**
   * Commits the transaction to the table.
   *
   * @return CommitMetrics containing information about the commit operation
   */
  public CommitMetrics commit() {
    if ((deletedDataFiles == null || deletedDataFiles.isEmpty())
        && (newDataFiles == null || newDataFiles.isEmpty())) {
      LOGGER.info("Nothing to commit.");
      return new CommitMetrics(table.name());
    }

    if (overwrite) {
      return commitOverwriteOperation();
    } else {
      ValidationException.check(
          deletedDataFiles == null || deletedDataFiles.isEmpty(),
          "Files cannot be deleted in append operation.");

      return commitAppendOperation();
    }
  }

  /**
   * Commits an append operation to the Iceberg table.
   *
   * @return CommitMetrics containing information about the commit operation
   */
  private CommitMetrics commitAppendOperation() {
    List<org.apache.iceberg.DataFile> ibDataFilesToAdd =
        newDataFiles.stream().map(d -> d.getIcebergDataFile()).collect(Collectors.toList());
    // Create a new append operation
    AppendFiles appendFiles = table.newAppend();
    ibDataFilesToAdd.forEach(appendFiles::appendFile);
    String commitMessage = String.format("append with %d new data files", ibDataFilesToAdd.size());
    long duration = commitOperation(table, appendFiles, commitMessage);
    return reportCommitMetrics(newDataFiles, null, duration);
  }

  /**
   * Commits an overwrite operation to the Iceberg table.
   *
   * @return CommitMetrics containing information about the commit operation
   */
  private CommitMetrics commitOverwriteOperation() {
    OverwriteFiles overwriteFiles = table.newOverwrite();
    int deletedCount = 0;
    int addedCount = 0;

    // Delete files if specified
    if (deletedDataFiles != null) {
      deletedDataFiles.forEach(overwriteFiles::deleteFile);
      deletedCount = deletedDataFiles.size();
    }

    // Add new files if specified
    if (newDataFiles != null) {
      List<org.apache.iceberg.DataFile> dataFilesToAdd =
          newDataFiles.stream().map(d -> d.getIcebergDataFile()).collect(Collectors.toList());

      dataFilesToAdd.forEach(overwriteFiles::addFile);
      addedCount = dataFilesToAdd.size();
    }

    // Set scan snapshot ID if provided
    long scanSnapshotId = -1;
    if (fromSnapshotId != null) {
      scanSnapshotId = fromSnapshotId;
      overwriteFiles.validateFromSnapshot(scanSnapshotId);
    }

    String commitMsg = null;
    // Handle overwrite by row filter
    if (overwriteByRowFilter != null) {
      overwriteFiles.overwriteByRowFilter(overwriteByRowFilter.getExpression());
      overwriteFiles.validateAddedFilesMatchOverwriteFilter();
      if (isolationLevel == IsolationLevel.SERIALIZABLE) {
        overwriteFiles.validateNoConflictingData();
        overwriteFiles.validateNoConflictingDeletes();
      } else if (isolationLevel == IsolationLevel.SNAPSHOT) {
        overwriteFiles.validateNoConflictingDeletes();
      }
      commitMsg =
          String.format(
              "overwrite by filter %s with %d new data files", overwriteByRowFilter, addedCount);
    }

    // Handle conflict detection filter
    if (conflictDetectionFilter != null) {
      overwriteFiles.conflictDetectionFilter(conflictDetectionFilter.getExpression());
      if (isolationLevel == IsolationLevel.SERIALIZABLE) {
        overwriteFiles.validateNoConflictingData();
        overwriteFiles.validateNoConflictingDeletes();
      } else if (isolationLevel == IsolationLevel.SNAPSHOT) {
        overwriteFiles.validateNoConflictingDeletes();
      }

      commitMsg =
          String.format(
              "overwrite of %d data files with %d new data files, scanSnapshotId: %d, conflictDetectionFilter: %s",
              deletedCount, addedCount, scanSnapshotId, conflictDetectionFilter);
    }

    long duration = commitOperation(table, overwriteFiles, commitMsg);
    return reportCommitMetrics(newDataFiles, deletedDataFiles, duration);
  }

  /**
   * Commits the given operation to the Iceberg table.
   *
   * @param table The Iceberg table
   * @param operation The snapshot update operation to commit
   * @param description A description of the operation for logging
   * @return The duration of the commit operation in milliseconds
   * @throws CommitStateUnknownException if the commit state is unknown
   */
  private long commitOperation(Table table, SnapshotUpdate<?> operation, String description) {
    LOGGER.info("Committing {} to table {}", description, table);
    // Set application ID if provided
    if (applicationId != null) {
      operation.set("swift-lake.app.id", applicationId);
    }

    // Set snapshot metadata if provided
    if (snapshotMetadata != null && !snapshotMetadata.isEmpty()) {
      snapshotMetadata.forEach(operation::set);
    }

    // Set branch if provided
    if (branch != null) {
      operation.toBranch(branch);
    }

    try {
      long start = System.currentTimeMillis();
      operation.commit();
      long duration = System.currentTimeMillis() - start;
      LOGGER.info("Committed in {} ms", duration);
      return duration;
    } catch (CommitStateUnknownException ex) {
      throw ex;
    }
  }

  private CommitMetrics reportCommitMetrics(
      List<DataFile> addedFiles,
      List<org.apache.iceberg.DataFile> removedFiles,
      long commitTimeInMillis) {
    try {
      int addedFilesCount = 0;
      long addedRecords = 0;
      if (addedFiles != null) {
        addedFilesCount = addedFiles.size();
        addedRecords =
            addedFiles.stream()
                .collect(Collectors.summingLong(f -> f.getIcebergDataFile().recordCount()));
      }
      int removedFilesCount = 0;
      long removedRecords = 0;

      if (removedFiles != null) {
        removedFilesCount = removedFiles.size();
        removedRecords =
            removedFiles.stream().collect(Collectors.summingLong(f -> f.recordCount()));
      }
      List<PartitionCommitMetrics> partitionCommitMetrics = null;
      PartitionSpec spec = table.spec();
      if (spec.isPartitioned()) {
        partitionCommitMetrics = getPartitionCommitMetrics(addedFiles, removedFiles, spec, table);
      }

      CommitMetrics metrics =
          new CommitMetrics(
              table.name(),
              partitionCommitMetrics,
              Duration.ofMillis(commitTimeInMillis),
              addedFilesCount,
              removedFilesCount,
              addedRecords,
              removedRecords);
      if (metricCollector != null) {
        metricCollector.collectMetrics(metrics);
      }
      return metrics;
    } catch (Exception ex) {
      LOGGER.error("Exception occurred while collecting commit metrics.", ex);
      return new CommitMetrics(
          table.name(), null, Duration.ofMillis(commitTimeInMillis), 0, 0, 0, 0);
    }
  }

  private List<PartitionCommitMetrics> getPartitionCommitMetrics(
      List<DataFile> addedFiles,
      List<org.apache.iceberg.DataFile> removedFiles,
      PartitionSpec spec,
      Table table) {
    Map<Pair<ArrayStructLike, Integer>, PartitionCommitMetricsCounter> partitionStatsMap =
        new HashMap<>();
    if (addedFiles != null) {
      addedFiles.forEach(
          f -> {
            ArrayStructLike partitionData = (ArrayStructLike) f.getPartitionData();
            Pair<ArrayStructLike, Integer> key = Pair.of(partitionData, spec.specId());
            if (!partitionStatsMap.containsKey(key)) {
              partitionStatsMap.put(key, new PartitionCommitMetricsCounter());
            }
            PartitionCommitMetricsCounter partitionCommitMetrics = partitionStatsMap.get(key);
            partitionCommitMetrics.increaseAddedFilesCount(1);
            partitionCommitMetrics.increaseAddedRecordsCount(f.getIcebergDataFile().recordCount());
          });
    }

    if (removedFiles != null) {
      removedFiles.forEach(
          f -> {
            ArrayStructLike partitionData = new ArrayStructLike(f.partition());
            Pair<ArrayStructLike, Integer> key = Pair.of(partitionData, f.specId());
            if (!partitionStatsMap.containsKey(key)) {
              partitionStatsMap.put(key, new PartitionCommitMetricsCounter());
            }
            PartitionCommitMetricsCounter partitionCommitMetrics = partitionStatsMap.get(key);
            partitionCommitMetrics.increaseRemovedFilesCount(1);
            partitionCommitMetrics.increaseRemovedRecordsCount(f.recordCount());
          });
    }

    List<PartitionField> partitionFields = spec.fields();
    List<PartitionCommitMetrics> partitionStats = new ArrayList<>();
    Map<Integer, List<PartitionField>> fieldsMap = new HashMap<>();
    Map<Integer, PartitionSpec> specMap = table.specs();
    partitionStatsMap
        .entrySet()
        .forEach(
            e -> {
              List<Pair<PartitionField, Object>> partitionValues = new ArrayList<>();
              List<PartitionField> fields = null;
              Integer specId = null;
              if (e.getKey().getRight() == null) {
                fields = partitionFields;
                specId = spec.specId();
              } else {
                if (!fieldsMap.containsKey(e.getKey().getRight())) {
                  fieldsMap.put(e.getKey().getRight(), specMap.get(e.getKey().getRight()).fields());
                }
                fields = fieldsMap.get(e.getKey().getRight());
                specId = e.getKey().getRight();
              }
              for (int i = 0; i < fields.size(); i++) {
                partitionValues.add(
                    Pair.of(fields.get(i), e.getKey().getLeft().get(i, Object.class)));
              }

              e.getValue().setPartitionData(new PartitionData(specId, partitionValues));
              partitionStats.add(e.getValue().getPartitionCommitMetrics());
            });

    return partitionStats;
  }

  /**
   * Creates a new Builder instance for a transaction on the specified table.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance to use for the transaction
   * @param table The table on which the transaction will be performed
   * @return A new Builder instance
   */
  public static Builder builderFor(SwiftLakeEngine swiftLakeEngine, Table table) {
    return new Builder(swiftLakeEngine, table);
  }

  /**
   * Creates a new Builder instance for an overwrite transaction on the specified table scan.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance to use for the transaction
   * @param tableScan The TableScan object representing the data to be overwritten
   * @return A new Builder instance
   */
  public static Builder builderForOverwrite(SwiftLakeEngine swiftLakeEngine, TableScan tableScan) {
    return new Builder(swiftLakeEngine, tableScan);
  }

  /**
   * Creates a new Builder instance for an overwrite transaction on the specified table with a
   * filter.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance to use for the transaction
   * @param table The table on which the overwrite transaction will be performed
   * @param overwriteByFilter The filter expression to determine which data to overwrite
   * @return A new Builder instance
   */
  public static Builder builderForOverwrite(
      SwiftLakeEngine swiftLakeEngine, Table table, Expression overwriteByFilter) {
    return new Builder(swiftLakeEngine, table, overwriteByFilter);
  }

  /**
   * Creates a new Builder instance with advanced configuration options.
   *
   * @param table The table on which the transaction will be performed
   * @param overwrite Whether this is an overwrite transaction
   * @param fromSnapshotId The ID of the snapshot to start the transaction from
   * @param conflictDetectionFilter The filter expression for conflict detection
   * @param applicationId The ID of the application performing the transaction
   * @param metricCollector The MetricCollector to use for gathering metrics
   * @return A new Builder instance
   */
  static Builder builderFor(
      Table table,
      boolean overwrite,
      Long fromSnapshotId,
      Expression conflictDetectionFilter,
      String applicationId,
      MetricCollector metricCollector) {
    return new Builder(
        table, overwrite, fromSnapshotId, conflictDetectionFilter, applicationId, metricCollector);
  }

  /** A builder class for creating Transaction objects. */
  public static class Builder {
    private final Table table;
    private boolean overwrite;
    private Long fromSnapshotId;
    private ResolvedExpression conflictDetectionFilter;
    private List<DataFile> newDataFiles;
    private List<org.apache.iceberg.DataFile> deletedDataFiles;
    private ResolvedExpression overwriteByRowFilter;
    private String applicationId;
    private String branch;
    private Map<String, String> snapshotMetadata;
    private IsolationLevel isolationLevel;
    private final MetricCollector metricCollector;

    /**
     * Creates a new Builder instance for the given SwiftLakeEngine and Table.
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
     * Creates a new Builder instance for the given SwiftLakeEngine and TableScan.
     *
     * @param swiftLakeEngine The SwiftLakeEngine instance.
     * @param tableScan The TableScan instance.
     */
    private Builder(SwiftLakeEngine swiftLakeEngine, TableScan tableScan) {
      this.overwrite = true;
      Snapshot snapshot = tableScan.snapshot();
      if (snapshot != null) this.fromSnapshotId = snapshot.snapshotId();
      this.conflictDetectionFilter = Expressions.resolved(tableScan.filter());
      this.table = tableScan.table();
      this.applicationId = swiftLakeEngine.getApplicationId();
      this.metricCollector = swiftLakeEngine.getMetricCollector();
    }

    /**
     * Creates a new Builder instance for the given SwiftLakeEngine, Table, and overwrite filter.
     *
     * @param swiftLakeEngine The SwiftLakeEngine instance.
     * @param table The Table instance.
     * @param overwriteByFilter The Expression to use as an overwrite filter.
     */
    private Builder(SwiftLakeEngine swiftLakeEngine, Table table, Expression overwriteByFilter) {
      this.overwrite = true;
      this.table = table;
      Snapshot snapshot = table.currentSnapshot();
      if (snapshot != null) this.fromSnapshotId = snapshot.snapshotId();
      if (overwriteByFilter != null) {
        this.overwriteByRowFilter = Expressions.resolveExpression(overwriteByFilter);
      }
      this.applicationId = swiftLakeEngine.getApplicationId();
      this.metricCollector = swiftLakeEngine.getMetricCollector();
    }

    /**
     * Creates a new Builder instance with the given parameters.
     *
     * @param table The Table instance.
     * @param overwrite Whether to overwrite existing data.
     * @param fromSnapshotId The ID of the snapshot to start from.
     * @param conflictDetectionFilter The Expression to use for conflict detection.
     * @param applicationId The ID of the application.
     * @param metricCollector The MetricCollector instance.
     */
    private Builder(
        Table table,
        boolean overwrite,
        Long fromSnapshotId,
        Expression conflictDetectionFilter,
        String applicationId,
        MetricCollector metricCollector) {
      this.table = table;
      this.overwrite = overwrite;
      this.fromSnapshotId = fromSnapshotId;
      if (conflictDetectionFilter != null) {
        this.conflictDetectionFilter = Expressions.resolveExpression(conflictDetectionFilter);
      }
      this.applicationId = applicationId;
      this.metricCollector = metricCollector;
    }

    /**
     * Sets the new data files for the transaction.
     *
     * @param newDataFiles A list of DataFile objects representing new data.
     * @return This Builder instance.
     */
    public Builder newDataFiles(List<DataFile> newDataFiles) {
      this.newDataFiles = newDataFiles;
      return this;
    }

    /**
     * Sets the data files to be deleted in the transaction.
     *
     * @param deletedDataFiles A list of DataFile objects to be deleted.
     * @return This Builder instance.
     */
    public Builder deletedDataFiles(List<org.apache.iceberg.DataFile> deletedDataFiles) {
      this.deletedDataFiles = deletedDataFiles;
      return this;
    }

    /**
     * Sets the branch for the transaction.
     *
     * @param branch The branch name.
     * @return This Builder instance.
     */
    public Builder branch(String branch) {
      this.branch = branch;
      return this;
    }

    /**
     * Sets the snapshot metadata for the transaction.
     *
     * @param snapshotMetadata A map of snapshot metadata.
     * @return This Builder instance.
     */
    public Builder snapshotMetadata(Map<String, String> snapshotMetadata) {
      this.snapshotMetadata =
          snapshotMetadata == null ? null : ImmutableMap.copyOf(snapshotMetadata);
      return this;
    }

    /**
     * Sets the conflict detection filter for the transaction.
     *
     * @param conflictDetectionFilter The Expression to use for conflict detection.
     * @return This Builder instance.
     */
    public Builder conflictDetectionFilter(Expression conflictDetectionFilter) {
      if (conflictDetectionFilter != null) {
        this.conflictDetectionFilter = Expressions.resolveExpression(conflictDetectionFilter);
      }
      return this;
    }

    /**
     * Sets the isolation level for the transaction.
     *
     * @param isolationLevel The IsolationLevel to use.
     * @return This Builder instance.
     */
    public Builder isolationLevel(IsolationLevel isolationLevel) {
      this.isolationLevel = isolationLevel;
      return this;
    }

    /**
     * Builds and returns a new Transaction instance with the configured parameters.
     *
     * @return A new Transaction instance.
     */
    public Transaction build() {
      return new Transaction(
          table,
          overwrite,
          fromSnapshotId,
          conflictDetectionFilter,
          newDataFiles,
          deletedDataFiles,
          overwriteByRowFilter,
          applicationId,
          branch,
          snapshotMetadata,
          isolationLevel,
          metricCollector);
    }
  }
}
