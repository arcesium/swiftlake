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
import com.arcesium.swiftlake.dao.SCD2MergeDao;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.io.SwiftLakeFileIO;
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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles SCD2 (Slowly Changing Dimension Type 2) merge operations. */
public class SCD2Merge {
  private static final Logger LOGGER = LoggerFactory.getLogger(SCD2Merge.class);
  private final SwiftLakeEngine swiftLakeEngine;
  private final Table table;
  private final SCD2MergeDao scd2MergeDao;
  private final CommonDao commonDao;
  private final SCD2MergeProperties properties;
  private final TableBatchTransaction tableBatchTransaction;
  private final SqlQueryProcessor sqlQueryProcessor;
  private final String branch;
  private final Map<String, String> snapshotMetadata;
  private final IsolationLevel isolationLevel;

  /**
   * Constructor for SCD2Merge.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param table The target table for the merge operation.
   * @param properties The SCD2MergeProperties containing merge configuration.
   * @param tableBatchTransaction The TableBatchTransaction instance, if any.
   * @param branch The branch name for the merge operation.
   * @param snapshotMetadata The snapshot metadata for the merge operation.
   * @param isolationLevel The isolation level for the merge operation.
   */
  private SCD2Merge(
      SwiftLakeEngine swiftLakeEngine,
      Table table,
      SCD2MergeProperties properties,
      TableBatchTransaction tableBatchTransaction,
      String branch,
      Map<String, String> snapshotMetadata,
      IsolationLevel isolationLevel) {
    this.swiftLakeEngine = swiftLakeEngine;
    this.table = table;
    this.commonDao = swiftLakeEngine.getCommonDao();
    this.scd2MergeDao = new SCD2MergeDao(swiftLakeEngine);
    this.properties = properties;
    this.tableBatchTransaction = tableBatchTransaction;
    this.properties.setCompression(BaseDataFileWriter.PARQUET_COMPRESSION_DEFAULT);
    this.sqlQueryProcessor = swiftLakeEngine.getSqlQueryProcessor();
    this.branch = branch;
    this.snapshotMetadata = snapshotMetadata;
    this.isolationLevel = isolationLevel;
  }

  /**
   * Executes the SCD2 merge operation.
   *
   * @return CommitMetrics containing the results of the merge operation
   */
  public CommitMetrics execute() {
    Transaction tx = executeWithoutCommit();
    if (tx == null) {
      return new CommitMetrics(table.name());
    }
    if (tableBatchTransaction != null) {
      tableBatchTransaction.add(tx);
      return null;
    } else {
      return tx.commit();
    }
  }

  /**
   * Executes the SCD2 merge operation without committing the transaction. This method handles both
   * CHANGES and SNAPSHOT modes for SCD2 merging.
   *
   * @return A Transaction object representing the merge operation, or null if no changes were made
   */
  private Transaction executeWithoutCommit() {
    // Validate columns and set up effective period columns
    WriteUtil.validateColumns(table, properties.getColumns());
    List<String> allColumnsIncludingEffectivePeriod = WriteUtil.getColumns(table);
    Set<String> effectivePeriodColumns = new HashSet<>();
    effectivePeriodColumns.add(properties.getEffectiveStartColumn());
    effectivePeriodColumns.add(properties.getEffectiveEndColumn());
    if (properties.getCurrentFlagColumn() != null) {
      effectivePeriodColumns.add(properties.getCurrentFlagColumn());
    }
    // Set all columns excluding effective period columns
    properties.setAllColumns(
        allColumnsIncludingEffectivePeriod.stream()
            .filter(c -> !effectivePeriodColumns.contains(c))
            .collect(Collectors.toList()));
    // Set change tracking columns if not provided
    if (properties.getChangeTrackingColumns() == null
        || properties.getChangeTrackingColumns().isEmpty()) {
      properties.setChangeTrackingColumns(
          WriteUtil.getRemainingColumns(properties.getAllColumns(), properties.getKeyColumns()));
    }
    // Validate columns and table filter
    WriteUtil.validateColumns(
        properties.getAllColumns(),
        properties.getKeyColumns(),
        properties.getChangeTrackingColumns(),
        properties.getChangeTrackingMetadataMap());
    WriteUtil.validateTableFilterColumns(
        properties.getTableFilterColumns(), properties.getKeyColumns());
    Schema schema = table.schema();
    validateEffectivePeriodColumns(schema);
    processChangeTrackingMetadata(schema);

    // Create temporary directory for merge operation
    String tmpDir = swiftLakeEngine.getLocalDir() + "/scd2_merge/" + UUID.randomUUID().toString();
    new File(tmpDir).mkdirs();

    // Set up paths for diffs and modified files
    String diffsFileBaseFolder = "/diffs_" + UUID.randomUUID().toString();
    String diffsFilePath = tmpDir + diffsFileBaseFolder;
    String modifiedFileNamesFilePath =
        tmpDir + "/modified_files_" + UUID.randomUUID().toString() + ".parquet";
    TableScanResult swiftLakeTableScanResult = null;
    List<InputFiles> sourceSqlTmpFiles = null;
    try {
      // Generate effective timestamp if needed
      if (properties.getEffectiveTimestamp() == null && properties.isGenerateEffectiveTimestamp()) {
        properties.setEffectiveTimestamp(
            DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                LocalDateTime.now(ZoneId.systemDefault())));
      }

      // Prepare schema for input data
      Schema schemaForInputData =
          new Schema(
              schema.asStruct().fields().stream()
                  .filter(
                      f ->
                          !f.name().equals(properties.getEffectiveStartColumn())
                              && !f.name().equals(properties.getEffectiveEndColumn())
                              && (properties.getCurrentFlagColumn() == null
                                  || !f.name().equals(properties.getCurrentFlagColumn())))
                  .collect(Collectors.toList()));

      List<String> extraColumnsInInput = null;
      if (properties.getMode() == SCD2MergeMode.CHANGES) {
        extraColumnsInInput = Arrays.asList(properties.getOperationTypeColumn());
      }

      String inputDataSql = this.getInputDataSql(properties);
      // Process source tables if needed
      if (properties.isProcessSourceTables()) {
        Pair<String, List<InputFiles>> result =
            WriteUtil.processSourceTables(swiftLakeEngine, inputDataSql, table);
        inputDataSql = result.getLeft();
        sourceSqlTmpFiles = result.getRight();
      }

      List<String> inputFilePaths = null;
      String sourceTableName = null;
      // Execute source SQL and prepare source table name
      if (properties.isExecuteSourceSqlOnceOnly()) {
        inputFilePaths =
            UnpartitionedDataFileWriter.builderFor(swiftLakeEngine, table, inputDataSql)
                .columns(properties.getColumns())
                .tmpDir(tmpDir)
                .schema(schemaForInputData)
                .skipDataSorting(true)
                .targetFileSizeBytes(null)
                .additionalColumns(extraColumnsInInput)
                .writeToPerThreadParquetFile(true)
                .build()
                .createLocalDataFiles();

        WriteUtil.closeInputFiles(sourceSqlTmpFiles);
        sourceSqlTmpFiles = null;

        if (!inputFilePaths.isEmpty()) {
          sourceTableName =
              commonDao.getDataSqlFromParquetFiles(
                  inputFilePaths, null, true, false, true, false, true);
        }
      } else {
        sourceTableName =
            "(SELECT *, 1 AS file_row_number FROM "
                + WriteUtil.getSqlWithProjection(
                    swiftLakeEngine,
                    schemaForInputData,
                    inputDataSql,
                    extraColumnsInInput,
                    true,
                    properties.getColumns())
                + ")";
      }

      // Check if source is empty
      if (properties.isSkipEmptySource()) {
        if (sourceTableName == null || commonDao.isTableEmpty(sourceTableName)) {
          LOGGER.info("Empty data source.");
          return null;
        }
      }

      // Apply table filter
      sourceTableName = this.createTableFilter(sourceTableName);
      // Set up file filter and conflict detection filter
      Expression tableFilter = properties.getTableFilter();
      Expression conflictDetectionFilter = tableFilter;
      tableFilter = appendEffectivePeriodFilter(tableFilter, properties);
      tableFilter = Expressions.resolveExpression(tableFilter);

      // Scan target table
      swiftLakeTableScanResult =
          swiftLakeEngine
              .getIcebergScanExecutor()
              .executeTableScan(table, tableFilter, true, true, null, branch, null, false);
      List<DataFile> matchedDataFiles = swiftLakeTableScanResult.getScanResult().getRight();
      boolean isAppendOnly = matchedDataFiles.isEmpty();
      properties.setAppendOnly(isAppendOnly);

      String destinationTableName = swiftLakeTableScanResult.getSql();

      // Set up properties for merge operation
      String boundaryCondition =
          swiftLakeEngine.getSchemaEvolution().getDuckDBFilterSql(table, tableFilter);
      List<String> modifiedFiles = null;
      String upsertsSql = null;
      properties.setSourceTableName(sourceTableName);
      properties.setBoundaryCondition(boundaryCondition);
      setTimestampStrings(properties, schema);

      if (!isAppendOnly) {
        properties.setDestinationTableName(destinationTableName);
        if (scd2MergeDao.verifyNoOutOfOrderRecords(properties)) {
          throw new ValidationException(
              "SCD2 merge failed: Out-of-order records detected. Found records in destination table '%s' with %s or %s greater than or equal to the current merge timestamp (%s).",
              table.name(),
              properties.getEffectiveStartColumn(),
              properties.getEffectiveEndColumn(),
              properties.getEffectiveTimestamp());
        }
      }

      SwiftLakeFileIO fileIO = (SwiftLakeFileIO) table.io();
      // Perform merge operation based on mode (CHANGES or SNAPSHOT)
      if (properties.getMode() == SCD2MergeMode.CHANGES) {
        if (!isAppendOnly) {
          properties.setDiffsFilePath("'" + diffsFilePath + "'");

          scd2MergeDao.changesBasedSCD2MergeFindDiffs(properties);

          boolean emptyDiffs = FileUtil.isEmptyFolder(diffsFilePath);
          if (!emptyDiffs) {
            WriteUtil.uploadDebugFiles(
                fileIO,
                swiftLakeEngine.getDebugFileUploadPath(),
                diffsFilePath,
                diffsFileBaseFolder);
            properties.setDiffsFilePath(getDiffsTableName(diffsFilePath));
            properties.setModifiedFileNamesFilePath(modifiedFileNamesFilePath);
            scd2MergeDao.saveDistinctFileNamesForChangesMerge(properties);

            WriteUtil.checkMergeCardinality(commonDao, properties.getDiffsFilePath());

            modifiedFiles = scd2MergeDao.getFileNames(modifiedFileNamesFilePath);
          } else {
            properties.setDiffsFilePath(null);
          }
        }

        upsertsSql = scd2MergeDao.getChangesBasedSCD2MergeUpsertsSql(properties);

      } else if (properties.getMode() == SCD2MergeMode.SNAPSHOT) {
        if (!isAppendOnly) {
          properties.setDiffsFilePath("'" + diffsFilePath + "'");

          scd2MergeDao.snapshotBasedSCD2MergeFindDiffs(properties);

          if (FileUtil.isEmptyFolder(diffsFilePath)) {
            LOGGER.info("No changes to apply.");
            return null;
          }
          WriteUtil.uploadDebugFiles(
              fileIO, swiftLakeEngine.getDebugFileUploadPath(), diffsFilePath, diffsFileBaseFolder);
          properties.setDiffsFilePath(getDiffsTableName(diffsFilePath));
          properties.setModifiedFileNamesFilePath(modifiedFileNamesFilePath);
          scd2MergeDao.saveDistinctFileNamesForSnapshotMerge(properties);

          WriteUtil.checkMergeCardinality(commonDao, properties.getDiffsFilePath());

          modifiedFiles = scd2MergeDao.getFileNames(modifiedFileNamesFilePath);
        }

        if (isAppendOnly) {
          upsertsSql = scd2MergeDao.getSnapshotBasedSCD2MergeAppendOnlySql(properties);
        } else {
          upsertsSql = scd2MergeDao.getSnapshotBasedSCD2MergeUpsertsSql(properties);
        }
      }

      // Write new files
      boolean isPartitionedTable = table.spec().isPartitioned();
      List<com.arcesium.swiftlake.common.DataFile> newFiles;
      if (isPartitionedTable) {
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
                .currentFlagColumn(properties.getCurrentFlagColumn())
                .effectiveStartColumn(properties.getEffectiveStartColumn())
                .effectiveEndColumn(properties.getEffectiveEndColumn())
                .skipDataSorting(properties.isSkipDataSorting())
                .build()
                .write();
      } else {
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
          .conflictDetectionFilter(conflictDetectionFilter)
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
        LOGGER.warn("Unable to delete the temporary directory {}", tmpDir);
      }
    }
  }

  private String getDiffsTableName(String diffsFilePath) {
    return "read_parquet(['"
        + diffsFilePath
        + "/*/*.parquet"
        + "'], hive_partitioning=1, union_by_name=True)";
  }

  private String createTableFilter(String sourceTable) {
    boolean addFilterToSource = true;
    if (properties.getTableFilter() == null) {
      if (properties.getTableFilterSql() != null) {
        properties.setTableFilter(
            sqlQueryProcessor.parseConditionExpression(
                properties.getTableFilterSql(), table.schema(), null));
      } else if (properties.getTableFilterColumns() != null
          && !properties.getTableFilterColumns().isEmpty()) {
        Expression expr =
            WriteUtil.getEqualityConditionExpression(
                commonDao, sourceTable, table, properties.getTableFilterColumns());
        properties.setTableFilter(expr);
        addFilterToSource = false;
      } else {
        throw new ValidationException("Table filter is mandatory.");
      }
    }

    if (addFilterToSource) {
      String boundaryCondition =
          swiftLakeEngine
              .getSchemaEvolution()
              .getDuckDBFilterSql(table, properties.getTableFilter());

      sourceTable = "(SELECT * FROM " + sourceTable + " WHERE " + boundaryCondition + ")";
    }

    return sourceTable;
  }

  private Expression appendEffectivePeriodFilter(
      Expression tableFilter, SCD2MergeProperties properties) {
    Expression baseEffectiveEndFilter = null;

    if (properties.getCurrentFlagColumn() != null) {
      baseEffectiveEndFilter = Expressions.equal(properties.getCurrentFlagColumn(), true);
    } else {
      baseEffectiveEndFilter = Expressions.isNull(properties.getEffectiveEndColumn());
    }

    Expression effectiveEndFilter =
        Expressions.or(
            baseEffectiveEndFilter,
            Expressions.greaterThanOrEqual(
                properties.getEffectiveEndColumn(), properties.getEffectiveTimestamp()));

    return Expressions.and(tableFilter, effectiveEndFilter);
  }

  private void validateEffectivePeriodColumns(Schema schema) {
    if (properties.getCurrentFlagColumn() != null
        && schema.findField(properties.getCurrentFlagColumn()) == null) {
      throw new ValidationException("Column %s does not exist.", properties.getCurrentFlagColumn());
    }
    if (properties.getEffectiveStartColumn() != null
        && schema.findField(properties.getEffectiveStartColumn()) == null) {
      throw new ValidationException(
          "Column %s does not exist.", properties.getEffectiveStartColumn());
    }
    if (properties.getEffectiveEndColumn() != null
        && schema.findField(properties.getEffectiveEndColumn()) == null) {
      throw new ValidationException(
          "Column %s does not exist.", properties.getEffectiveEndColumn());
    }
  }

  private String getInputDataSql(SCD2MergeProperties properties) {
    if (properties.getSql() != null && !properties.getSql().isEmpty()) {
      return properties.getSql();
    }
    return WriteUtil.getSql(
        properties.getSqlSessionFactory(),
        properties.getMybatisStatementId(),
        properties.getMybatisStatementParameter());
  }

  private void processChangeTrackingMetadata(Schema schema) {
    if (properties.getChangeTrackingMetadataMap() == null
        || properties.getChangeTrackingMetadataMap().isEmpty()) {
      return;
    }
    properties.setChangeTrackingColumnMaxDeltaValues(
        properties.getChangeTrackingMetadataMap().entrySet().stream()
            .filter(e -> e.getValue() != null && e.getValue().getMaxDeltaValue() != null)
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().getMaxDeltaValue())));
    properties.setChangeTrackingColumnNullReplacements(
        properties.getChangeTrackingMetadataMap().entrySet().stream()
            .filter(e -> e.getValue() != null && e.getValue().getNullReplacement() != null)
            .map(
                e -> {
                  String nullValue =
                      swiftLakeEngine
                          .getSchemaEvolution()
                          .getPrimitiveTypeValueForSql(
                              schema.findField(e.getKey()).type().asPrimitiveType(),
                              e.getValue().getNullReplacement());
                  return Pair.of(e.getKey(), nullValue);
                })
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
  }

  private void setTimestampStrings(SCD2MergeProperties properties, Schema schema) {
    if (properties.getEffectiveTimestamp() != null) {
      Type kdType = schema.findField(properties.getEffectiveStartColumn()).type();
      properties.setEffectiveTimestamp(
          swiftLakeEngine
              .getSchemaEvolution()
              .getPrimitiveTypeValueForSql(
                  kdType.asPrimitiveType(), properties.getEffectiveTimestamp()));
    }

    Type effectiveEndType = schema.findField(properties.getEffectiveEndColumn()).type();
    properties.setDefaultEffectiveEndTimestamp(
        swiftLakeEngine
            .getSchemaEvolution()
            .getPrimitiveTypeValueForSql(effectiveEndType.asPrimitiveType(), null));
  }

  /**
   * Applies changes as SCD2 (Slowly Changing Dimension Type 2) merge operation to a table specified
   * by its name.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param tableName The name of the table to apply changes to.
   * @return A SetTableFilter for further configuration.
   */
  public static SetTableFilter applyChanges(SwiftLakeEngine swiftLakeEngine, String tableName) {
    return new BuilderImpl(swiftLakeEngine, swiftLakeEngine.getTable(tableName, true));
  }

  /**
   * Applies changes as SCD2 (Slowly Changing Dimension Type 2) merge operation to a specified
   * table.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param table The table to apply changes to.
   * @return A SetTableFilter for further configuration.
   */
  public static SetTableFilter applyChanges(SwiftLakeEngine swiftLakeEngine, Table table) {
    return new BuilderImpl(swiftLakeEngine, table);
  }

  /**
   * Applies changes as SCD2 (Slowly Changing Dimension Type 2) merge operation using a
   * TableBatchTransaction.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param tableBatchTransaction The TableBatchTransaction to use for applying changes.
   * @return A SetTableFilter for further configuration.
   */
  public static SetTableFilter applyChanges(
      SwiftLakeEngine swiftLakeEngine, TableBatchTransaction tableBatchTransaction) {
    return new BuilderImpl(swiftLakeEngine, tableBatchTransaction);
  }

  /**
   * Applies a snapshot as SCD2 (Slowly Changing Dimension Type 2) merge operation to a table
   * specified by its name.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param tableName The name of the table to apply the snapshot to.
   * @return A SnapshotModeSetTableFilter for further configuration.
   */
  public static SnapshotModeSetTableFilter applySnapshot(
      SwiftLakeEngine swiftLakeEngine, String tableName) {
    return new SnapshotModeBuilderImpl(swiftLakeEngine, swiftLakeEngine.getTable(tableName, true));
  }

  /**
   * Applies a snapshot as SCD2 (Slowly Changing Dimension Type 2) merge operation to a specified
   * table.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param table The table to apply the snapshot to.
   * @return A SnapshotModeSetTableFilter for further configuration.
   */
  public static SnapshotModeSetTableFilter applySnapshot(
      SwiftLakeEngine swiftLakeEngine, Table table) {
    return new SnapshotModeBuilderImpl(swiftLakeEngine, table);
  }

  /**
   * Applies a snapshot as SCD2 (Slowly Changing Dimension Type 2) merge operation using a
   * TableBatchTransaction.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param tableBatchTransaction The TableBatchTransaction to use for applying the snapshot.
   * @return A SnapshotModeSetTableFilter for further configuration.
   */
  public static SnapshotModeSetTableFilter applySnapshot(
      SwiftLakeEngine swiftLakeEngine, TableBatchTransaction tableBatchTransaction) {
    return new SnapshotModeBuilderImpl(swiftLakeEngine, tableBatchTransaction);
  }

  /** Interface to set table filter conditions. */
  public interface SetTableFilter {
    /**
     * Sets a table filter condition using an Expression.
     *
     * @param condition The filter condition
     * @return SetSourceData interface for further configuration
     */
    SetSourceData tableFilter(Expression condition);

    /**
     * Sets a table filter condition using SQL.
     *
     * @param conditionSql The SQL condition string
     * @return SetSourceData interface for further configuration
     */
    SetSourceData tableFilterSql(String conditionSql);

    /**
     * Sets a table filter based on specified columns.
     *
     * @param columns List of column names to filter
     * @return SetSourceData interface for further configuration
     */
    SetSourceData tableFilterColumns(List<String> columns);
  }

  /** Interface to set the source data for the SCD2 merge operation. */
  public interface SetSourceData {
    /**
     * Sets the source data using a SQL query.
     *
     * @param sql The SQL query string
     * @return SetEffectiveTimestamp interface for further configuration
     */
    SetEffectiveTimestamp sourceSql(String sql);

    /**
     * Sets the source data using a MyBatis statement ID.
     *
     * @param id The MyBatis statement ID
     * @return SetEffectiveTimestamp interface for further configuration
     */
    SetEffectiveTimestamp sourceMybatisStatement(String id);

    /**
     * Sets the source data using a MyBatis statement ID and parameters.
     *
     * @param id The MyBatis statement ID
     * @param parameter The parameter object for the MyBatis statement
     * @return SetEffectiveTimestamp interface for further configuration
     */
    SetEffectiveTimestamp sourceMybatisStatement(String id, Object parameter);
  }

  /** Interface to set the effective timestamp for the SCD2 merge operation. */
  public interface SetEffectiveTimestamp {
    /**
     * Sets the effective timestamp using a string representation.
     *
     * @param effectiveTimestamp The effective timestamp as a string
     * @return SetKeyColumns interface for further configuration
     */
    SetKeyColumns effectiveTimestamp(String effectiveTimestamp);

    /**
     * Sets the effective timestamp using a LocalDateTime object.
     *
     * @param effectiveTimestamp The effective timestamp as a LocalDateTime
     * @return SetKeyColumns interface for further configuration
     */
    SetKeyColumns effectiveTimestamp(LocalDateTime effectiveTimestamp);

    /**
     * Configures whether to generate the effective timestamp.
     *
     * @param generateEffectiveTimestamp True to generate, false otherwise
     * @return SetKeyColumns interface for further configuration
     */
    SetKeyColumns generateEffectiveTimestamp(boolean generateEffectiveTimestamp);
  }

  /** Interface to set the key columns for the SCD2 merge operation. */
  public interface SetKeyColumns {
    /**
     * Sets the key columns for the merge operation.
     *
     * @param keyColumns List of key column names
     * @return SetOperationColumn interface for further configuration
     */
    SetOperationColumn keyColumns(List<String> keyColumns);
  }

  /** Interface to set the operation column for the SCD2 merge operation. */
  public interface SetOperationColumn {
    /**
     * Sets the operation type column and delete operation value.
     *
     * @param column The name of the operation type column
     * @param deleteOperationValue The value indicating a delete operation
     * @return SetEffectivePeriodColumns interface for further configuration
     */
    SetEffectivePeriodColumns operationTypeColumn(String column, String deleteOperationValue);
  }

  /** Interface to set the effective period columns for the SCD2 merge operation. */
  public interface SetEffectivePeriodColumns {
    /**
     * Sets the effective start and end columns for the merge operation.
     *
     * @param effectiveStartColumn The name of the effective start column
     * @param effectiveEndColumn The name of the effective end column
     * @return Builder interface for final configuration and execution
     */
    Builder effectivePeriodColumns(String effectiveStartColumn, String effectiveEndColumn);
  }

  /** Interface for final configuration and execution of the SCD2 merge operation. */
  public interface Builder {
    /**
     * Sets the columns to be included in the merge operation.
     *
     * @param columns List of column names
     * @return Builder interface for further configuration
     */
    Builder columns(List<String> columns);

    /**
     * Sets the columns to be tracked for changes.
     *
     * @param changeTrackingColumns List of column names to track changes
     * @return Builder interface for further configuration
     */
    Builder changeTrackingColumns(List<String> changeTrackingColumns);

    /**
     * Sets the change tracking metadata for multiple columns.
     *
     * @param metadataMap Map of column names to their change tracking metadata
     * @return Builder interface for further configuration
     */
    Builder changeTrackingMetadata(Map<String, ChangeTrackingMetadata<?>> metadataMap);

    /**
     * Sets the change tracking metadata for a single column.
     *
     * @param column The column name
     * @param metadata The change tracking metadata for the column
     * @param <T> The type of the metadata
     * @return Builder interface for further configuration
     */
    <T> Builder changeTrackingMetadata(String column, ChangeTrackingMetadata<T> metadata);

    /**
     * Sets the column used to indicate the current (active) record.
     *
     * @param currentFlagColumn The name of the current flag column
     * @return Builder interface for further configuration
     */
    Builder currentFlagColumn(String currentFlagColumn);

    /**
     * Sets the SQL session factory to be used for database operations.
     *
     * @param sqlSessionFactory The SwiftLakeSqlSessionFactory instance
     * @return Builder interface for further configuration
     */
    Builder sqlSessionFactory(SwiftLakeSqlSessionFactory sqlSessionFactory);

    /**
     * Configures whether to execute the source SQL only once.
     *
     * @param executeSourceSqlOnceOnly True to execute once, false for multiple executions
     * @return Builder interface for further configuration
     */
    Builder executeSourceSqlOnceOnly(boolean executeSourceSqlOnceOnly);

    /**
     * Configures whether to skip data sorting during the merge operation.
     *
     * @param skipDataSorting True to skip sorting, false otherwise
     * @return Builder interface for further configuration
     */
    Builder skipDataSorting(boolean skipDataSorting);

    /**
     * Sets the branch name for the merge operation.
     *
     * @param branch The branch name
     * @return Builder interface for further configuration
     */
    Builder branch(String branch);

    /**
     * Sets the snapshot metadata for the merge operation.
     *
     * @param snapshotMetadata Map of snapshot metadata key-value pairs
     * @return Builder interface for further configuration
     */
    Builder snapshotMetadata(Map<String, String> snapshotMetadata);

    /**
     * Sets the isolation level for the merge operation.
     *
     * @param isolationLevel The desired isolation level
     * @return Builder interface for further configuration
     */
    Builder isolationLevel(IsolationLevel isolationLevel);

    /**
     * Configures whether to process source tables during the merge operation.
     *
     * @param processSourceTables True to process source tables, false otherwise
     * @return Builder interface for further configuration
     */
    Builder processSourceTables(boolean processSourceTables);

    /**
     * Executes the SCD2 merge operation with the configured settings.
     *
     * @return CommitMetrics containing the results of the merge operation
     */
    CommitMetrics execute();
  }

  public static class BuilderImpl
      implements SetSourceData,
          SetTableFilter,
          SetEffectiveTimestamp,
          SetKeyColumns,
          SetOperationColumn,
          SetEffectivePeriodColumns,
          Builder {
    private final SwiftLakeEngine swiftLakeEngine;
    private final Table table;
    private final SCD2MergeProperties properties;
    private TableBatchTransaction tableBatchTransaction;
    private String branch;
    private Map<String, String> snapshotMetadata;
    private IsolationLevel isolationLevel;

    private BuilderImpl(SwiftLakeEngine swiftLakeEngine, Table table) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = table;
      this.properties = new SCD2MergeProperties();
      initProperties();
    }

    private BuilderImpl(
        SwiftLakeEngine swiftLakeEngine, TableBatchTransaction tableBatchTransaction) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = tableBatchTransaction.getTable();
      this.tableBatchTransaction = tableBatchTransaction;
      this.properties = new SCD2MergeProperties();
      initProperties();
    }

    private void initProperties() {
      properties.setMode(SCD2MergeMode.CHANGES);
      properties.setSqlSessionFactory(swiftLakeEngine.getSqlSessionFactory());
      properties.setProcessSourceTables(swiftLakeEngine.getProcessTablesDefaultValue());
      properties.setChangeTrackingMetadataMap(new HashMap<>());
    }

    @Override
    public SetSourceData tableFilter(Expression condition) {
      properties.setTableFilter(condition);
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
    public SetEffectiveTimestamp sourceSql(String sql) {
      properties.setSql(sql);
      return this;
    }

    @Override
    public SetEffectiveTimestamp sourceMybatisStatement(String id) {
      properties.setMybatisStatementId(id);
      return this;
    }

    @Override
    public SetEffectiveTimestamp sourceMybatisStatement(String id, Object parameter) {
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
    public SetKeyColumns effectiveTimestamp(String effectiveTimestamp) {
      checkLocalDateTimeString(effectiveTimestamp, "effectiveTimestamp");
      properties.setEffectiveTimestamp(effectiveTimestamp);
      return this;
    }

    @Override
    public SetKeyColumns effectiveTimestamp(LocalDateTime effectiveTimestamp) {
      properties.setEffectiveTimestamp(getIsoLocalDateTimeString(effectiveTimestamp));
      return this;
    }

    @Override
    public SetKeyColumns generateEffectiveTimestamp(boolean generateEffectiveTimestamp) {
      properties.setGenerateEffectiveTimestamp(generateEffectiveTimestamp);
      return this;
    }

    @Override
    public Builder columns(List<String> columns) {
      properties.setColumns(
          columns == null ? null : columns.stream().distinct().collect(Collectors.toList()));
      return this;
    }

    @Override
    public SetOperationColumn keyColumns(List<String> keyColumns) {
      properties.setKeyColumns(keyColumns.stream().distinct().collect(Collectors.toList()));
      return this;
    }

    @Override
    public SetEffectivePeriodColumns operationTypeColumn(
        String column, String deleteOperationValue) {
      properties.setOperationTypeColumn(column);
      properties.setDeleteOperationValue(deleteOperationValue);
      return this;
    }

    @Override
    public Builder effectivePeriodColumns(String effectiveStartColumn, String effectiveEndColumn) {
      properties.setEffectiveStartColumn(effectiveStartColumn);
      properties.setEffectiveEndColumn(effectiveEndColumn);
      return this;
    }

    @Override
    public Builder changeTrackingColumns(List<String> changeTrackingColumns) {
      properties.setChangeTrackingColumns(
          changeTrackingColumns == null
              ? null
              : changeTrackingColumns.stream().distinct().collect(Collectors.toList()));
      return this;
    }

    @Override
    public Builder changeTrackingMetadata(Map<String, ChangeTrackingMetadata<?>> metadataMap) {
      properties.getChangeTrackingMetadataMap().putAll(metadataMap);
      return this;
    }

    @Override
    public <T> Builder changeTrackingMetadata(String column, ChangeTrackingMetadata<T> metadata) {
      properties.getChangeTrackingMetadataMap().put(column, metadata);
      return this;
    }

    @Override
    public Builder currentFlagColumn(String currentFlagColumn) {
      properties.setCurrentFlagColumn(currentFlagColumn);
      return this;
    }

    @Override
    public Builder sqlSessionFactory(SwiftLakeSqlSessionFactory sqlSessionFactory) {
      properties.setSqlSessionFactory(sqlSessionFactory);
      return this;
    }

    @Override
    public Builder executeSourceSqlOnceOnly(boolean executeSourceSqlOnceOnly) {
      properties.setExecuteSourceSqlOnceOnly(executeSourceSqlOnceOnly);
      return this;
    }

    @Override
    public Builder skipDataSorting(boolean skipDataSorting) {
      properties.setSkipDataSorting(skipDataSorting);
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
      SCD2Merge merge =
          new SCD2Merge(
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

  /** Interface for setting table filter conditions in snapshot mode. */
  public interface SnapshotModeSetTableFilter {
    /**
     * Sets a table filter condition using an Expression.
     *
     * @param condition The filter condition as an Expression
     * @return SnapshotModeSetSourceData instance for further configuration
     */
    SnapshotModeSetSourceData tableFilter(Expression condition);

    /**
     * Sets a table filter condition using SQL.
     *
     * @param conditionSql The filter condition as an SQL string
     * @return SnapshotModeSetSourceData instance for further configuration
     */
    SnapshotModeSetSourceData tableFilterSql(String conditionSql);
  }

  /** Interface for setting source data in snapshot mode. */
  public interface SnapshotModeSetSourceData {
    /**
     * Sets the source data using an SQL query.
     *
     * @param sql The SQL query to fetch source data
     * @return SnapshotModeSetEffectiveTimestamp instance for further configuration
     */
    SnapshotModeSetEffectiveTimestamp sourceSql(String sql);

    /**
     * Sets the source data using a MyBatis statement ID.
     *
     * @param id The MyBatis statement ID
     * @return SnapshotModeSetEffectiveTimestamp instance for further configuration
     */
    SnapshotModeSetEffectiveTimestamp sourceMybatisStatement(String id);

    /**
     * Sets the source data using a MyBatis statement ID and parameters.
     *
     * @param id The MyBatis statement ID
     * @param parameter The parameter object for the MyBatis statement
     * @return SnapshotModeSetEffectiveTimestamp instance for further configuration
     */
    SnapshotModeSetEffectiveTimestamp sourceMybatisStatement(String id, Object parameter);
  }

  /** Interface for setting effective timestamp in snapshot mode. */
  public interface SnapshotModeSetEffectiveTimestamp {
    /**
     * Sets the effective timestamp using a string representation.
     *
     * @param effectiveTimestamp The effective timestamp as a string
     * @return SnapshotModeSetKeyColumns instance for further configuration
     */
    SnapshotModeSetKeyColumns effectiveTimestamp(String effectiveTimestamp);

    /**
     * Sets the effective timestamp using a LocalDateTime object.
     *
     * @param effectiveTimestamp The effective timestamp as a LocalDateTime
     * @return SnapshotModeSetKeyColumns instance for further configuration
     */
    SnapshotModeSetKeyColumns effectiveTimestamp(LocalDateTime effectiveTimestamp);

    /**
     * Configures whether to generate the effective timestamp.
     *
     * @param generateEffectiveTimestamp True to generate effective timestamp, false otherwise
     * @return SnapshotModeSetKeyColumns instance for further configuration
     */
    SnapshotModeSetKeyColumns generateEffectiveTimestamp(boolean generateEffectiveTimestamp);
  }

  /** Interface for setting key columns in snapshot mode. */
  public interface SnapshotModeSetKeyColumns {
    /**
     * Sets the key columns for the snapshot.
     *
     * @param keyColumns List of key column names
     * @return SnapshotModeSetEffectivePeriodColumns instance for further configuration
     */
    SnapshotModeSetEffectivePeriodColumns keyColumns(List<String> keyColumns);
  }

  /** Interface for setting effective period columns in snapshot mode. */
  public interface SnapshotModeSetEffectivePeriodColumns {
    /**
     * Sets the effective period columns for the snapshot.
     *
     * @param effectiveStartColumn The column name for effective start period
     * @param effectiveEndColumn The column name for effective end period
     * @return SnapshotModeBuilder instance for further configuration
     */
    SnapshotModeBuilder effectivePeriodColumns(
        String effectiveStartColumn, String effectiveEndColumn);
  }

  /** Interface for building and executing snapshot mode operations. */
  public interface SnapshotModeBuilder {
    /**
     * Sets the columns to be included in the snapshot.
     *
     * @param columns List of column names
     * @return This SnapshotModeBuilder instance
     */
    SnapshotModeBuilder columns(List<String> columns);

    /**
     * Sets the columns to be used for change tracking.
     *
     * @param changeTrackingColumns List of change tracking column names
     * @return This SnapshotModeBuilder instance
     */
    SnapshotModeBuilder changeTrackingColumns(List<String> changeTrackingColumns);

    /**
     * Sets the change tracking metadata for multiple columns.
     *
     * @param metadataMap Map of column names to their respective ChangeTrackingMetadata
     * @return This SnapshotModeBuilder instance
     */
    SnapshotModeBuilder changeTrackingMetadata(Map<String, ChangeTrackingMetadata<?>> metadataMap);

    /**
     * Sets the change tracking metadata for a single column.
     *
     * @param column The column name
     * @param metadata The ChangeTrackingMetadata for the column
     * @return This SnapshotModeBuilder instance
     */
    <T> SnapshotModeBuilder changeTrackingMetadata(
        String column, ChangeTrackingMetadata<T> metadata);

    /**
     * Sets the column name for the current flag.
     *
     * @param currentFlagColumn The name of the current flag column
     * @return This SnapshotModeBuilder instance
     */
    SnapshotModeBuilder currentFlagColumn(String currentFlagColumn);

    /**
     * Sets the SQL session factory to be used.
     *
     * @param sqlSessionFactory The SwiftLakeSqlSessionFactory instance
     * @return This SnapshotModeBuilder instance
     */
    SnapshotModeBuilder sqlSessionFactory(SwiftLakeSqlSessionFactory sqlSessionFactory);

    /**
     * Configures whether to skip empty source data.
     *
     * @param skipEmptySource True to skip empty source data, false otherwise
     * @return This SnapshotModeBuilder instance
     */
    SnapshotModeBuilder skipEmptySource(boolean skipEmptySource);

    /**
     * Configures whether to skip data sorting.
     *
     * @param skipDataSorting True to skip data sorting, false otherwise
     * @return This SnapshotModeBuilder instance
     */
    SnapshotModeBuilder skipDataSorting(boolean skipDataSorting);

    /**
     * Configures whether to execute source SQL only once.
     *
     * @param executeSourceSqlOnceOnly True to execute source SQL once only, false otherwise
     * @return This SnapshotModeBuilder instance
     */
    SnapshotModeBuilder executeSourceSqlOnceOnly(boolean executeSourceSqlOnceOnly);

    /**
     * Sets the branch name for the snapshot operation.
     *
     * @param branch The branch name
     * @return This SnapshotModeBuilder instance
     */
    SnapshotModeBuilder branch(String branch);

    /**
     * Sets the snapshot metadata.
     *
     * @param snapshotMetadata Map of snapshot metadata key-value pairs
     * @return This SnapshotModeBuilder instance
     */
    SnapshotModeBuilder snapshotMetadata(Map<String, String> snapshotMetadata);

    /**
     * Sets the isolation level for the snapshot operation.
     *
     * @param isolationLevel The IsolationLevel to be used
     * @return This SnapshotModeBuilder instance
     */
    SnapshotModeBuilder isolationLevel(IsolationLevel isolationLevel);

    /**
     * Configures whether to process source tables.
     *
     * @param processSourceTables True to process source tables, false otherwise
     * @return This SnapshotModeBuilder instance
     */
    SnapshotModeBuilder processSourceTables(boolean processSourceTables);

    /**
     * Executes the snapshot mode operation.
     *
     * @return CommitMetrics containing metrics about the operation
     */
    CommitMetrics execute();
  }

  private static void checkLocalDateTimeString(String localDateTimeString, String name) {
    if (localDateTimeString == null) {
      return;
    }
    String message =
        String.format(
            "Invalid %s. Use ISO date-time format yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS.", name);
    try {
      LocalDateTime localDateTime =
          LocalDateTime.parse(localDateTimeString, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
      ValidationException.checkNotNull(localDateTime, message);
    } catch (Exception ex) {
      throw new ValidationException(message);
    }
  }

  private static String getIsoLocalDateTimeString(LocalDateTime localDateTime) {
    if (localDateTime == null) return null;

    return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(localDateTime);
  }

  public static class SnapshotModeBuilderImpl
      implements SnapshotModeSetSourceData,
          SnapshotModeSetTableFilter,
          SnapshotModeSetEffectiveTimestamp,
          SnapshotModeSetKeyColumns,
          SnapshotModeSetEffectivePeriodColumns,
          SnapshotModeBuilder {
    private final SwiftLakeEngine swiftLakeEngine;
    private final Table table;
    private final SCD2MergeProperties properties;
    private TableBatchTransaction tableBatchTransaction;
    private String branch;
    private Map<String, String> snapshotMetadata;
    private IsolationLevel isolationLevel;

    private SnapshotModeBuilderImpl(SwiftLakeEngine swiftLakeEngine, Table table) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = table;
      this.properties = new SCD2MergeProperties();
      initProperties();
    }

    private SnapshotModeBuilderImpl(
        SwiftLakeEngine swiftLakeEngine, TableBatchTransaction tableBatchTransaction) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = tableBatchTransaction.getTable();
      this.tableBatchTransaction = tableBatchTransaction;
      this.properties = new SCD2MergeProperties();
      initProperties();
    }

    private void initProperties() {
      properties.setMode(SCD2MergeMode.SNAPSHOT);
      properties.setSqlSessionFactory(swiftLakeEngine.getSqlSessionFactory());
      properties.setProcessSourceTables(swiftLakeEngine.getProcessTablesDefaultValue());
      properties.setChangeTrackingMetadataMap(new HashMap<>());
    }

    @Override
    public SnapshotModeSetSourceData tableFilter(Expression condition) {
      properties.setTableFilter(condition);
      return this;
    }

    @Override
    public SnapshotModeSetSourceData tableFilterSql(String conditionSql) {
      properties.setTableFilterSql(conditionSql);
      return this;
    }

    @Override
    public SnapshotModeSetEffectiveTimestamp sourceSql(String sql) {
      properties.setSql(sql);
      return this;
    }

    @Override
    public SnapshotModeSetEffectiveTimestamp sourceMybatisStatement(String id) {
      properties.setMybatisStatementId(id);
      return this;
    }

    @Override
    public SnapshotModeSetEffectiveTimestamp sourceMybatisStatement(String id, Object parameter) {
      properties.setMybatisStatementId(id);
      properties.setMybatisStatementParameter(parameter);
      return this;
    }

    @Override
    public SnapshotModeBuilder processSourceTables(boolean processSourceTables) {
      properties.setProcessSourceTables(processSourceTables);
      return this;
    }

    @Override
    public SnapshotModeSetKeyColumns effectiveTimestamp(String effectiveTimestamp) {
      checkLocalDateTimeString(effectiveTimestamp, "effectiveTimestamp");
      properties.setEffectiveTimestamp(effectiveTimestamp);
      return this;
    }

    @Override
    public SnapshotModeSetKeyColumns effectiveTimestamp(LocalDateTime effectiveTimestamp) {
      properties.setEffectiveTimestamp(getIsoLocalDateTimeString(effectiveTimestamp));
      return this;
    }

    @Override
    public SnapshotModeSetKeyColumns generateEffectiveTimestamp(
        boolean generateEffectiveTimestamp) {
      properties.setGenerateEffectiveTimestamp(generateEffectiveTimestamp);
      return this;
    }

    @Override
    public SnapshotModeSetEffectivePeriodColumns keyColumns(List<String> keyColumns) {
      properties.setKeyColumns(keyColumns.stream().distinct().collect(Collectors.toList()));
      return this;
    }

    @Override
    public SnapshotModeBuilder effectivePeriodColumns(
        String effectiveStartColumn, String effectiveEndColumn) {
      properties.setEffectiveStartColumn(effectiveStartColumn);
      properties.setEffectiveEndColumn(effectiveEndColumn);
      return this;
    }

    @Override
    public SnapshotModeBuilder columns(List<String> columns) {
      properties.setColumns(
          columns == null ? null : columns.stream().distinct().collect(Collectors.toList()));
      return this;
    }

    @Override
    public SnapshotModeBuilder changeTrackingColumns(List<String> changeTrackingColumns) {
      properties.setChangeTrackingColumns(
          changeTrackingColumns == null
              ? null
              : changeTrackingColumns.stream().distinct().collect(Collectors.toList()));
      return this;
    }

    @Override
    public SnapshotModeBuilder changeTrackingMetadata(
        Map<String, ChangeTrackingMetadata<?>> metadataMap) {
      properties.getChangeTrackingMetadataMap().putAll(metadataMap);
      return this;
    }

    @Override
    public <T> SnapshotModeBuilder changeTrackingMetadata(
        String column, ChangeTrackingMetadata<T> metadata) {
      properties.getChangeTrackingMetadataMap().put(column, metadata);
      return this;
    }

    @Override
    public SnapshotModeBuilder currentFlagColumn(String currentFlagColumn) {
      properties.setCurrentFlagColumn(currentFlagColumn);
      return this;
    }

    @Override
    public SnapshotModeBuilder sqlSessionFactory(SwiftLakeSqlSessionFactory sqlSessionFactory) {
      properties.setSqlSessionFactory(sqlSessionFactory);
      return this;
    }

    @Override
    public SnapshotModeBuilder skipEmptySource(boolean skipEmptySource) {
      properties.setSkipEmptySource(skipEmptySource);
      return this;
    }

    @Override
    public SnapshotModeBuilder skipDataSorting(boolean skipDataSorting) {
      properties.setSkipDataSorting(skipDataSorting);
      return this;
    }

    @Override
    public SnapshotModeBuilder executeSourceSqlOnceOnly(boolean executeSourceSqlOnceOnly) {
      properties.setExecuteSourceSqlOnceOnly(executeSourceSqlOnceOnly);
      return this;
    }

    @Override
    public SnapshotModeBuilder branch(String branch) {
      ValidationException.check(
          branch == null || tableBatchTransaction == null,
          "Set branch name on the batch transaction.");
      this.branch = branch;
      return this;
    }

    @Override
    public SnapshotModeBuilder snapshotMetadata(Map<String, String> snapshotMetadata) {
      ValidationException.check(
          snapshotMetadata == null || tableBatchTransaction == null,
          "Set snapshot metadata on the batch transaction.");
      this.snapshotMetadata =
          snapshotMetadata == null ? null : ImmutableMap.copyOf(snapshotMetadata);
      return this;
    }

    @Override
    public SnapshotModeBuilder isolationLevel(IsolationLevel isolationLevel) {
      ValidationException.check(
          isolationLevel == null || tableBatchTransaction == null,
          "Set isolation level on the batch transaction.");
      this.isolationLevel = isolationLevel;
      return this;
    }

    @Override
    public CommitMetrics execute() {
      SCD2Merge merge =
          new SCD2Merge(
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
