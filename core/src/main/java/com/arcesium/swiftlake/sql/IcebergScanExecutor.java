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
package com.arcesium.swiftlake.sql;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.commands.WriteUtil;
import com.arcesium.swiftlake.common.FileUtil;
import com.arcesium.swiftlake.common.InputFile;
import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.MultipleInputFiles;
import com.arcesium.swiftlake.common.SwiftLakeConfiguration;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.ResolvedExpression;
import com.arcesium.swiftlake.io.DefaultInputFile;
import com.arcesium.swiftlake.io.DefaultInputFiles;
import com.arcesium.swiftlake.io.SwiftLakeFileIO;
import com.arcesium.swiftlake.metrics.PartitionData;
import com.arcesium.swiftlake.metrics.TableScanMetrics;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executor class for performing Iceberg scans. */
public class IcebergScanExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergScanExecutor.class);
  private static final int IN_PREDICATE_LIMIT = 200;
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private final SwiftLakeEngine swiftLakeEngine;
  private final SwiftLakeConfiguration configuration;

  /**
   * Constructs a new IcebergScanExecutor.
   *
   * @param swiftLakeEngine The SwiftLake engine to use for operations.
   * @param configuration The configuration for SwiftLake.
   */
  public IcebergScanExecutor(
      SwiftLakeEngine swiftLakeEngine, SwiftLakeConfiguration configuration) {
    this.swiftLakeEngine = swiftLakeEngine;
    this.configuration = configuration;
  }

  /**
   * Executes a table scan on the specified table with the given file filter.
   *
   * @param tableName The name of the table to scan.
   * @param fileFilter The expression to filter files.
   * @return A TableScanResult containing the scan results.
   */
  public TableScanResult executeTableScan(
      String tableName, com.arcesium.swiftlake.expressions.Expression fileFilter) {
    return this.executeTableScan(swiftLakeEngine.getTable(tableName), fileFilter);
  }

  /**
   * Executes a table scan on the specified table with the given file filter.
   *
   * @param table The Table instance to scan.
   * @param fileFilter The expression to filter files.
   * @return A TableScanResult containing the scan results.
   */
  public TableScanResult executeTableScan(
      Table table, com.arcesium.swiftlake.expressions.Expression fileFilter) {
    return this.executeTableScan(table, fileFilter, false, false, null, null, null, true);
  }

  /**
   * Executes a table scan on the given Iceberg table.
   *
   * @param table The Iceberg table to scan
   * @param fileFilter Expression to filter files
   * @param includeFileName Whether to include file names in the result
   * @param includeFileRowNumber Whether to include file row numbers in the result
   * @param asOfTimestamp Timestamp for time travel queries
   * @param branchOrTagName Branch or tag name for the scan
   * @param snapshotId Specific snapshot ID to use
   * @param allowDirectRead Whether to allow direct file reading
   * @return TableScanResult containing the SQL query and input files
   * @throws SwiftLakeException if an error occurs during the scan
   */
  public TableScanResult executeTableScan(
      Table table,
      com.arcesium.swiftlake.expressions.Expression fileFilter,
      boolean includeFileName,
      boolean includeFileRowNumber,
      LocalDateTime asOfTimestamp,
      String branchOrTagName,
      Long snapshotId,
      boolean allowDirectRead) {
    InputFiles inputFiles = null;
    Exception exception = null;
    try {
      Stopwatch totalDuration = Stopwatch.createStarted();
      Stopwatch scanDuration = Stopwatch.createStarted();
      // Scan data files
      ResolvedExpression resolvedExpression =
          com.arcesium.swiftlake.expressions.Expressions.resolveExpression(fileFilter);
      Pair<TableScan, List<DataFile>> scanResult =
          this.scanDataFiles(
              table,
              resolvedExpression.getExpression(),
              true,
              asOfTimestamp,
              branchOrTagName,
              snapshotId);
      scanDuration.stop();
      checkTableScanLimits(scanResult);

      Stopwatch fileFetchDuration = Stopwatch.createStarted();
      // Get input files for the table scan
      inputFiles = getInputFilesForTableScan(scanResult, allowDirectRead);
      fileFetchDuration.stop();

      Stopwatch sqlPreparationDuration = Stopwatch.createStarted();
      // Prepare local and remote file names for SQL query
      List<String> localFileNamesForSql =
          inputFiles.getInputFiles().stream()
              .filter(i -> i.getLocalFileLocation() != null)
              .map(i -> i.getLocalFileLocation())
              .collect(Collectors.toList());
      List<String> remoteFileNamesForSql =
          inputFiles.getInputFiles().stream()
              .filter(i -> i.getLocalFileLocation() == null)
              .map(i -> i.getLocation())
              .collect(Collectors.toList());

      // Generate SQL for data files
      String filesSql =
          swiftLakeEngine
              .getSchemaEvolution()
              .getSelectSQLForDataFiles(
                  table,
                  localFileNamesForSql,
                  remoteFileNamesForSql,
                  includeFileName,
                  includeFileRowNumber);
      sqlPreparationDuration.stop();
      totalDuration.stop();
      // Collect metrics if a metric collector is available
      if (swiftLakeEngine.getMetricCollector() != null) {
        swiftLakeEngine
            .getMetricCollector()
            .collectMetrics(
                new TableScanMetrics(
                    table.name(),
                    totalDuration.elapsed(),
                    scanDuration.elapsed(),
                    fileFetchDuration.elapsed(),
                    sqlPreparationDuration.elapsed(),
                    scanResult.getRight().size(),
                    getTotalFileSize(scanResult.getRight())));
      }
      return new TableScanResult(filesSql, inputFiles, scanResult);
    } catch (Exception e) {
      exception = e;
      if (e instanceof ValidationException || e instanceof SwiftLakeException) {
        throw e;
      } else {
        throw new SwiftLakeException(e, "An error occurred while scanning table");
      }
    } finally {
      // Close input files if an exception occurred
      if (exception != null && inputFiles != null) {
        try {
          inputFiles.close();
        } catch (Exception e) {
          LOGGER.error("An error occurred while closing resource.", e);
        }
      }
    }
  }

  /**
   * Retrieves input files for a table scan based on the scan result and direct read allowance.
   *
   * @param scanResult The result of the table scan
   * @param allowDirectRead Whether direct file reading is allowed
   * @return InputFiles object containing the input files for the scan
   */
  private InputFiles getInputFilesForTableScan(
      Pair<TableScan, List<DataFile>> scanResult, boolean allowDirectRead) {
    if (scanResult.getRight().isEmpty()) {
      return new DefaultInputFiles(List.of());
    }

    SwiftLakeFileIO fileIO = (SwiftLakeFileIO) scanResult.getLeft().table().io();
    if (!allowDirectRead) {
      // If direct read is not allowed, return all files as input files
      List<String> dataFileNames =
          scanResult.getRight().stream()
              .map(d -> d.location().toString())
              .collect(Collectors.toList());
      return fileIO.newInputFiles(dataFileNames);
    }

    List<InputFiles> inputFilesList = new ArrayList<>();
    // Filter files that cannot be downloaded for direct read
    List<InputFile> directReadInputFiles =
        scanResult.getRight().stream()
            .filter(d -> !fileIO.isDownloadable(d.location().toString(), d.fileSizeInBytes()))
            .map(d -> new DefaultInputFile(null, d.location().toString(), d.fileSizeInBytes()))
            .collect(Collectors.toList());
    if (!directReadInputFiles.isEmpty()) {
      inputFilesList.add(new DefaultInputFiles(directReadInputFiles));
    }

    // Filter files that can be downloaded
    List<String> downloadFileNames =
        scanResult.getRight().stream()
            .filter(d -> fileIO.isDownloadable(d.location().toString(), d.fileSizeInBytes()))
            .map(d -> d.location().toString())
            .collect(Collectors.toList());

    if (!downloadFileNames.isEmpty()) {
      inputFilesList.add(fileIO.newInputFiles(downloadFileNames));
    }
    return new MultipleInputFiles(inputFilesList);
  }

  private void checkTableScanLimits(Pair<TableScan, List<DataFile>> scanResult) {
    if (configuration.getTotalFileSizePerScanLimitInMiB() != null
        && configuration.getTotalFileSizePerScanLimitInMiB() > 0) {
      long totalFileSize = getTotalFileSize(scanResult.getRight());
      long totalFileSizeLimit =
          configuration.getTotalFileSizePerScanLimitInMiB() * FileUtil.MIB_FACTOR;
      ValidationException.check(
          totalFileSize <= totalFileSizeLimit,
          "Total file size per scan exceeds limit (%d MiB)",
          configuration.getTotalFileSizePerScanLimitInMiB());
    }
  }

  private long getTotalFileSize(List<DataFile> dataFiles) {
    long totalFileSize = 0;
    for (DataFile dataFile : dataFiles) {
      totalFileSize += dataFile.fileSizeInBytes();
    }
    return totalFileSize;
  }

  /**
   * Executes table scans and updates the SQL query based on the provided filters.
   *
   * @param sql The original SQL query
   * @param tableFilters List of table filters to apply
   * @param connection SwiftLake connection
   * @param allowDirectRead Flag to allow direct read access
   * @return A pair containing the updated SQL query and a list of InputFiles
   * @throws ValidationException If the table filter is invalid
   * @throws SwiftLakeException If an error occurs during table scanning
   */
  public Pair<String, List<InputFiles>> executeTableScansAndUpdateSql(
      String sql, List<TableFilter> tableFilters, Connection connection, boolean allowDirectRead) {
    List<Pair<String, TableScanResult>> scanResults = null;
    try {
      // Process table filters without SQL
      List<TableFilter> allTableFilters = new ArrayList<>();
      allTableFilters.addAll(
          tableFilters.stream()
              .filter(f -> f.getSql() == null)
              .map(
                  tf -> {
                    if (tf.getTableName() != null && tf.getTimeTravelOptions() == null) {
                      Pair<String, TimeTravelOptions> result =
                          swiftLakeEngine
                              .getSqlQueryProcessor()
                              .parseTimeTravelOptions(tf.getTableName());
                      if (result != null) {
                        tf.setTableName(result.getLeft());
                        tf.setTimeTravelOptions(result.getRight());
                      }
                    }
                    return tf;
                  })
              .collect(Collectors.toList()));

      // Process table filters with SQL
      for (TableFilter filter :
          tableFilters.stream().filter(f -> f.getSql() != null).collect(Collectors.toList())) {

        Pair<String, List<TableFilter>> result =
            swiftLakeEngine
                .getSqlQueryProcessor()
                .process(filter.getSql(), filter.getParams(), connection, null);
        ValidationException.check(result.getRight().size() == 1, "Invalid table filter.");
        sql = sql.replace(filter.getPlaceholder(), result.getRight().get(0).getPlaceholder());
        allTableFilters.addAll(result.getRight());
      }

      // Execute table scans for all filters
      scanResults =
          allTableFilters.stream()
              .map(
                  (tableFilter) -> {
                    if (tableFilter.getTable() == null) {
                      tableFilter.setTable(swiftLakeEngine.getTable(tableFilter.getTableName()));
                    }
                    com.arcesium.swiftlake.expressions.Expression expr =
                        getOrCreateTableFilterExpression(tableFilter);
                    String key =
                        tableFilter.getPlaceholder() != null
                            ? tableFilter.getPlaceholder()
                            : tableFilter.getTableName();

                    TableScanResult scanResult = null;
                    if (tableFilter.getTimeTravelOptions() == null) {
                      scanResult =
                          executeTableScan(
                              tableFilter.getTable(),
                              expr,
                              false,
                              false,
                              null,
                              null,
                              null,
                              allowDirectRead);
                    } else {
                      scanResult =
                          executeTableScan(
                              tableFilter.getTable(),
                              expr,
                              false,
                              false,
                              tableFilter.getTimeTravelOptions().getTimestamp(),
                              tableFilter.getTimeTravelOptions().getBranchOrTagName(),
                              tableFilter.getTimeTravelOptions().getSnapshotId(),
                              allowDirectRead);
                    }
                    return Pair.of(key, scanResult);
                  })
              .collect(Collectors.toList());

      // Update SQL with scan results
      for (Pair<String, TableScanResult> scanResult : scanResults) {
        sql = sql.replace(scanResult.getKey(), scanResult.getValue().getSql());
      }

      // Return updated SQL and input files
      return Pair.of(
          sql,
          scanResults.stream().map(r -> r.getValue().getInputFiles()).collect(Collectors.toList()));
    } catch (Throwable t) {
      // Close resources
      if (scanResults != null) {
        scanResults.forEach(
            r -> {
              try {
                r.getValue().getInputFiles().close();
              } catch (Exception ex) {
                LOGGER.error("An error occurred while closing resource.", ex);
              }
            });
      }
      if (t instanceof ValidationException || t instanceof SwiftLakeException) {
        throw t;
      } else if (t instanceof Error error) {
        throw error;
      } else {
        throw new SwiftLakeException(t, "An error occurred while scanning table.");
      }
    }
  }

  private com.arcesium.swiftlake.expressions.Expression getOrCreateTableFilterExpression(
      TableFilter tableFilter) {
    if (tableFilter.getCondition() != null) return tableFilter.getCondition();

    com.arcesium.swiftlake.expressions.Expression condition =
        com.arcesium.swiftlake.expressions.Expressions.alwaysTrue();
    if (tableFilter.getConditionSql() != null) {
      condition =
          swiftLakeEngine
              .getSqlQueryProcessor()
              .parseConditionExpression(
                  tableFilter.getConditionSql(),
                  tableFilter.getTable().schema(),
                  tableFilter.getParams());
    }
    return condition;
  }

  /**
   * Validates if a full table scan is allowed based on the given expression and configuration.
   *
   * @param expression The filter expression to be validated
   * @param tableName The name of the table being scanned
   * @throws ValidationException if full table scan is not allowed and the expression is always true
   */
  public void validateFullTableScan(Expression expression, String tableName) {
    ValidationException.check(
        configuration.isAllowFullTableScan()
            || !expression.isEquivalentTo(Expressions.alwaysTrue()),
        "Operation prohibited: Full table scan on '%s'. Use more selective filters to avoid performance issues.",
        tableName);
  }

  /**
   * Scans the data files of a table based on the given parameters.
   *
   * @param table The Iceberg table to scan
   * @param expression The filter expression for the scan
   * @param fixExpr Whether to fix the expression before scanning
   * @param asOfTimestamp The timestamp to use for time travel queries
   * @param branchOrTagName The branch or tag name to use for the scan
   * @param snapshotId The snapshot ID to use for the scan
   * @return A pair containing the TableScan and a list of DataFiles
   * @throws SwiftLakeException if the scan fails
   */
  public Pair<TableScan, List<DataFile>> scanDataFiles(
      Table table,
      Expression expression,
      boolean fixExpr,
      LocalDateTime asOfTimestamp,
      String branchOrTagName,
      Long snapshotId) {
    validateFullTableScan(expression, table.name());
    Instant start = Instant.now();
    // Fix expression if required
    if (fixExpr) {
      expression = fixExpression(expression);
    }
    // Initialize table scan with filter
    TableScan scan = table.newScan();
    scan = scan.filter(expression);

    // Apply time travel, branch/tag, or snapshot ID if specified
    if (asOfTimestamp != null) {
      long asOfTimestampMillis =
          ChronoUnit.MILLIS.between(EPOCH, asOfTimestamp.atOffset(ZoneOffset.UTC));
      scan = scan.asOfTime(asOfTimestampMillis);
    } else if (branchOrTagName != null) {
      scan = scan.useRef(branchOrTagName);
    } else if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }
    LOGGER.debug("Iceberg table scan {}", scan);

    List<DataFile> files = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles();
        CloseableIterator<FileScanTask> iterator = tasks.iterator()) {
      while (iterator.hasNext()) {
        FileScanTask task = iterator.next();
        // Check for delete files (not supported)
        if (task.deletes() != null && !task.deletes().isEmpty()) {
          throw new ValidationException(
              "Found delete file. 'merge-on-read' mode is not supported.");
        }
        if (task.file() != null) {
          files.add(task.file());
        }
      }
    } catch (IOException e) {
      throw new SwiftLakeException(e, "Iceberg Table Scan Failed.");
    }

    LOGGER.debug(
        "Scan time: {}, Data files count: {}",
        Duration.between(start, Instant.now()).toMillis(),
        files.size());
    return Pair.of(scan, files);
  }

  /**
   * Retrieves partition-level record counts for the given table and partition data list.
   *
   * @param table The Iceberg table
   * @param partitionDataList List of partition data to filter on
   * @return A list of pairs containing PartitionData and corresponding record count
   * @throws SwiftLakeException if the scan fails
   */
  public List<Pair<PartitionData, Long>> getPartitionLevelRecordCounts(
      Table table, List<PartitionData> partitionDataList) {
    return getPartitionLevelRecordCounts(
        table, WriteUtil.getPartitionFilterExpression(table, partitionDataList));
  }

  /**
   * Retrieves partition-level record counts for the given table and expression.
   *
   * @param table The Iceberg table
   * @param expression Expression to filter on
   * @return A list of pairs containing PartitionData and corresponding record count
   * @throws SwiftLakeException if the scan fails
   */
  public List<Pair<PartitionData, Long>> getPartitionLevelRecordCounts(
      Table table, com.arcesium.swiftlake.expressions.Expression expression) {
    Map<Integer, PartitionSpec> specMap = table.specs();
    Map<Pair<Integer, StructLike>, Long> recordCounts = new HashMap<>();
    // Create table scan with partition filter
    TableScan scan = table.newScan();
    ResolvedExpression resolvedExpression =
        com.arcesium.swiftlake.expressions.Expressions.resolveExpression(expression);
    scan = scan.filter(resolvedExpression.getExpression());
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles();
        CloseableIterator<FileScanTask> iterator = tasks.iterator()) {
      while (iterator.hasNext()) {
        FileScanTask task = iterator.next();
        // Check for delete files (not supported)
        if (task.deletes() != null && !task.deletes().isEmpty()) {
          throw new ValidationException(
              "Found delete file. 'merge-on-read' mode is not supported.");
        }
        if (task.file() == null) {
          continue;
        }
        DataFile dataFile = task.file();
        Pair<Integer, StructLike> key = Pair.of(dataFile.specId(), dataFile.partition());
        if (!recordCounts.containsKey(key)) {
          recordCounts.put(key, 0L);
        }
        // Accumulate record counts for each partition
        recordCounts.put(key, recordCounts.get(key) + dataFile.recordCount());
      }
    } catch (IOException e) {
      throw new SwiftLakeException(e, "Iceberg Table Scan Failed.");
    }

    // Convert results to list of pairs with PartitionData and record count
    return recordCounts.entrySet().stream()
        .map(
            e ->
                Pair.of(
                    getPartitionData(specMap, e.getKey().getKey(), e.getKey().getValue()),
                    e.getValue()))
        .collect(Collectors.toList());
  }

  private PartitionData getPartitionData(
      Map<Integer, PartitionSpec> specMap, int specId, StructLike partition) {
    PartitionSpec spec = specMap.get(specId);
    List<Pair<PartitionField, Object>> partitionValues = new ArrayList<>();
    for (int i = 0; i < spec.fields().size(); i++) {
      partitionValues.add(Pair.of(spec.fields().get(i), partition.get(i, Object.class)));
    }
    return new PartitionData(specId, partitionValues);
  }

  Expression fixExpression(Expression filter) {
    if (filter instanceof And andOp) {
      return Expressions.and(fixExpression(andOp.left()), fixExpression(andOp.right()));
    } else if (filter instanceof Or orOp) {
      return Expressions.or(fixExpression(orOp.left()), fixExpression(orOp.right()));
    } else if (filter instanceof Not notOp) {
      return Expressions.not(fixExpression(notOp.child()));
    } else if (filter instanceof UnboundPredicate<?> unboundPredicate) {
      return fixPredicate((UnboundPredicate<Object>) unboundPredicate, IN_PREDICATE_LIMIT);
    } else {
      return filter;
    }
  }

  private Expression fixPredicate(UnboundPredicate<Object> predicate, int literalsLimit) {
    if (predicate.op() != Expression.Operation.IN
        || predicate.literals() == null
        || predicate.literals().size() <= literalsLimit) {
      return predicate;
    }
    return Lists.partition(
            predicate.literals().stream().map(l -> l.value()).collect(Collectors.toList()),
            literalsLimit)
        .stream()
        .reduce(
            (Expression) Expressions.alwaysFalse(),
            (left, list) -> {
              Expression right = Expressions.in(predicate.term(), list.toArray());
              return Expressions.or(left, right);
            },
            (left, right) -> Expressions.or(left, right));
  }
}
