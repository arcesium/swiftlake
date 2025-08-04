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
import com.arcesium.swiftlake.common.DateTimeUtil;
import com.arcesium.swiftlake.common.FileUtil;
import com.arcesium.swiftlake.common.InputFile;
import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.dao.CommonDao;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.io.SwiftLakeFileIO;
import com.arcesium.swiftlake.metrics.PartitionData;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import com.arcesium.swiftlake.sql.SqlQueryProcessor;
import com.arcesium.swiftlake.sql.TableFilter;
import com.arcesium.swiftlake.sql.TableScanResult;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class providing various helper methods for write operations in the SwiftLake system. */
public class WriteUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteUtil.class);

  /**
   * Checks the cardinality of a merge operation for a given table.
   *
   * @param commonDao The CommonDao instance to perform the check
   * @param table The name of the table to check
   * @throws ValidationException if a single row from the target table matches multiple rows from
   *     the source data
   */
  public static void checkMergeCardinality(CommonDao commonDao, String table) {
    if (commonDao.mergeCardinalityCheck(table)) {
      throw new ValidationException(
          "Merge operation matched a single row from target table data with multiple rows of the source data.");
    }
  }

  /**
   * Retrieves the modified Iceberg data files based on the table scan result and modified local
   * files.
   *
   * @param tableScanResult The result of the table scan
   * @param modifiedLocalFiles List of modified local file paths
   * @return List of DataFile objects representing the modified Iceberg data files
   */
  public static List<DataFile> getModifiedIcebergDataFiles(
      TableScanResult tableScanResult, List<String> modifiedLocalFiles) {
    if (modifiedLocalFiles == null || modifiedLocalFiles.isEmpty()) return new ArrayList<>();

    Map<String, String> fileLocationsMap =
        tableScanResult.getInputFiles().getInputFiles().stream()
            .collect(Collectors.toMap(InputFile::getLocalFileLocation, InputFile::getLocation));

    Set<String> modifiedRemoteFiles =
        modifiedLocalFiles.stream().map(k -> fileLocationsMap.get(k)).collect(Collectors.toSet());
    return tableScanResult.getScanResult().getRight().stream()
        .filter(f -> modifiedRemoteFiles.contains(f.location().toString()))
        .collect(Collectors.toList());
  }

  /**
   * Gets the list of column names for a given table.
   *
   * @param table The Table object to extract column names from
   * @return List of column names
   */
  public static List<String> getColumns(Table table) {
    return table.schema().asStruct().fields().stream()
        .map(f -> f.name())
        .collect(Collectors.toList());
  }

  /**
   * Validates that the given columns exist in the table schema.
   *
   * @param table The Table object to validate against
   * @param columns List of column names to validate
   * @throws ValidationException if any column is not found in the table schema
   */
  public static void validateColumns(Table table, List<String> columns) {
    if (columns == null || columns.isEmpty()) return;
    Set<String> allColumnsSet = new HashSet<>(getColumns(table));
    for (String column : columns) {
      ValidationException.check(allColumnsSet.contains(column), "Invalid column %s", column);
    }
  }

  /**
   * Validates that the filter columns are a subset of the key columns.
   *
   * @param filterColumns List of filter column names
   * @param keyColumns List of key column names
   * @throws ValidationException if any filter column is not a key column
   */
  public static void validateTableFilterColumns(
      List<String> filterColumns, List<String> keyColumns) {
    if (filterColumns == null || filterColumns.isEmpty()) return;
    Set<String> keyColumnsSet = new HashSet<>(keyColumns);
    for (String column : filterColumns) {
      ValidationException.check(
          keyColumnsSet.contains(column), "Filter column %s is not a key column", column);
    }
  }

  /**
   * Gets the list of non-key columns from all columns.
   *
   * @param allColumns List of all column names
   * @param keyColumns List of key column names
   * @return List of non-key column names
   */
  public static List<String> getRemainingColumns(List<String> allColumns, List<String> keyColumns) {
    Set<String> keyColumnsSet = new HashSet<>(keyColumns);
    List<String> remainingColumns = new ArrayList<>();
    for (String column : allColumns) {
      if (!keyColumnsSet.contains(column)) {
        remainingColumns.add(column);
      }
    }
    return remainingColumns;
  }

  /**
   * Retrieves SQL from the SwiftLakeSqlSessionFactory.
   *
   * @param sqlSessionFactory The SQL session factory
   * @param id The SQL identifier
   * @param parameter The parameter object
   * @return The SQL string
   * @throws ValidationException if id or sqlSessionFactory is null
   */
  public static String getSql(
      SwiftLakeSqlSessionFactory sqlSessionFactory, String id, Object parameter) {
    ValidationException.checkNotNull(id, "Sql id is null.");
    ValidationException.checkNotNull(sqlSessionFactory, "SqlSessionFactory is null.");
    return sqlSessionFactory.getSql(id, parameter);
  }

  /**
   * Gets the condition pair for a table.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param sqlQueryProcessor The SQL query processor
   * @param table The table
   * @param condition The condition expression
   * @param conditionSql The condition SQL
   * @return A pair containing the condition SQL and the condition expression
   * @throws ValidationException if conditionSql is null when condition is null
   */
  public static Pair<String, Expression> getCondition(
      SwiftLakeEngine swiftLakeEngine,
      SqlQueryProcessor sqlQueryProcessor,
      Table table,
      Expression condition,
      String conditionSql) {
    if (condition != null) {
      condition = Expressions.resolveExpression(condition);
      conditionSql = swiftLakeEngine.getSchemaEvolution().getDuckDBFilterSql(table, condition);
    } else {
      ValidationException.checkNotNull(conditionSql, "Table filter condition cannot be null.");
      condition = sqlQueryProcessor.parseConditionExpression(conditionSql, table.schema(), null);
      condition = Expressions.resolveExpression(condition);
    }
    return Pair.of(conditionSql, condition);
  }

  /**
   * Gets the equality condition expression for a table.
   *
   * @param commonDao The common DAO
   * @param sourceSqlTable The source SQL table
   * @param table The table
   * @param columns The list of columns
   * @return The equality condition expression
   */
  public static Expression getEqualityConditionExpression(
      CommonDao commonDao, String sourceSqlTable, Table table, List<String> columns) {
    List<Map<String, Object>> distinctRecords =
        commonDao.getDistinctValues(sourceSqlTable, columns);
    return getEqualityConditionExpression(table.schema(), columns, distinctRecords);
  }

  /**
   * Gets the equality condition expression for a schema and data.
   *
   * @param schema The schema
   * @param columns The list of columns
   * @param data The list of data maps
   * @return The equality condition expression
   */
  public static Expression getEqualityConditionExpression(
      Schema schema, List<String> columns, List<Map<String, Object>> data) {
    Expression expression = Expressions.alwaysFalse();
    for (Map<String, Object> record : data) {

      Expression recordExpr = Expressions.alwaysTrue();
      for (String col : columns) {
        Object value = record.get(col);
        Type type = schema.findType(col);
        if (value == null) {
          recordExpr = Expressions.and(recordExpr, Expressions.isNull(col));
        } else {
          recordExpr =
              Expressions.and(
                  recordExpr, Expressions.equal(col, getValueForConditionExpression(value, type)));
        }
      }

      expression = Expressions.or(expression, recordExpr);
    }
    return expression;
  }

  private static Object getValueForConditionExpression(Object value, Type dataType) {
    ValidationException.check(dataType.isPrimitiveType(), "Data type should be a primitive type");
    try {
      Type.TypeID typeId = dataType.typeId();
      if (typeId == Type.TypeID.STRING) {
        return value.toString();
      } else if (typeId == Type.TypeID.BOOLEAN) {
        ValidationException.check(value instanceof Boolean, "Invalid boolean value %s", value);
        return value;
      } else if (typeId == Type.TypeID.INTEGER) {
        if (value instanceof Long longValue) return longValue.intValue();
        if (value instanceof Integer) return value;

        throw new ValidationException("Invalid integer value %s", value);
      } else if (typeId == Type.TypeID.LONG) {
        if (value instanceof Integer integerValue) return integerValue.longValue();

        if (value instanceof Long) return value;

        throw new ValidationException("Invalid long value %s", value);
      } else if (typeId == Type.TypeID.FLOAT) {
        if (value instanceof Double doubleValue) return doubleValue.floatValue();
        if (value instanceof Float) return value;

        throw new ValidationException("Invalid float value %s", value);
      } else if (typeId == Type.TypeID.DOUBLE) {
        if (value instanceof Float floatValue) return floatValue.doubleValue();
        if (value instanceof Double) return value;

        throw new ValidationException("Invalid double value %s", value);
      } else if (typeId == Type.TypeID.DECIMAL) {
        if (value instanceof Integer integerValue) return new BigDecimal(integerValue);
        if (value instanceof Long longValue) return new BigDecimal(longValue);
        if (value instanceof Float floatValue) return new BigDecimal(floatValue);
        if (value instanceof Double doubleValue) return new BigDecimal(doubleValue);
        if (value instanceof BigDecimal) return value;

        throw new ValidationException("Invalid decimal value %s", value);
      } else if (typeId == Type.TypeID.DATE
          || typeId == Type.TypeID.TIME
          || typeId == Type.TypeID.TIMESTAMP) {
        return getFormattedValueForDateTimes(dataType, value);
      }
    } catch (Exception e) {
      throw new SwiftLakeException(
          e, "An error occurred while converting value %s to data type %s", value, dataType);
    }

    throw new ValidationException("Unsupported value %s with type %s", value, dataType);
  }

  /**
   * Gets the SQL with projection.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param schema The schema
   * @param sql The SQL string
   * @param additionalColumns Additional columns to include
   * @param wrapWithParenthesis Whether to wrap the result with parentheses
   * @param columns The list of columns
   * @return The SQL string with projection
   */
  public static String getSqlWithProjection(
      SwiftLakeEngine swiftLakeEngine,
      Schema schema,
      String sql,
      List<String> additionalColumns,
      boolean wrapWithParenthesis,
      List<String> columns) {
    StringBuilder wrappedSql = new StringBuilder();
    // Wrap sql with the latest schema projection
    String projection =
        swiftLakeEngine
            .getSchemaEvolution()
            .getProjectionExprWithTypeCasting(schema.asStruct().fields(), columns);

    if (wrapWithParenthesis) {
      wrappedSql.append("(");
    }
    wrappedSql.append("SELECT ");
    wrappedSql.append(projection);
    if (additionalColumns != null) {
      wrappedSql.append(",").append(additionalColumns.stream().collect(Collectors.joining(",")));
    }
    wrappedSql.append(" FROM (").append(sql).append(")");
    if (wrapWithParenthesis) {
      wrappedSql.append(")");
    }
    return wrappedSql.toString();
  }

  /**
   * Splits a Parquet file into multiple files.
   *
   * @param inputFilePath The input file path
   * @param outputPath The output path
   * @param targetFileSizeBytes The target file size in bytes
   * @return A list of output file paths
   * @throws ValidationException if targetFileSizeBytes is not positive
   * @throws SwiftLakeException if an error occurs during file splitting
   */
  public static List<String> splitParquetFile(
      String inputFilePath, String outputPath, long targetFileSizeBytes) {
    ValidationException.check(
        targetFileSizeBytes > 0, "targetFileSizeBytes should be positive value");
    double tolerancePct = 0.1;
    File inputFile = new File(inputFilePath);
    if (inputFile.length() <= (targetFileSizeBytes * (1 + tolerancePct))) {
      return Arrays.asList(inputFilePath);
    }
    List<BlockMetaData> blockMetaDataList = null;
    MessageType schema = null;
    Map<String, String> keyValueMetaData = null;

    try (ParquetFileReader reader =
        ParquetFileReader.open(
            HadoopInputFile.fromPath(new Path(inputFilePath), new Configuration()))) {
      blockMetaDataList = reader.getRowGroups();
      ParquetMetadata metadata = reader.getFooter();
      schema = metadata.getFileMetaData().getSchema();
      keyValueMetaData = metadata.getFileMetaData().getKeyValueMetaData();
    } catch (IOException e) {
      throw new SwiftLakeException(e, "An error occurred in parquet file split activity.");
    }

    List<List<BlockMetaData>> blocksList = new ArrayList<>();
    if (blockMetaDataList != null && !blockMetaDataList.isEmpty()) {
      long targetSize = targetFileSizeBytes;
      List<BlockMetaData> currentBlocks = new ArrayList<>();
      blocksList.add(currentBlocks);

      for (int i = 0; i < blockMetaDataList.size(); i++) {
        if (targetSize <= 0) {
          targetSize = targetFileSizeBytes;
          currentBlocks = new ArrayList<>();
          blocksList.add(currentBlocks);
        }
        BlockMetaData block = blockMetaDataList.get(i);
        targetSize -= block.getCompressedSize();
        currentBlocks.add(block);
      }
    }

    if (blocksList.size() <= 1) {
      return Arrays.asList(inputFilePath);
    }

    outputPath = FileUtil.stripTrailingSlash(outputPath);

    try (SeekableInputStream stream =
        HadoopInputFile.fromPath(new Path(inputFilePath), new Configuration()).newStream(); ) {

      long blockSize = 122880L * 1024L; // does not matter
      List<String> outputFiles = new ArrayList<>();
      for (int i = 0; i < blocksList.size(); i++) {
        String outPath = outputPath + "/data_" + i + ".parquet";
        ParquetFileWriter writer = null;
        try {
          writer =
              new ParquetFileWriter(
                  HadoopOutputFile.fromPath(new Path(outPath), new Configuration()),
                  schema,
                  ParquetFileWriter.Mode.CREATE,
                  blockSize,
                  0,
                  Integer.MAX_VALUE,
                  Integer.MAX_VALUE,
                  true);
          writer.start();
          writer.appendRowGroups(stream, blocksList.get(i), false);
          writer.end(keyValueMetaData);
          writer = null;
          outputFiles.add(outPath);
        } finally {
          if (writer != null) {
            writer.end(keyValueMetaData);
          }
        }
      }

      return outputFiles;
    } catch (IOException e) {
      throw new SwiftLakeException(e, "An error occurred in parquet file split activity.");
    }
  }

  /**
   * Gets the partition filter expression for a table.
   *
   * @param table The table
   * @param partitionDataList The list of partition data
   * @return The partition filter expression
   */
  public static Expression getPartitionFilterExpression(
      Table table, List<PartitionData> partitionDataList) {
    Schema schema = table.schema();
    Expression orExpr = Expressions.alwaysFalse();
    for (PartitionData partition : partitionDataList) {
      Expression andExpr = Expressions.alwaysTrue();
      for (Pair<PartitionField, Object> pair : partition.getPartitionValues()) {
        Expression cond = getPartitionPredicate(schema, pair.getLeft(), pair.getRight());
        andExpr = Expressions.and(andExpr, cond);
      }
      orExpr = Expressions.or(orExpr, andExpr);
    }

    return orExpr;
  }

  private static Expression getPartitionPredicate(
      Schema schema, PartitionField partitionField, Object value) {
    Types.NestedField field = schema.findField(partitionField.sourceId());
    UnboundTerm<Object> transform =
        org.apache.iceberg.expressions.Expressions.transform(
            field.name(), (Transform<?, Object>) partitionField.transform());
    if (value == null) {
      return Expressions.isNull(transform);
    } else {
      return Expressions.equal(transform, value);
    }
  }

  /**
   * Processes source tables and returns the updated SQL and input files.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param sourceSql The source SQL
   * @param targetTable The target table
   * @return A pair containing the updated SQL and list of input files
   */
  public static Pair<String, List<InputFiles>> processSourceTables(
      SwiftLakeEngine swiftLakeEngine, String sourceSql, Table targetTable) {
    // Process the SQL query to extract table filters
    Pair<String, List<TableFilter>> processingResult =
        swiftLakeEngine.getSqlQueryProcessor().process(sourceSql, null, null, List.of(targetTable));

    List<TableFilter> tableFilters = processingResult.getRight();
    String processedSql = processingResult.getLeft();

    List<InputFiles> resources = new ArrayList<>();
    try {
      // Execute optimized table scans if filters are available
      if (tableFilters != null && !tableFilters.isEmpty()) {
        Pair<String, List<InputFiles>> scanResult =
            swiftLakeEngine
                .getIcebergScanExecutor()
                .executeTableScansAndUpdateSql(processedSql, tableFilters, null, false);

        processedSql = scanResult.getLeft();
        if (scanResult.getRight() != null) {
          resources.addAll(scanResult.getRight());
        }
      }

      return Pair.of(processedSql, resources);
    } catch (Throwable t) {
      closeInputFiles(resources);
      if (t instanceof ValidationException || t instanceof SwiftLakeException) {
        throw t;
      } else if (t instanceof Error error) {
        throw error;
      } else {
        throw new SwiftLakeException(t, "An error occurred while processing source tables.");
      }
    }
  }

  /**
   * Closes the input files.
   *
   * @param inputFiles The list of input files to close
   */
  public static void closeInputFiles(List<InputFiles> inputFiles) {
    if (inputFiles != null) {
      inputFiles.forEach(
          f -> {
            try {
              f.close();
            } catch (Exception e) {
              LOGGER.error("An error occurred while closing resources.");
            }
          });
    }
  }

  /**
   * Uploads debug files to a specified path.
   *
   * @param fileIO The SwiftLakeFileIO instance
   * @param uploadPath The upload path
   * @param sourceFolder The source folder
   * @param prefix The prefix for uploaded files
   */
  public static void uploadDebugFiles(
      SwiftLakeFileIO fileIO, String uploadPath, String sourceFolder, String prefix) {
    if (uploadPath == null) return;

    List<com.arcesium.swiftlake.common.DataFile> files =
        FileUtil.getFilesInFolder(sourceFolder).stream()
            .map(
                f -> {
                  String path = f.getAbsolutePath();
                  return new com.arcesium.swiftlake.common.DataFile(
                      path,
                      FileUtil.concatPaths(
                          uploadPath, prefix, path.substring(sourceFolder.length())));
                })
            .collect(Collectors.toList());

    if (!files.isEmpty()) {
      LOGGER.info(
          "Uploading debug files from {} to {}",
          sourceFolder,
          FileUtil.concatPaths(uploadPath, prefix));
      fileIO.uploadFiles(files);
    }
  }

  /**
   * Uploads a single debug file to a specified path.
   *
   * @param fileIO The SwiftLakeFileIO instance
   * @param uploadPath The upload path
   * @param sourceFilePath The source file path
   * @param prefix The prefix for the uploaded file
   * @param fileName The file name
   */
  public static void uploadDebugFile(
      SwiftLakeFileIO fileIO,
      String uploadPath,
      String sourceFilePath,
      String prefix,
      String fileName) {
    if (uploadPath == null) return;

    com.arcesium.swiftlake.common.DataFile dataFile =
        new com.arcesium.swiftlake.common.DataFile(
            sourceFilePath, FileUtil.concatPaths(uploadPath, prefix, fileName));
    LOGGER.info(
        "Uploading debug file {} to {}",
        dataFile.getSourceDataFilePath(),
        dataFile.getDestinationDataFilePath());
    fileIO.uploadFiles(Arrays.asList(dataFile));
  }

  /**
   * Formats date, time, and timestamp values as strings.
   *
   * @param type The Iceberg Type of the value
   * @param value The value to be formatted
   * @return Formatted string representation of the value
   */
  public static String getFormattedValueForDateTimes(Type type, Object value) {
    Type.TypeID typeId = type.typeId();
    if (typeId == Type.TypeID.DATE) {
      ValidationException.check(
          value instanceof LocalDate || value instanceof java.sql.Date,
          "Invalid date value %s",
          value);
      if (value instanceof java.sql.Date) {
        value = ((java.sql.Date) value).toLocalDate();
      }
      return DateTimeUtil.formatLocalDate((LocalDate) value);
    } else if (typeId == Type.TypeID.TIME) {
      ValidationException.check(value instanceof LocalTime, "Invalid time value %s", value);
      return DateTimeUtil.formatLocalTimeWithMicros((LocalTime) value);
    } else if (typeId == Type.TypeID.TIMESTAMP) {
      if (((Types.TimestampType) type).shouldAdjustToUTC()) {
        ValidationException.check(
            value instanceof OffsetDateTime, "Invalid timestamptz value %s", value);
        return DateTimeUtil.formatOffsetDateTimeWithMicros((OffsetDateTime) value);
      } else {
        ValidationException.check(
            value instanceof LocalDateTime || value instanceof Timestamp,
            "Invalid timestamp value %s",
            value);
        if (value instanceof Timestamp) {
          value = ((Timestamp) value).toLocalDateTime();
        }
        return DateTimeUtil.formatLocalDateTimeWithMicros((LocalDateTime) value);
      }
    } else {
      throw new ValidationException("Invalid type %s", type);
    }
  }
}
