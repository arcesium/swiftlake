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
import com.arcesium.swiftlake.commands.WriteUtil;
import com.arcesium.swiftlake.common.FileUtil;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.dao.CommonDao;
import com.arcesium.swiftlake.io.SwiftLakeFileIO;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for data file writers in SwiftLake. */
public abstract class BaseDataFileWriter implements DataFileWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseDataFileWriter.class);
  private static final String DUCKDB_NESTED_FIELD_ID_KEY = "__duckdb_field_id";
  public static final String PARQUET_ROW_GROUP_SIZE = "write.parquet.row-group-size";
  public static final long PARQUET_ROW_GROUP_SIZE_DEFAULT = 122880;
  public static final String PARQUET_COMPRESSION_DEFAULT = "ZSTD";

  protected final SwiftLakeEngine swiftLakeEngine;
  protected final Table table;
  protected final String compression;
  protected final SwiftLakeFileIO fileIO;
  private final CommonDao commonDao;

  /**
   * Constructor for BaseDataFileWriter.
   *
   * @param swiftLakeEngine The SwiftLake engine instance
   * @param table The Iceberg table
   */
  BaseDataFileWriter(SwiftLakeEngine swiftLakeEngine, Table table) {
    this.swiftLakeEngine = swiftLakeEngine;
    this.commonDao = swiftLakeEngine.getCommonDao();
    this.table = table;
    this.fileIO = (SwiftLakeFileIO) table.io();
    this.compression =
        PropertyUtil.propertyAsString(
            table.properties(), TableProperties.PARQUET_COMPRESSION, PARQUET_COMPRESSION_DEFAULT);
  }

  /**
   * Abstract method to write data files.
   *
   * @return List of DataFile objects representing the written files
   */
  @Override
  public abstract List<com.arcesium.swiftlake.common.DataFile> write();

  /**
   * Executes SQL and creates local data files.
   *
   * @param stmt Statement object for executing SQL
   * @param sql SQL query to execute
   * @param outputDir Directory to output files
   * @param schema Schema of the data
   * @param sortOrder Sort order for the data
   * @param targetFileSizeBytes Target file size in bytes
   * @param additionalColumns Additional columns to include
   * @param writeToPerThreadParquetFile Whether to write to per-thread Parquet files
   * @param rowGroupSize Size of row groups
   * @param columns List of columns to include
   * @return List of file paths created
   */
  protected List<String> executeSqlAndCreateLocalDataFiles(
      Statement stmt,
      String sql,
      String outputDir,
      Schema schema,
      SortOrder sortOrder,
      Long targetFileSizeBytes,
      List<String> additionalColumns,
      boolean writeToPerThreadParquetFile,
      Long rowGroupSize,
      List<String> columns) {

    sql =
        WriteUtil.getSqlWithProjection(
            swiftLakeEngine, schema, sql, additionalColumns, false, columns);

    Long targetFileSizeBytesForSplit = null;
    if (sortOrderExists(sortOrder) && targetFileSizeBytes != null) {
      targetFileSizeBytesForSplit = targetFileSizeBytes;
      targetFileSizeBytes = null;
    }

    if (sortOrderExists(sortOrder) && writeToPerThreadParquetFile) {
      LOGGER.debug(
          "Ignoring per thread file request. It is not supported when sort order is provided. ");
      writeToPerThreadParquetFile = false;
    }

    String outputPath;
    if (targetFileSizeBytes != null || writeToPerThreadParquetFile) {
      outputPath = outputDir + "/" + getUniqueName("");
    } else {
      outputPath = outputDir + "/" + getUniqueParquetFileName();
    }

    executeCopyToFile(
        stmt,
        sql,
        outputPath,
        schema,
        sortOrder,
        targetFileSizeBytes,
        writeToPerThreadParquetFile,
        rowGroupSize);

    if (targetFileSizeBytesForSplit == null) {
      if (targetFileSizeBytes != null || writeToPerThreadParquetFile) {
        return FileUtil.getFilePathsInFolder(outputPath);
      } else {
        return Arrays.asList(outputPath);
      }
    } else {
      return WriteUtil.splitParquetFile(
          outputPath, outputDir + "/" + getUniqueName(""), targetFileSizeBytesForSplit);
    }
  }

  /**
   * Executes a COPY TO FILE command to write data to a file.
   *
   * @param stmt Statement object for executing SQL
   * @param sql SQL query to execute
   * @param outputPath Path to output file
   * @param schema Schema of the data
   * @param sortOrder Sort order for the data
   * @param targetFileSizeBytes Target file size in bytes
   * @param writeToPerThreadParquetFile Whether to write to per-thread Parquet files
   * @param rowGroupSize Size of row groups
   */
  private void executeCopyToFile(
      Statement stmt,
      String sql,
      String outputPath,
      Schema schema,
      SortOrder sortOrder,
      Long targetFileSizeBytes,
      boolean writeToPerThreadParquetFile,
      Long rowGroupSize) {

    String fieldIds = getIcebergTableFieldIdsForDuckDB(schema);
    String orderBy = getOrderBySql(sortOrder);

    StringJoiner options = new StringJoiner(", ");
    options.add("FORMAT 'PARQUET'");
    options.add("COMPRESSION '" + compression + "'");
    options.add("FIELD_IDS " + fieldIds);

    if (targetFileSizeBytes != null) {
      options.add(String.format("FILE_SIZE_BYTES %d", targetFileSizeBytes));
    } else if (writeToPerThreadParquetFile) {
      options.add("PER_THREAD_OUTPUT True");
    }

    if (rowGroupSize != null) {
      options.add(String.format("ROW_GROUP_SIZE %d", rowGroupSize));
    }

    String copySql = commonDao.getCopyToFileSql(sql, orderBy, outputPath, options.toString());

    try {
      LOGGER.debug("Running copy query: {}", copySql);
      stmt.executeUpdate(copySql);
      LOGGER.debug("Completed copy query.");
    } catch (SQLException e) {
      throw new SwiftLakeException(e, "An error occurred while executing query.");
    }
  }

  /**
   * Generates a unique Parquet file name.
   *
   * @return A unique Parquet file name
   */
  protected String getUniqueParquetFileName() {
    return FileFormat.PARQUET.addExtension(
        "data_" + UUID.randomUUID().toString().replace("-", "_"));
  }

  /**
   * Generates a unique name with a given prefix.
   *
   * @param prefix Prefix for the unique name
   * @return A unique name
   */
  protected String getUniqueName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "_");
  }

  /**
   * Gets the list of projected columns.
   *
   * @return List of projected column names
   */
  protected List<String> getProjectedColumns() {
    return swiftLakeEngine
        .getSchemaEvolution()
        .getProjectionListWithTypeCasting(table.schema().asStruct().fields(), null);
  }

  /**
   * Checks if a sort order exists.
   *
   * @param sortOrder Sort order to check
   * @return true if sort order exists, false otherwise
   */
  protected boolean sortOrderExists(SortOrder sortOrder) {
    return sortOrder != null && sortOrder.fields() != null && !sortOrder.fields().isEmpty();
  }

  /**
   * Generates ORDER BY SQL clause from a SortOrder object.
   *
   * @param sortOrder Sort order to convert to SQL
   * @return ORDER BY SQL clause
   */
  private String getOrderBySql(SortOrder sortOrder) {
    if (!sortOrderExists(sortOrder)) return "";

    StringBuilder sb = new StringBuilder();
    sb.append("ORDER BY");
    Schema schema = sortOrder.schema();
    int count = 0;
    for (SortField field : sortOrder.fields()) {
      if (field.transform() != null && !field.transform().isIdentity())
        throw new ValidationException("Transforms not supported in sort order");
      count++;
      if (count > 1) sb.append(",");
      sb.append(" ").append(schema.findField(field.sourceId()).name());
      sb.append(" ").append(field.direction() == SortDirection.ASC ? "ASC" : "DESC");
      sb.append(" ")
          .append(field.nullOrder() == NullOrder.NULLS_FIRST ? "NULLS FIRST" : "NULLS LAST");
    }
    return sb.toString();
  }

  /**
   * Gets Iceberg table field IDs for DuckDB.
   *
   * @param schema Schema to get field IDs from
   * @return String representation of field IDs
   */
  private String getIcebergTableFieldIdsForDuckDB(Schema schema) {
    StringBuilder ids = new StringBuilder();
    ids.append("{");
    getFieldIdsForNestedType(ids, schema.asStruct(), DUCKDB_NESTED_FIELD_ID_KEY);
    ids.append("}");
    return ids.toString();
  }

  /**
   * Gets field IDs for a nested type and appends them to the provided StringBuilder.
   *
   * @param ids StringBuilder to append the field IDs to
   * @param nestedType Nested type to get field IDs from
   * @param nestedFieldIdName Name of the nested field ID
   */
  private void getFieldIdsForNestedType(
      StringBuilder ids, Type.NestedType nestedType, String nestedFieldIdName) {
    boolean isFirst = true;
    for (Types.NestedField field : nestedType.fields()) {
      if (!isFirst) {
        ids.append(",");
      }
      isFirst = false;
      getFieldIdsForNestedField(ids, field, nestedFieldIdName);
    }
  }

  /**
   * Builds a string representation of field IDs for a nested field.
   *
   * @param ids StringBuilder to append the field IDs
   * @param field Nested field to get IDs from
   * @param nestedFieldIdName Name of the nested field ID
   */
  private void getFieldIdsForNestedField(
      StringBuilder ids, Types.NestedField field, String nestedFieldIdName) {
    ids.append(field.name()).append(": ");
    if (field.type().isNestedType()) {
      ids.append("{").append(nestedFieldIdName).append(": ").append(field.fieldId()).append(", ");
      getFieldIdsForNestedType(ids, field.type().asNestedType(), nestedFieldIdName);
      ids.append("}");
    } else {
      ids.append(field.fieldId());
    }
  }

  /**
   * Prepares new data files by validating and collecting metrics.
   *
   * @param table Iceberg table
   * @param addFiles List of files to add
   * @param sortOrder Sort order of the data
   * @return List of prepared DataFile objects
   */
  protected List<com.arcesium.swiftlake.common.DataFile> prepareNewDataFiles(
      Table table, List<com.arcesium.swiftlake.common.DataFile> addFiles, SortOrder sortOrder) {
    if (addFiles.isEmpty()) return new ArrayList<>();

    List<com.arcesium.swiftlake.common.DataFile> newFiles = new ArrayList<>();
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    PartitionSpec spec = table.spec();
    for (com.arcesium.swiftlake.common.DataFile addFile : addFiles) {
      InputFile inputFile = org.apache.iceberg.Files.localInput(addFile.getSourceDataFilePath());
      Metrics metrics = null;
      List<Long> splitOffsets = null;
      try (ParquetFileReader reader =
          com.arcesium.swiftlake.common.ParquetUtil.getParquetFileReader(inputFile)) {
        ParquetMetadata metadata = reader.getFooter();
        Schema fileSchema = validateSchema(table, inputFile, metadata);
        metrics = getMetrics(metadata, metricsConfig, fileSchema);
        if (metrics.recordCount() == 0) {
          continue;
        }
        checkNullability(table, metrics);
        splitOffsets = ParquetUtil.getSplitOffsets(metadata);
      } catch (IOException e) {
        throw new SwiftLakeException(e, "Failed to read footer of file: %s", inputFile);
      }

      DataFiles.Builder builder =
          DataFiles.builder(spec)
              .withPath(addFile.getDestinationDataFilePath())
              .withFileSizeInBytes(inputFile.getLength())
              .withFormat(FileFormat.PARQUET)
              .withMetrics(metrics)
              .withPartition(addFile.getPartitionData())
              .withSplitOffsets(splitOffsets)
              .withSortOrder(sortOrder);

      addFile.setIcebergDataFile(builder.build());
      newFiles.add(addFile);
    }

    return newFiles;
  }

  /**
   * Validates the schema of a Parquet file against the table schema.
   *
   * @param table Iceberg table
   * @param inputFile Input file to validate
   * @param metadata Parquet metadata of the file
   * @return Validated Schema object
   */
  private Schema validateSchema(Table table, InputFile inputFile, ParquetMetadata metadata) {
    MessageType messageType = metadata.getFileMetaData().getSchema();
    // Check if any extra columns exist in the data file
    if (com.arcesium.swiftlake.common.ParquetUtil.hasNullIds(messageType)) {
      throw new ValidationException(
          "Null ids found in one ore more fields. File schema is invalid.");
    }
    // Converts a Parquet schema to an Iceberg schema and prunes fields without IDs
    Schema fileSchema = ParquetSchemaUtil.convertAndPrune(messageType);

    Type tableSchemaType =
        swiftLakeEngine.getSchemaEvolution().createTypeForComparison(table.schema().asStruct());
    Type fileSchemaType =
        swiftLakeEngine.getSchemaEvolution().createTypeForComparison(fileSchema.asStruct());
    if (!tableSchemaType.equals(fileSchemaType)) {
      throw new ValidationException(
          "Schema mismatch between table %s and the file %s\nTable Schema:\n%s\nFile Schema:\n%s",
          table.name(), inputFile, tableSchemaType, fileSchemaType);
    }

    return fileSchema;
  }

  /**
   * Collects metrics from Parquet metadata.
   *
   * @param metadata Parquet metadata
   * @param metricsConfig Metrics configuration
   * @param fileSchema Schema of the file
   * @return Metrics object
   */
  private Metrics getMetrics(
      ParquetMetadata metadata, MetricsConfig metricsConfig, Schema fileSchema) {
    Metrics metrics = ParquetUtil.footerMetrics(metadata, Stream.empty(), metricsConfig, null);
    Set<Integer> fieldIdsWithoutNullCounts = new HashSet<>();

    for (BlockMetaData block : metadata.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        Statistics stats = column.getStatistics();
        if (stats != null && !stats.isEmpty() && stats.getNumNulls() < 0) {
          Integer fieldId = fileSchema.aliasToId(column.getPath().toDotString());
          if (fieldId != null) {
            fieldIdsWithoutNullCounts.add(fieldId);
          }
        }
      }
    }
    fieldIdsWithoutNullCounts.forEach(k -> metrics.nullValueCounts().remove(k));
    return metrics;
  }

  /**
   * Checks for null values in required fields.
   *
   * @param table Iceberg table
   * @param metrics Metrics of the file
   */
  private void checkNullability(Table table, Metrics metrics) {
    Schema schema = table.schema();
    List<String> errors = new ArrayList<>();

    if (metrics.nullValueCounts() != null) {
      metrics
          .nullValueCounts()
          .entrySet()
          .forEach(
              nv -> {
                if (nv.getValue() > 0) {
                  Types.NestedField f = schema.findField(nv.getKey());
                  if (f.isRequired()) {
                    errors.add(f.name());
                  }
                }
              });
    }

    if (!errors.isEmpty()) {
      throw new ValidationException(
          "Required fields cannot contain null values - %s",
          errors.stream().collect(Collectors.joining(",")));
    }
  }

  /**
   * Generates a new file path for a data file.
   *
   * @param table Iceberg table
   * @param partitionData Partition data
   * @param fileName Name of the file
   * @return New file path
   */
  protected String getNewFilePath(Table table, StructLike partitionData, String fileName) {
    PartitionSpec spec = table.spec();
    if (partitionData == null) {
      if (spec.isPartitioned())
        throw new ValidationException("Partitioning exists on table. Provide partition data.");
      return table.locationProvider().newDataLocation(fileName);
    } else {
      if (!spec.isPartitioned())
        throw new ValidationException("Partitioning does not exist on table.");
      return table.locationProvider().newDataLocation(spec, partitionData, fileName);
    }
  }
}
