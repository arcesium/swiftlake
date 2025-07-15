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
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles writing of unpartitioned data files for Swift Lake. */
public class UnpartitionedDataFileWriter extends BaseDataFileWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnpartitionedDataFileWriter.class);
  private final String tmpDir;
  private final String sql;
  private final Schema schema;
  private final SortOrder sortOrder;
  private final Long targetFileSizeBytes;
  private final List<String> additionalColumns;
  private final boolean writeToPerThreadParquetFile;
  private final Long rowGroupSize;
  private final List<String> columns;

  /** Private constructor for UnpartitionedDataFileWriter. */
  private UnpartitionedDataFileWriter(
      SwiftLakeEngine swiftLakeEngine,
      Table table,
      String sql,
      Schema schema,
      SortOrder sortOrder,
      Long targetFileSizeBytes,
      List<String> additionalColumns,
      Boolean writeToPerThreadParquetFile,
      String tmpDir,
      Long rowGroupSize,
      List<String> columns) {
    super(swiftLakeEngine, table);
    this.sql = sql;
    this.schema = schema;
    this.sortOrder = sortOrder;
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.additionalColumns = additionalColumns;
    this.writeToPerThreadParquetFile =
        writeToPerThreadParquetFile == null ? false : writeToPerThreadParquetFile;
    this.rowGroupSize = rowGroupSize;
    this.columns = columns;

    this.tmpDir =
        (tmpDir == null ? swiftLakeEngine.getLocalDir() + "/" + getUniqueName("updfw") : tmpDir);
    new File(this.tmpDir).mkdirs();
  }

  /**
   * Writes the data files and returns a list of DataFile objects.
   *
   * @return List of DataFile objects
   */
  @Override
  public List<DataFile> write() {
    try {
      List<String> localFilePaths = createLocalDataFiles();
      return uploadDataFiles(table, localFilePaths);
    } finally {
      try {
        FileUtils.deleteDirectory(new File(tmpDir));
      } catch (IOException e) {
        LOGGER.warn("Unable to delete the temporary directory {}", tmpDir);
      }
    }
  }

  /**
   * Creates local data files based on the SQL query.
   *
   * @return List of local file paths
   */
  public List<String> createLocalDataFiles() {
    try (Connection connection = swiftLakeEngine.createConnection(false);
        Statement stmt = connection.createStatement()) {
      return executeSqlAndCreateLocalDataFiles(
          stmt,
          sql,
          tmpDir,
          schema,
          sortOrder,
          targetFileSizeBytes,
          additionalColumns,
          writeToPerThreadParquetFile,
          rowGroupSize,
          columns);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generates a new file path for a given file name.
   *
   * @param table The Iceberg table
   * @param fileName The name of the file
   * @return The new file path
   */
  private String getNewFilePath(Table table, String fileName) {
    return getNewFilePath(table, null, fileName);
  }

  /**
   * Uploads the local data files to the target location.
   *
   * @param table The Iceberg table
   * @param localFilePaths List of local file paths
   * @return List of DataFile objects
   */
  private List<DataFile> uploadDataFiles(Table table, List<String> localFilePaths) {
    List<DataFile> dataFileDetails =
        localFilePaths.stream()
            .map(
                localFilePath -> {
                  String newDataFilePath = getNewFilePath(table, getUniqueParquetFileName());
                  return new DataFile(localFilePath, newDataFilePath);
                })
            .collect(Collectors.toList());

    List<DataFile> dataFiles = this.prepareNewDataFiles(table, dataFileDetails, sortOrder);
    fileIO.uploadFiles(dataFiles);
    return dataFiles;
  }

  /**
   * Creates a new Builder instance for UnpartitionedDataFileWriter.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param table The Iceberg table
   * @param sql The SQL query
   * @return A new Builder instance
   */
  public static Builder builderFor(SwiftLakeEngine swiftLakeEngine, Table table, String sql) {
    return new Builder(swiftLakeEngine, table, sql);
  }

  /** Builder class for UnpartitionedDataFileWriter. */
  public static class Builder {
    private final SwiftLakeEngine swiftLakeEngine;
    private final Table table;
    private final String sql;
    private Schema schema;
    private SortOrder sortOrder;
    private Long targetFileSizeBytes;
    private List<String> additionalColumns;
    private Boolean writeToPerThreadParquetFile;
    private String tmpDir;
    private Long rowGroupSize;
    private List<String> columns;

    /** Private constructor for Builder. */
    private Builder(SwiftLakeEngine swiftLakeEngine, Table table, String sql) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = table;
      this.sql = sql;
      this.schema = table.schema();
      this.sortOrder = table.sortOrder();
      Map<String, String> props = table.properties();
      this.targetFileSizeBytes =
          PropertyUtil.propertyAsLong(
              props,
              TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
              TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
      this.rowGroupSize =
          PropertyUtil.propertyAsLong(
              props, PARQUET_ROW_GROUP_SIZE, PARQUET_ROW_GROUP_SIZE_DEFAULT);
    }

    /**
     * Sets the schema for the writer.
     *
     * @param schema The Iceberg schema
     * @return This Builder instance
     */
    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /**
     * Sets the sort order for the writer.
     *
     * @param sortOrder The Iceberg sort order
     * @return This Builder instance
     */
    public Builder sortOrder(SortOrder sortOrder) {
      this.sortOrder = sortOrder;
      return this;
    }

    /**
     * Sets whether to skip data sorting.
     *
     * @param skipDataSorting True to skip data sorting, false otherwise
     * @return This Builder instance
     */
    public Builder skipDataSorting(boolean skipDataSorting) {
      if (skipDataSorting) {
        this.sortOrder = null;
      }
      return this;
    }

    /**
     * Sets the target file size in bytes.
     *
     * @param targetFileSizeBytes The target file size in bytes
     * @return This Builder instance
     */
    public Builder targetFileSizeBytes(Long targetFileSizeBytes) {
      this.targetFileSizeBytes = targetFileSizeBytes;
      return this;
    }

    /**
     * Sets additional columns to be included.
     *
     * @param additionalColumns List of additional column names
     * @return This Builder instance
     */
    public Builder additionalColumns(List<String> additionalColumns) {
      this.additionalColumns = additionalColumns;
      return this;
    }

    /**
     * Sets whether to write to per-thread Parquet files.
     *
     * @param writeToPerThreadParquetFile True to write to per-thread Parquet files, false otherwise
     * @return This Builder instance
     */
    public Builder writeToPerThreadParquetFile(Boolean writeToPerThreadParquetFile) {
      this.writeToPerThreadParquetFile = writeToPerThreadParquetFile;
      return this;
    }

    /**
     * Sets the temporary directory for file creation.
     *
     * @param tmpDir The temporary directory path
     * @return This Builder instance
     */
    public Builder tmpDir(String tmpDir) {
      this.tmpDir = tmpDir;
      return this;
    }

    /**
     * Sets the row group size for Parquet files.
     *
     * @param rowGroupSize The row group size
     * @return This Builder instance
     */
    public Builder rowGroupSize(Long rowGroupSize) {
      this.rowGroupSize = rowGroupSize;
      return this;
    }

    /**
     * Sets the columns to be included in the output.
     *
     * @param columns List of column names
     * @return This Builder instance
     */
    public Builder columns(List<String> columns) {
      this.columns = columns;
      return this;
    }

    /**
     * Builds and returns a new UnpartitionedDataFileWriter instance.
     *
     * @return A new UnpartitionedDataFileWriter instance
     */
    public UnpartitionedDataFileWriter build() {
      return new UnpartitionedDataFileWriter(
          swiftLakeEngine,
          table,
          sql,
          schema,
          sortOrder,
          targetFileSizeBytes,
          additionalColumns,
          writeToPerThreadParquetFile,
          tmpDir,
          rowGroupSize,
          columns);
    }
  }
}
