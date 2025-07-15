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

import com.arcesium.swiftlake.common.InputFiles;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.TableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the result of a table scan operation. This class implements AutoCloseable to ensure
 * proper resource management.
 */
public class TableScanResult implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableScanResult.class);
  private String sql;
  private InputFiles inputFiles;
  private Pair<TableScan, List<DataFile>> scanResult;

  /**
   * Constructs a TableScanResult with the given SQL, input files, and scan result.
   *
   * @param sql the SQL query string
   * @param inputFiles the InputFiles object containing the input file information
   * @param scanResult a Pair containing the TableScan and a List of DataFiles
   */
  public TableScanResult(
      String sql, InputFiles inputFiles, Pair<TableScan, List<DataFile>> scanResult) {
    this.sql = sql;
    this.inputFiles = inputFiles;
    this.scanResult = scanResult;
  }

  /**
   * Gets the SQL query string.
   *
   * @return the SQL query string
   */
  public String getSql() {
    return sql;
  }

  /**
   * Gets the InputFiles object.
   *
   * @return the InputFiles object
   */
  public InputFiles getInputFiles() {
    return inputFiles;
  }

  /**
   * Gets the scan result as a Pair of TableScan and List of DataFiles.
   *
   * @return a Pair containing the TableScan and a List of DataFiles
   */
  public Pair<TableScan, List<DataFile>> getScanResult() {
    return scanResult;
  }

  /**
   * Closes the resources associated with this TableScanResult. This method is called automatically
   * when using try-with-resources.
   */
  @Override
  public void close() {
    if (inputFiles != null) {
      try {
        inputFiles.close();
        inputFiles = null;
      } catch (Exception e) {
        LOGGER.error("An error occurred while closing resources.");
      }
    }
  }
}
