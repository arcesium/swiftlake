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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.arcesium.swiftlake.common.InputFiles;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.TableScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TableScanResultTest {

  @Mock private InputFiles mockInputFiles;

  @Mock private TableScan mockTableScan;

  private List<DataFile> dataFiles;
  private TableScanResult tableScanResult;

  @BeforeEach
  void setUp() {
    String sql = "SELECT * FROM test_table";
    dataFiles = new ArrayList<>();
    Pair<TableScan, List<DataFile>> scanResult = Pair.of(mockTableScan, dataFiles);
    tableScanResult = new TableScanResult(sql, mockInputFiles, scanResult);
  }

  @Test
  void testGetSql() {
    assertThat(tableScanResult.getSql()).isEqualTo("SELECT * FROM test_table");
  }

  @Test
  void testGetInputFiles() {
    assertThat(tableScanResult.getInputFiles()).isEqualTo(mockInputFiles);
  }

  @Test
  void testGetScanResult() {
    Pair<TableScan, List<DataFile>> result = tableScanResult.getScanResult();
    assertThat(result.getLeft()).isEqualTo(mockTableScan);
    assertThat(result.getRight()).isEqualTo(dataFiles);
  }

  @Test
  void testClose() throws Exception {
    tableScanResult.close();
    verify(mockInputFiles, times(1)).close();
  }

  @Test
  void testCloseWithNullInputFiles() throws Exception {
    TableScanResult resultWithNullInputFiles =
        new TableScanResult("SELECT * FROM test_table", null, Pair.of(mockTableScan, dataFiles));

    // This should not throw an exception
    resultWithNullInputFiles.close();
  }

  @Test
  void testCloseWithException() throws Exception {
    doThrow(new RuntimeException("Test exception")).when(mockInputFiles).close();

    // This should not throw an exception, but log an error
    tableScanResult.close();

    // Verify that close was called
    verify(mockInputFiles, times(1)).close();
  }

  @Test
  void testAutoCloseable() throws Exception {
    try (TableScanResult result =
        new TableScanResult(
            "SELECT * FROM test_table", mockInputFiles, Pair.of(mockTableScan, dataFiles))) {
      // Do nothing
    }

    // Verify that close was called
    verify(mockInputFiles, times(1)).close();
  }
}
