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
package com.arcesium.swiftlake.common;

import static org.assertj.core.api.Assertions.*;

import org.apache.iceberg.StructLike;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DataFileTest {

  @Mock private StructLike mockPartitionData;

  @Mock private org.apache.iceberg.DataFile mockIcebergDataFile;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void testConstructorWithPaths() {
    String sourcePath = "/source/path.parquet";
    String destPath = "/dest/path.parquet";
    DataFile dataFile = new DataFile(sourcePath, destPath);

    assertThat(dataFile.getSourceDataFilePath()).isEqualTo(sourcePath);
    assertThat(dataFile.getDestinationDataFilePath()).isEqualTo(destPath);
    assertThat(dataFile.getPartitionData()).isNull();
  }

  @Test
  void testConstructorWithPartitionDataAndPaths() {
    String sourcePath = "/source/path.parquet";
    String destPath = "/dest/path.parquet";
    DataFile dataFile = new DataFile(mockPartitionData, sourcePath, destPath);

    assertThat(dataFile.getSourceDataFilePath()).isEqualTo(sourcePath);
    assertThat(dataFile.getDestinationDataFilePath()).isEqualTo(destPath);
    assertThat(dataFile.getPartitionData()).isEqualTo(mockPartitionData);
  }

  @Test
  void testGetPartitionData() {
    DataFile dataFile =
        new DataFile(mockPartitionData, "/source/path.parquet", "/dest/path.parquet");
    assertThat(dataFile.getPartitionData()).isEqualTo(mockPartitionData);
  }

  @Test
  void testGetSourceDataFilePath() {
    String sourcePath = "/source/path.parquet";
    DataFile dataFile = new DataFile(sourcePath, "/dest/path.parquet");
    assertThat(dataFile.getSourceDataFilePath()).isEqualTo(sourcePath);
  }

  @Test
  void testGetDestinationDataFilePath() {
    String destPath = "/dest/path.parquet";
    DataFile dataFile = new DataFile("/source/path.parquet", destPath);
    assertThat(dataFile.getDestinationDataFilePath()).isEqualTo(destPath);
  }

  @Test
  void testSetAndGetIcebergDataFile() {
    DataFile dataFile = new DataFile("/source/path.parquet", "/dest/path.parquet");
    assertThat(dataFile.getIcebergDataFile()).isNull();

    dataFile.setIcebergDataFile(mockIcebergDataFile);
    assertThat(dataFile.getIcebergDataFile()).isEqualTo(mockIcebergDataFile);
  }

  @Test
  void testToString() {
    String sourcePath = "/source/path.parquet";
    String destPath = "/dest/path.parquet";
    DataFile dataFile = new DataFile(sourcePath, destPath);

    String expectedString =
        "DataFile{sourceDataFilePath='"
            + sourcePath
            + "', destinationDataFilePath='"
            + destPath
            + "'}";
    assertThat(dataFile.toString()).isEqualTo(expectedString);
  }

  @Test
  void testConstructorWithNullPaths() {
    DataFile dataFile = new DataFile(null, null);
    assertThat(dataFile.getSourceDataFilePath()).isNull();
    assertThat(dataFile.getDestinationDataFilePath()).isNull();
  }

  @Test
  void testConstructorWithNullPartitionData() {
    DataFile dataFile = new DataFile(null, "/source/path.parquet", "/dest/path.parquet");
    assertThat(dataFile.getPartitionData()).isNull();
  }
}
