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

import org.apache.iceberg.StructLike;

/**
 * Represents a data file with source and destination paths, partition data, and an associated
 * Iceberg DataFile.
 */
public class DataFile {
  private StructLike partitionData;
  private String sourceDataFilePath;
  private String destinationDataFilePath;

  private org.apache.iceberg.DataFile icebergDataFile;

  /**
   * Constructs a DataFile with source and destination paths.
   *
   * @param sourceDataFilePath The path of the source data file.
   * @param destinationDataFilePath The path of the destination data file.
   */
  public DataFile(String sourceDataFilePath, String destinationDataFilePath) {
    this.sourceDataFilePath = sourceDataFilePath;
    this.destinationDataFilePath = destinationDataFilePath;
  }

  /**
   * Constructs a DataFile with partition data, source and destination paths.
   *
   * @param partitionData The partition data associated with this file.
   * @param sourceDataFilePath The path of the source data file.
   * @param destinationDataFilePath The path of the destination data file.
   */
  public DataFile(
      StructLike partitionData, String sourceDataFilePath, String destinationDataFilePath) {
    this.partitionData = partitionData;
    this.sourceDataFilePath = sourceDataFilePath;
    this.destinationDataFilePath = destinationDataFilePath;
  }

  /**
   * Gets the partition data associated with this file.
   *
   * @return The partition data.
   */
  public StructLike getPartitionData() {
    return partitionData;
  }

  /**
   * Gets the path of the source data file.
   *
   * @return The source data file path.
   */
  public String getSourceDataFilePath() {
    return sourceDataFilePath;
  }

  /**
   * Gets the path of the destination data file.
   *
   * @return The destination data file path.
   */
  public String getDestinationDataFilePath() {
    return destinationDataFilePath;
  }

  /**
   * Gets the associated Iceberg DataFile.
   *
   * @return The Iceberg DataFile.
   */
  public org.apache.iceberg.DataFile getIcebergDataFile() {
    return icebergDataFile;
  }

  /**
   * Sets the associated Iceberg DataFile.
   *
   * @param icebergDataFile The Iceberg DataFile to set.
   */
  public void setIcebergDataFile(org.apache.iceberg.DataFile icebergDataFile) {
    this.icebergDataFile = icebergDataFile;
  }

  /**
   * Returns a string representation of the DataFile.
   *
   * @return A string containing the source and destination file paths.
   */
  @Override
  public String toString() {
    return "DataFile{sourceDataFilePath='"
        + sourceDataFilePath
        + '\''
        + ", destinationDataFilePath='"
        + destinationDataFilePath
        + '\''
        + '}';
  }
}
