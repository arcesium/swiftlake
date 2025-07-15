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
package com.arcesium.swiftlake.dao;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.commands.SCD2MergeProperties;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

/**
 * Data Access Object for handling Slowly Changing Dimension Type 2 (SCD2) merge operations. Extends
 * BaseDao to leverage common DAO functionality.
 */
public class SCD2MergeDao extends BaseDao {
  private static final String namespace = "SCD2Merge";

  /**
   * Constructs an SCD2MergeDao with the given SwiftLakeEngine.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance to use for database operations.
   */
  public SCD2MergeDao(SwiftLakeEngine swiftLakeEngine) {
    super(swiftLakeEngine);
  }

  /**
   * Verifies that there are no out-of-order records based on the provided properties.
   *
   * @param properties The SCD2MergeProperties containing the necessary information for verification
   * @return true if there are no out-of-order records, false otherwise
   */
  public boolean verifyNoOutOfOrderRecords(SCD2MergeProperties properties) {
    try (SqlSession session = getSession()) {
      return session.selectOne(namespace + ".verifyNoOutOfOrderRecords", properties);
    }
  }

  /**
   * Performs the merge operation to find differences in the changes-based SCD2 process.
   *
   * @param properties The SCD2MergeProperties containing configuration for the merge operation.
   */
  public void changesBasedSCD2MergeFindDiffs(SCD2MergeProperties properties) {
    try (SqlSession session = getSession()) {
      session.update(namespace + ".changesBasedSCD2MergeFindDiffs", properties);
    }
  }

  /**
   * Retrieves the SQL for merging in the changes-based SCD2 process.
   *
   * @param properties The SCD2MergeProperties containing configuration for the merge operation.
   * @return A String containing the SQL for merging changes.
   */
  public String getChangesBasedSCD2MergeUpsertsSql(SCD2MergeProperties properties) {
    return getSql(namespace + ".changesBasedSCD2MergeUpserts", properties);
  }

  /**
   * Performs the merge operation to find differences in the snapshot-based SCD2 process.
   *
   * @param properties The SCD2MergeProperties containing configuration for the merge operation.
   */
  public void snapshotBasedSCD2MergeFindDiffs(SCD2MergeProperties properties) {
    try (SqlSession session = getSession()) {
      session.update(namespace + ".snapshotBasedSCD2MergeFindDiffs", properties);
    }
  }

  /**
   * Retrieves the SQL for merging in the snapshot-based SCD2 process.
   *
   * @param properties The SCD2MergeProperties containing configuration for the merge operation.
   * @return A String containing the SQL for merging snapshot.
   */
  public String getSnapshotBasedSCD2MergeUpsertsSql(SCD2MergeProperties properties) {
    return getSql(namespace + ".snapshotBasedSCD2MergeUpserts", properties);
  }

  /**
   * Retrieves the SQL for append-only operations in the snapshot-based SCD2 process.
   *
   * @param properties The SCD2MergeProperties containing configuration for the merge operation.
   * @return A String containing the SQL for append-only operations.
   */
  public String getSnapshotBasedSCD2MergeAppendOnlySql(SCD2MergeProperties properties) {
    return getSql(namespace + ".snapshotBasedSCD2MergeAppendOnly", properties);
  }

  /**
   * Retrieves a list of file names from a specified data path.
   *
   * @param dataPath The path where the data files are located.
   * @return A List of Strings containing the file names.
   */
  public List<String> getFileNames(String dataPath) {
    try (SqlSession session = getSession(); ) {
      return session.selectList(namespace + ".getFileNames", dataPath);
    }
  }

  /**
   * Saves distinct file names for snapshot-based merge operations.
   *
   * @param properties The SCD2MergeProperties containing configuration for the operation.
   */
  public void saveDistinctFileNamesForSnapshotMerge(SCD2MergeProperties properties) {
    try (SqlSession session = getSession()) {
      session.update(namespace + ".saveDistinctFileNamesForSnapshotMerge", properties);
    }
  }

  /**
   * Saves distinct file names for changes-based merge operations.
   *
   * @param properties The SCD2MergeProperties containing configuration for the operation.
   */
  public void saveDistinctFileNamesForChangesMerge(SCD2MergeProperties properties) {
    try (SqlSession session = getSession()) {
      session.update(namespace + ".saveDistinctFileNamesForChangesMerge", properties);
    }
  }
}
