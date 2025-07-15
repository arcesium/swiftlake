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
import com.arcesium.swiftlake.commands.SCD1MergeProperties;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

/**
 * Data Access Object for handling Slowly Changing Dimension Type 1 (SCD1) merge operations. Extends
 * BaseDao to leverage common DAO functionality.
 */
public class SCD1MergeDao extends BaseDao {
  private static final String namespace = "SCD1Merge";

  /**
   * Constructs an SCD1MergeDao with the given SwiftLakeEngine.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance to use for database operations.
   */
  public SCD1MergeDao(SwiftLakeEngine swiftLakeEngine) {
    super(swiftLakeEngine);
  }

  /**
   * Performs the merge operation to find differences in the SCD1 process.
   *
   * @param properties The SCD1MergeProperties containing configuration for the merge operation.
   */
  public void mergeFindDiffs(SCD1MergeProperties properties) {
    try (SqlSession session = getSession()) {
      session.update(namespace + ".mergeFindDiffs", properties);
    }
  }

  /**
   * Retrieves the SQL for merging changes in the SCD1 process.
   *
   * @param properties The SCD1MergeProperties containing configuration for the merge operation.
   * @return A String containing the SQL for merging changes.
   */
  public String getMergeUpsertsSql(SCD1MergeProperties properties) {
    return getSql(namespace + ".mergeUpserts", properties);
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
   * Saves distinct file names based on the provided SCD1 merge properties.
   *
   * @param properties The SCD1MergeProperties containing configuration for the operation.
   */
  public void saveDistinctFileNames(SCD1MergeProperties properties) {
    try (SqlSession session = getSession()) {
      session.update(namespace + ".saveDistinctFileNames", properties);
    }
  }
}
