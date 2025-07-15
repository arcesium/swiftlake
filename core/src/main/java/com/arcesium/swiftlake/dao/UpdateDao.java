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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.session.SqlSession;

/**
 * Data Access Object for handling update operations in the SwiftLake system. Extends BaseDao to
 * leverage common DAO functionality.
 */
public class UpdateDao extends BaseDao {
  private static final String namespace = "Update";

  /**
   * Constructs an UpdateDao with the given SwiftLakeEngine.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance to use for database operations.
   */
  public UpdateDao(SwiftLakeEngine swiftLakeEngine) {
    super(swiftLakeEngine);
  }

  /**
   * Retrieves a list of matching files based on the destination table name and a condition.
   *
   * @param destinationTableName The name of the destination table.
   * @param condition The condition to filter the matching files.
   * @return A List of Strings containing the names of matching files.
   */
  public List<String> getMatchingFiles(String destinationTableName, String condition) {
    try (SqlSession session = getSession()) {
      Map<String, Object> params = new HashMap<>();
      params.put("destinationTableName", destinationTableName);
      params.put("condition", condition);
      return session.selectList(namespace + ".getMatchingFiles", params);
    }
  }

  /**
   * Generates the SQL for updating records in the destination table.
   *
   * @param destinationTableWithMatchingFiles The name of the destination table with matching files.
   * @param condition The condition for the update operation.
   * @param updateDataMap A map containing the columns to update and their new values.
   * @return A String containing the generated SQL for the update operation.
   */
  public String getUpdateSql(
      String destinationTableWithMatchingFiles,
      String condition,
      Map<String, String> updateDataMap) {
    Map<String, Object> params = new HashMap<>();
    params.put("destinationTableWithMatchingFiles", destinationTableWithMatchingFiles);
    params.put("condition", condition);
    params.put("updateDataMap", updateDataMap);
    return getSql(namespace + ".update", params);
  }
}
