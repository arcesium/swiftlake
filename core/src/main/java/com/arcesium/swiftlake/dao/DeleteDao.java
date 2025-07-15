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
 * Data Access Object for delete operations in the SwiftLake system. Extends BaseDao to leverage
 * common DAO functionality.
 */
public class DeleteDao extends BaseDao {
  private static final String namespace = "Delete";

  /**
   * Constructs a DeleteDao with the given SwiftLakeEngine.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance to use for database operations.
   */
  public DeleteDao(SwiftLakeEngine swiftLakeEngine) {
    super(swiftLakeEngine);
  }

  /**
   * Retrieves a list of files matching a given condition in a destination table.
   *
   * @param destinationTableName The name of the destination table to search.
   * @param condition The condition to apply when searching for matching files.
   * @return A list of file names that match the given condition.
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
   * Generates SQL for deleting records from a table with matching files based on a condition.
   *
   * @param destinationTableWithMatchingFiles The name of the table containing the matching files.
   * @param condition The condition to apply for the delete operation.
   * @return A string containing the SQL delete statement.
   */
  public String getDeleteSql(String destinationTableWithMatchingFiles, String condition) {
    Map<String, Object> params = new HashMap<>();
    params.put("destinationTableWithMatchingFiles", destinationTableWithMatchingFiles);
    params.put("condition", condition);
    return getSql(namespace + ".delete", params);
  }
}
