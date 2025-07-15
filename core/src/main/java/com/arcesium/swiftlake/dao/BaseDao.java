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
import com.arcesium.swiftlake.common.SwiftLakeException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.session.SqlSession;

/**
 * Abstract base class for Data Access Objects (DAOs) in the SwiftLake system. Provides common
 * functionality for database operations.
 */
abstract class BaseDao {
  protected SwiftLakeEngine swiftLakeEngine;

  /**
   * Constructs a BaseDao with the given SwiftLakeEngine.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance to use for database operations.
   */
  public BaseDao(SwiftLakeEngine swiftLakeEngine) {
    this.swiftLakeEngine = swiftLakeEngine;
  }

  /**
   * Gets a new SqlSession from the SwiftLakeEngine.
   *
   * @return A new SqlSession instance.
   */
  protected SqlSession getSession() {
    return swiftLakeEngine.getInternalSqlSessionFactory().openSession();
  }

  /**
   * Retrieves the SQL string for a given ID and parameters.
   *
   * @param id The ID of the SQL statement.
   * @param params The parameters to be used in the SQL statement.
   * @return The SQL string.
   */
  protected String getSql(String id, Object params) {
    return swiftLakeEngine.getInternalSqlSessionFactory().getSql(id, params);
  }

  /**
   * Converts a ResultSet to a List of Maps, where each Map represents a row of data.
   *
   * @param resultSet The ResultSet to convert.
   * @return A List of Maps containing the data from the ResultSet.
   * @throws SwiftLakeException If an error occurs during processing.
   */
  protected List<Map<String, Object>> getDataInMap(ResultSet resultSet) {
    List<Map<String, Object>> data = new ArrayList<>();
    try {
      while (resultSet.next()) {
        Map<String, Object> map = new HashMap<>();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
          String name = resultSetMetaData.getColumnName(i);
          map.put(name, resultSet.getObject(name));
        }
        data.add(map);
      }
    } catch (SQLException e) {
      throw new SwiftLakeException(e, "An error occurred while reading data from ResultSet.");
    }
    return data;
  }
}
