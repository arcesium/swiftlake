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
package com.arcesium.swiftlake.mybatis.type;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.MappedTypes;

/** Custom TypeHandler for converting between Java LocalTime and JDBC types. */
@MappedTypes(LocalTime.class)
public class LocalTimeTypeHandler extends BaseTypeHandler<LocalTime> {

  @Override
  public void setNonNullParameter(
      PreparedStatement ps, int i, LocalTime parameter, JdbcType jdbcType) throws SQLException {
    ps.setObject(i, new SwiftLakeDuckDBTime(parameter));
  }

  @Override
  public LocalTime getNullableResult(ResultSet rs, String columnName) throws SQLException {
    return (LocalTime) rs.getObject(columnName);
  }

  @Override
  public LocalTime getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
    return (LocalTime) rs.getObject(columnIndex);
  }

  @Override
  public LocalTime getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
    return cs.getObject(columnIndex, LocalTime.class);
  }
}
