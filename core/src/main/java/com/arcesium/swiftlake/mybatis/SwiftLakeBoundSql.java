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
package com.arcesium.swiftlake.mybatis;

import java.util.List;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.session.Configuration;

/**
 * SwiftLakeBoundSql extends MyBatis BoundSql to provide custom functionality for SwiftLake,
 * including delegation to an existing BoundSql instance.
 */
public class SwiftLakeBoundSql extends BoundSql {
  private final BoundSql delegate;
  private final String sql;

  /**
   * Constructor for SwiftLakeBoundSql with a delegate BoundSql.
   *
   * @param configuration The MyBatis Configuration
   * @param delegate The delegate BoundSql instance
   * @param sql The SQL string
   */
  public SwiftLakeBoundSql(Configuration configuration, BoundSql delegate, String sql) {
    super(configuration, sql, delegate.getParameterMappings(), delegate.getParameterObject());
    this.delegate = delegate;
    this.sql = sql;
  }

  /**
   * Constructor for SwiftLakeBoundSql without a delegate.
   *
   * @param configuration The MyBatis Configuration
   * @param sql The SQL string
   * @param parameterMappings The list of parameter mappings
   * @param parameterObject The parameter object
   */
  public SwiftLakeBoundSql(
      Configuration configuration,
      String sql,
      List<ParameterMapping> parameterMappings,
      Object parameterObject) {
    super(configuration, sql, parameterMappings, parameterObject);
    this.delegate = null;
    this.sql = sql;
  }

  /**
   * Gets the SQL string.
   *
   * @return The SQL string
   */
  @Override
  public String getSql() {
    return sql;
  }

  /**
   * Gets the list of parameter mappings.
   *
   * @return The list of parameter mappings
   */
  @Override
  public List<ParameterMapping> getParameterMappings() {
    return delegate.getParameterMappings();
  }

  /**
   * Gets the parameter object.
   *
   * @return The parameter object
   */
  @Override
  public Object getParameterObject() {
    return delegate.getParameterObject();
  }

  /**
   * Checks if an additional parameter exists.
   *
   * @param name The name of the parameter
   * @return true if the additional parameter exists, false otherwise
   */
  @Override
  public boolean hasAdditionalParameter(String name) {
    return delegate.hasAdditionalParameter(name);
  }

  /**
   * Sets an additional parameter.
   *
   * @param name The name of the parameter
   * @param value The value of the parameter
   */
  @Override
  public void setAdditionalParameter(String name, Object value) {
    delegate.setAdditionalParameter(name, value);
  }

  /**
   * Gets an additional parameter.
   *
   * @param name The name of the parameter
   * @return The value of the additional parameter
   */
  @Override
  public Object getAdditionalParameter(String name) {
    return delegate.getAdditionalParameter(name);
  }
}
