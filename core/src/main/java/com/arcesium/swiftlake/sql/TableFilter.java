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

import com.arcesium.swiftlake.expressions.Expression;
import java.util.Map;
import org.apache.iceberg.Table;

/** Represents a filter for a table in SwiftLake SQL operations. */
public class TableFilter {
  private String tableName;
  private Table table;
  private String placeholder;
  private String conditionSql;
  private Expression condition;
  private Map<Integer, Object> params;
  private String sql;
  private TimeTravelOptions timeTravelOptions;

  /**
   * Constructs a TableFilter with a table name and condition SQL.
   *
   * @param tableName the name of the table
   * @param conditionSql the SQL condition string
   */
  public TableFilter(String tableName, String conditionSql) {
    this.tableName = tableName;
    this.conditionSql = conditionSql;
  }

  /**
   * Constructs a TableFilter with a table name, condition SQL, and parameters.
   *
   * @param tableName the name of the table
   * @param conditionSql the SQL condition string
   * @param params the parameters for the condition
   */
  public TableFilter(String tableName, String conditionSql, Map<Integer, Object> params) {
    this.tableName = tableName;
    this.conditionSql = conditionSql;
    this.params = params;
  }

  /**
   * Constructs a TableFilter with a table name and condition expression.
   *
   * @param tableName the name of the table
   * @param condition the condition expression
   */
  public TableFilter(String tableName, Expression condition) {
    this.tableName = tableName;
    this.condition = condition;
  }

  /**
   * Constructs a TableFilter with a table name, condition SQL, placeholder, and parameters.
   *
   * @param tableName the name of the table
   * @param conditionSql the SQL condition string
   * @param placeholder the placeholder string
   * @param params the parameters for the condition
   */
  public TableFilter(
      String tableName, String conditionSql, String placeholder, Map<Integer, Object> params) {
    this.tableName = tableName;
    this.conditionSql = conditionSql;
    this.placeholder = placeholder;
    this.params = params;
  }

  /**
   * Constructs a TableFilter with a table name, condition expression, and placeholder.
   *
   * @param tableName the name of the table
   * @param condition the condition expression
   * @param placeholder the placeholder string
   */
  public TableFilter(String tableName, Expression condition, String placeholder) {
    this.tableName = tableName;
    this.condition = condition;
    this.placeholder = placeholder;
  }

  /**
   * Constructs a TableFilter with a table name, condition SQL, placeholder, parameters, and SQL
   * string.
   *
   * @param tableName the name of the table
   * @param conditionSql the SQL condition string
   * @param placeholder the placeholder string
   * @param params the parameters for the condition
   * @param sql the SQL string
   */
  public TableFilter(
      String tableName,
      String conditionSql,
      String placeholder,
      Map<Integer, Object> params,
      String sql) {
    this.tableName = tableName;
    this.conditionSql = conditionSql;
    this.placeholder = placeholder;
    this.params = params;
    this.sql = sql;
  }

  /**
   * Gets the table name.
   *
   * @return the table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Sets the table name.
   *
   * @param tableName the table name to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Gets the placeholder.
   *
   * @return the placeholder
   */
  public String getPlaceholder() {
    return placeholder;
  }

  /**
   * Gets the condition SQL.
   *
   * @return the condition SQL
   */
  public String getConditionSql() {
    return conditionSql;
  }

  /**
   * Gets the condition expression.
   *
   * @return the condition expression
   */
  public Expression getCondition() {
    return condition;
  }

  /**
   * Gets the parameters for the condition.
   *
   * @return the parameters
   */
  public Map<Integer, Object> getParams() {
    return params;
  }

  /**
   * Gets the SQL string.
   *
   * @return the SQL string
   */
  public String getSql() {
    return sql;
  }

  /**
   * Sets the SQL string.
   *
   * @param sql the SQL string to set
   */
  public void setSql(String sql) {
    this.sql = sql;
  }

  /**
   * Gets the Table object.
   *
   * @return the Table object
   */
  public Table getTable() {
    return table;
  }

  /**
   * Sets the Table object.
   *
   * @param table the Table object to set
   */
  public void setTable(Table table) {
    this.table = table;
  }

  /**
   * Gets the TimeTravelOptions.
   *
   * @return the TimeTravelOptions
   */
  public TimeTravelOptions getTimeTravelOptions() {
    return timeTravelOptions;
  }

  /**
   * Sets the TimeTravelOptions.
   *
   * @param timeTravelOptions the TimeTravelOptions to set
   */
  public void setTimeTravelOptions(TimeTravelOptions timeTravelOptions) {
    this.timeTravelOptions = timeTravelOptions;
  }
}
