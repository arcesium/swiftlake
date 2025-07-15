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

import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.sql.QueryTransformer;
import com.arcesium.swiftlake.sql.TableFilter;
import java.util.ArrayList;
import java.util.List;

/**
 * QueryProperties class represents the properties of a query, including table filters, parameters,
 * and query transformers.
 *
 * @param <T> The type of the parameter object
 */
public class QueryProperties<T> {
  private List<TableFilter> tableFilters;
  private T parameter;
  private QueryTransformer queryTransformer;

  /**
   * Constructor for QueryProperties with a single table filter.
   *
   * @param tableName The name of the table
   * @param tableFilter The Expression representing the table filter
   * @param parameter The parameter object
   */
  public QueryProperties(String tableName, Expression tableFilter, T parameter) {
    this.tableFilters = new ArrayList<>();
    this.tableFilters.add(new TableFilter(tableName, tableFilter));
    this.parameter = parameter;
  }

  /**
   * Constructor for QueryProperties with multiple table filters.
   *
   * @param tableFilters The list of TableFilter objects
   * @param parameter The parameter object
   */
  public QueryProperties(List<TableFilter> tableFilters, T parameter) {
    this.tableFilters = tableFilters;
    this.parameter = parameter;
  }

  /**
   * Constructor for QueryProperties with table filters, parameter, and query transformer.
   *
   * @param tableFilters The list of TableFilter objects
   * @param parameter The parameter object
   * @param queryTransformer The QueryTransformer object
   */
  public QueryProperties(
      List<TableFilter> tableFilters, T parameter, QueryTransformer queryTransformer) {
    this.tableFilters = tableFilters;
    this.parameter = parameter;
    this.queryTransformer = queryTransformer;
  }

  /**
   * Gets the list of table filters.
   *
   * @return The list of TableFilter objects
   */
  public List<TableFilter> getTableFilters() {
    return tableFilters;
  }

  /**
   * Gets the parameter object.
   *
   * @return The parameter object of type T
   */
  public T getParameter() {
    return parameter;
  }

  /**
   * Gets the query transformer.
   *
   * @return The QueryTransformer object
   */
  public QueryTransformer getQueryTransformer() {
    return queryTransformer;
  }
}
