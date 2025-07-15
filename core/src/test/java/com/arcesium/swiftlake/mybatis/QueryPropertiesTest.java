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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.sql.QueryTransformer;
import com.arcesium.swiftlake.sql.TableFilter;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class QueryPropertiesTest {

  @Test
  void testConstructorWithSingleTableFilter() {
    String tableName = "testTable";
    Expression mockExpression = mock(Expression.class);
    String parameter = "testParameter";

    QueryProperties<String> queryProperties =
        new QueryProperties<>(tableName, mockExpression, parameter);

    assertThat(queryProperties.getTableFilters()).hasSize(1);
    assertThat(queryProperties.getTableFilters().get(0).getTableName()).isEqualTo(tableName);
    assertThat(queryProperties.getTableFilters().get(0).getCondition()).isEqualTo(mockExpression);
    assertThat(queryProperties.getParameter()).isEqualTo(parameter);
    assertThat(queryProperties.getQueryTransformer()).isNull();
  }

  @Test
  void testConstructorWithMultipleTableFilters() {
    List<TableFilter> tableFilters =
        Arrays.asList(
            new TableFilter("table1", mock(Expression.class)),
            new TableFilter("table2", mock(Expression.class)));
    Integer parameter = 42;

    QueryProperties<Integer> queryProperties = new QueryProperties<>(tableFilters, parameter);

    assertThat(queryProperties.getTableFilters()).isEqualTo(tableFilters);
    assertThat(queryProperties.getParameter()).isEqualTo(parameter);
    assertThat(queryProperties.getQueryTransformer()).isNull();
  }

  @Test
  void testConstructorWithAllParameters() {
    List<TableFilter> tableFilters =
        Arrays.asList(
            new TableFilter("table1", mock(Expression.class)),
            new TableFilter("table2", mock(Expression.class)));
    Double parameter = 3.14;
    QueryTransformer mockTransformer = mock(QueryTransformer.class);

    QueryProperties<Double> queryProperties =
        new QueryProperties<>(tableFilters, parameter, mockTransformer);

    assertThat(queryProperties.getTableFilters()).isEqualTo(tableFilters);
    assertThat(queryProperties.getParameter()).isEqualTo(parameter);
    assertThat(queryProperties.getQueryTransformer()).isEqualTo(mockTransformer);
  }
}
