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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.arcesium.swiftlake.expressions.Expression;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

class TableFilterTest {

  @Test
  void testConstructorWithTableNameAndConditionSql() {
    TableFilter filter = new TableFilter("users", "age > 18");

    assertThat(filter.getTableName()).isEqualTo("users");
    assertThat(filter.getConditionSql()).isEqualTo("age > 18");
  }

  @Test
  void testConstructorWithTableNameConditionSqlAndParams() {
    Map<Integer, Object> params = new HashMap<>();
    params.put(1, 18);
    TableFilter filter = new TableFilter("users", "age > ?", params);

    assertThat(filter.getTableName()).isEqualTo("users");
    assertThat(filter.getConditionSql()).isEqualTo("age > ?");
    assertThat(filter.getParams()).isEqualTo(params);
  }

  @Test
  void testConstructorWithTableNameAndCondition() {
    Expression mockExpression = mock(Expression.class);
    TableFilter filter = new TableFilter("users", mockExpression);

    assertThat(filter.getTableName()).isEqualTo("users");
    assertThat(filter.getCondition()).isEqualTo(mockExpression);
  }

  @Test
  void testConstructorWithTableNameConditionSqlPlaceholderAndParams() {
    Map<Integer, Object> params = new HashMap<>();
    params.put(1, 18);
    TableFilter filter = new TableFilter("users", "age > ?", "{{placeholder}}", params);

    assertThat(filter.getTableName()).isEqualTo("users");
    assertThat(filter.getConditionSql()).isEqualTo("age > ?");
    assertThat(filter.getPlaceholder()).isEqualTo("{{placeholder}}");
    assertThat(filter.getParams()).isEqualTo(params);
  }

  @Test
  void testConstructorWithTableNameConditionAndPlaceholder() {
    Expression mockExpression = mock(Expression.class);
    TableFilter filter = new TableFilter("users", mockExpression, "{{placeholder}}");

    assertThat(filter.getTableName()).isEqualTo("users");
    assertThat(filter.getCondition()).isEqualTo(mockExpression);
    assertThat(filter.getPlaceholder()).isEqualTo("{{placeholder}}");
  }

  @Test
  void testConstructorWithAllParameters() {
    Map<Integer, Object> params = new HashMap<>();
    params.put(1, 18);
    TableFilter filter =
        new TableFilter(
            "users", "age > ?", "{{placeholder}}", params, "SELECT * FROM users WHERE age > ?");

    assertThat(filter.getTableName()).isEqualTo("users");
    assertThat(filter.getConditionSql()).isEqualTo("age > ?");
    assertThat(filter.getPlaceholder()).isEqualTo("{{placeholder}}");
    assertThat(filter.getParams()).isEqualTo(params);
    assertThat(filter.getSql()).isEqualTo("SELECT * FROM users WHERE age > ?");
  }

  @Test
  void testSetAndGetTableName() {
    TableFilter filter = new TableFilter("users", "age > 18");
    filter.setTableName("employees");

    assertThat(filter.getTableName()).isEqualTo("employees");
  }

  @Test
  void testSetAndGetSql() {
    TableFilter filter = new TableFilter("users", "age > 18");
    filter.setSql("SELECT * FROM users WHERE age > 18");

    assertThat(filter.getSql()).isEqualTo("SELECT * FROM users WHERE age > 18");
  }

  @Test
  void testSetAndGetTable() {
    TableFilter filter = new TableFilter("users", "age > 18");
    Table mockTable = mock(Table.class);
    filter.setTable(mockTable);

    assertThat(filter.getTable()).isEqualTo(mockTable);
  }

  @Test
  void testSetAndGetTimeTravelOptions() {
    TableFilter filter = new TableFilter("users", "age > 18");
    TimeTravelOptions options = new TimeTravelOptions(LocalDateTime.now(), "test", 100L);
    filter.setTimeTravelOptions(options);

    assertThat(filter.getTimeTravelOptions()).isEqualTo(options);
  }
}
