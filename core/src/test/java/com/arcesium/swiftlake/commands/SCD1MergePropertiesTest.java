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
package com.arcesium.swiftlake.commands;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SCD1MergePropertiesTest {

  private SCD1MergeProperties properties;

  @BeforeEach
  void setUp() {
    properties = new SCD1MergeProperties();
  }

  @Test
  void testSetAndGetBasicProperties() {
    Expression mockExpression = mock(Expression.class);
    properties.setTableFilter(mockExpression);
    properties.setTableFilterSql("id > 100");
    properties.setSql("SELECT * FROM table");
    properties.setKeyColumns(Arrays.asList("id", "code"));
    properties.setColumns(Arrays.asList("name", "description"));
    properties.setOperationTypeColumn("operation");
    properties.setDeleteOperationValue("DELETE");
    List<String> allColumns = Arrays.asList("id", "name", "description");
    properties.setAllColumns(allColumns);

    assertThat(properties.getTableFilter()).isEqualTo(mockExpression);
    assertThat(properties.getTableFilterSql()).isEqualTo("id > 100");
    assertThat(properties.getSql()).isEqualTo("SELECT * FROM table");
    assertThat(properties.getKeyColumns()).containsExactly("id", "code");
    assertThat(properties.getColumns()).containsExactly("name", "description");
    assertThat(properties.getOperationTypeColumn()).isEqualTo("operation");
    assertThat(properties.getDeleteOperationValue()).isEqualTo("DELETE");
    assertThat(properties.getAllColumns()).containsExactlyElementsOf(allColumns);
  }

  @Test
  void testSetAndGetMybatisProperties() {
    properties.setMybatisStatementId("selectAll");
    properties.setMybatisStatementParameter("param");
    SwiftLakeSqlSessionFactory mockFactory = mock(SwiftLakeSqlSessionFactory.class);
    properties.setSqlSessionFactory(mockFactory);

    assertThat(properties.getMybatisStatementId()).isEqualTo("selectAll");
    assertThat(properties.getMybatisStatementParameter()).isEqualTo("param");
    assertThat(properties.getSqlSessionFactory()).isEqualTo(mockFactory);
  }

  @Test
  void testSetAndGetTableProperties() {
    properties.setSourceTableName("sourceTable");
    properties.setDestinationTableName("destTable");
    properties.setTableFilterColumns(Arrays.asList("col1", "col2"));
    properties.setBoundaryCondition("id > 1000");

    assertThat(properties.getSourceTableName()).isEqualTo("sourceTable");
    assertThat(properties.getDestinationTableName()).isEqualTo("destTable");
    assertThat(properties.getTableFilterColumns()).containsExactly("col1", "col2");
    assertThat(properties.getBoundaryCondition()).isEqualTo("id > 1000");
  }

  @Test
  void testSetAndGetFileProperties() {
    properties.setDiffsFilePath("/path/to/diffs");
    properties.setCompression("gzip");
    properties.setModifiedFileNamesFilePath("/path/to/modified");

    assertThat(properties.getDiffsFilePath()).isEqualTo("/path/to/diffs");
    assertThat(properties.getCompression()).isEqualTo("gzip");
    assertThat(properties.getModifiedFileNamesFilePath()).isEqualTo("/path/to/modified");
  }

  @Test
  void testSetAndGetBooleanProperties() {
    properties.setAppendOnly(true);
    properties.setExecuteSourceSqlOnceOnly(true);
    properties.setSkipDataSorting(true);
    properties.setProcessSourceTables(true);

    assertThat(properties.isAppendOnly()).isTrue();
    assertThat(properties.isExecuteSourceSqlOnceOnly()).isTrue();
    assertThat(properties.isSkipDataSorting()).isTrue();
    assertThat(properties.isProcessSourceTables()).isTrue();
  }

  @Test
  void testToString() {
    properties.setTableFilterSql("id > 100");
    properties.setSql("SELECT * FROM table");
    properties.setKeyColumns(Arrays.asList("id", "code"));
    properties.setOperationTypeColumn("operation");
    properties.setDeleteOperationValue("DELETE");

    String toStringResult = properties.toString();

    assertThat(toStringResult)
        .contains("tableFilterSql='id > 100'")
        .contains("sql='SELECT * FROM table'")
        .contains("keyColumns=[id, code]")
        .contains("operationColumnName='operation'")
        .contains("deleteOperationValue='DELETE'");
  }
}
