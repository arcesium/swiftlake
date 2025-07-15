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
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SCD2MergePropertiesTest {

  private SCD2MergeProperties properties;

  @BeforeEach
  void setUp() {
    properties = new SCD2MergeProperties();
  }

  @Test
  void testSetAndGetBasicProperties() {
    Expression mockExpression = mock(Expression.class);
    properties.setMode(SCD2MergeMode.CHANGES);
    properties.setTableFilter(mockExpression);
    properties.setTableFilterSql("id > 100");
    properties.setTableFilterColumns(Arrays.asList("col1", "col2"));
    properties.setSql("SELECT * FROM table");
    properties.setKeyColumns(Arrays.asList("id", "code"));
    properties.setColumns(Arrays.asList("name", "description"));
    properties.setCurrentFlagColumn("is_current");
    properties.setEffectiveStartColumn("effective_start");
    properties.setEffectiveEndColumn("effective_end");
    properties.setOperationTypeColumn("operation");
    properties.setDeleteOperationValue("DELETE");

    assertThat(properties.getMode()).isEqualTo(SCD2MergeMode.CHANGES);
    assertThat(properties.getTableFilter()).isEqualTo(mockExpression);
    assertThat(properties.getTableFilterSql()).isEqualTo("id > 100");
    assertThat(properties.getTableFilterColumns()).containsExactly("col1", "col2");
    assertThat(properties.getSql()).isEqualTo("SELECT * FROM table");
    assertThat(properties.getKeyColumns()).containsExactly("id", "code");
    assertThat(properties.getColumns()).containsExactly("name", "description");
    assertThat(properties.getCurrentFlagColumn()).isEqualTo("is_current");
    assertThat(properties.getEffectiveStartColumn()).isEqualTo("effective_start");
    assertThat(properties.getEffectiveEndColumn()).isEqualTo("effective_end");
    assertThat(properties.getOperationTypeColumn()).isEqualTo("operation");
    assertThat(properties.getDeleteOperationValue()).isEqualTo("DELETE");
  }

  @Test
  void testSetAndGetChangeTrackingAndMybatisProperties() {
    List<String> changeTrackingColumns = Arrays.asList("price", "quantity");
    Map<String, ChangeTrackingMetadata<?>> changeTrackingMetadataMap = new HashMap<>();
    changeTrackingMetadataMap.put("price", new ChangeTrackingMetadata<>(0.01, 0.0));
    changeTrackingMetadataMap.put("quantity", new ChangeTrackingMetadata<>(1.0, 0));

    properties.setChangeTrackingColumns(changeTrackingColumns);
    properties.setChangeTrackingMetadataMap(changeTrackingMetadataMap);
    properties.setMybatisStatementId("selectAll");
    properties.setMybatisStatementParameter("param");
    SwiftLakeSqlSessionFactory mockFactory = mock(SwiftLakeSqlSessionFactory.class);
    properties.setSqlSessionFactory(mockFactory);

    assertThat(properties.getChangeTrackingColumns())
        .containsExactlyElementsOf(changeTrackingColumns);
    assertThat(properties.getChangeTrackingMetadataMap()).isEqualTo(changeTrackingMetadataMap);
    assertThat(properties.getMybatisStatementId()).isEqualTo("selectAll");
    assertThat(properties.getMybatisStatementParameter()).isEqualTo("param");
    assertThat(properties.getSqlSessionFactory()).isEqualTo(mockFactory);
  }

  @Test
  void testSetAndGetFileAndTableProperties() {
    properties.setSourceTableName("sourceTable");
    properties.setDestinationTableName("destTable");
    properties.setDiffsFilePath("/path/to/diffs");
    properties.setModifiedFileNamesFilePath("/path/to/modified");
    properties.setCompression("gzip");

    assertThat(properties.getSourceTableName()).isEqualTo("sourceTable");
    assertThat(properties.getDestinationTableName()).isEqualTo("destTable");
    assertThat(properties.getDiffsFilePath()).isEqualTo("/path/to/diffs");
    assertThat(properties.getModifiedFileNamesFilePath()).isEqualTo("/path/to/modified");
    assertThat(properties.getCompression()).isEqualTo("gzip");
  }

  @Test
  void testSetAndGetBooleanAndTimestampProperties() {
    properties.setAppendOnly(true);
    properties.setExecuteSourceSqlOnceOnly(true);
    properties.setSkipDataSorting(true);
    properties.setProcessSourceTables(true);
    properties.setGenerateEffectiveTimestamp(true);
    properties.setSkipEmptySource(true);
    properties.setEffectiveTimestamp("2023-01-01 00:00:00");
    properties.setDefaultEffectiveEndTimestamp("9999-12-31 23:59:59");

    assertThat(properties.isAppendOnly()).isTrue();
    assertThat(properties.isExecuteSourceSqlOnceOnly()).isTrue();
    assertThat(properties.isSkipDataSorting()).isTrue();
    assertThat(properties.isProcessSourceTables()).isTrue();
    assertThat(properties.isGenerateEffectiveTimestamp()).isTrue();
    assertThat(properties.isSkipEmptySource()).isTrue();
    assertThat(properties.getEffectiveTimestamp()).isEqualTo("2023-01-01 00:00:00");
    assertThat(properties.getDefaultEffectiveEndTimestamp()).isEqualTo("9999-12-31 23:59:59");
  }

  @Test
  void testToString() {
    properties.setMode(SCD2MergeMode.SNAPSHOT);
    properties.setTableFilterSql("id > 100");
    properties.setKeyColumns(Arrays.asList("id", "code"));
    properties.setCurrentFlagColumn("is_current");
    properties.setEffectiveStartColumn("effective_start");
    properties.setEffectiveEndColumn("effective_end");

    String result = properties.toString();

    assertThat(result)
        .contains("mode=SNAPSHOT")
        .contains("tableFilterSql='id > 100'")
        .contains("keyColumns=[id, code]")
        .contains("currentFlagColumn='is_current'")
        .contains("effectiveStartColumn='effective_start'")
        .contains("effectiveEndColumn='effective_end'");
  }
}
