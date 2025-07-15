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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.dao.CommonDao;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.writer.TableBatchTransaction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class InsertTest {

  @Mock private SwiftLakeEngine mockEngine;
  @Mock private Table mockTable;
  @Mock private CommonDao mockCommonDao;
  @Mock private TableBatchTransaction mockTableBatchTransaction;

  private String tableName = "test_table";
  private String sql = "SELECT * FROM source_table";
  private List<String> columns = Arrays.asList("col1", "col2");

  @BeforeEach
  void setUp() {}

  @Test
  @DisplayName("Test validation of setting branch on batch transaction")
  void testValidationErrorOnBatchTransactionWithBranch() {
    when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

    assertThatThrownBy(
            () -> Insert.into(mockEngine, mockTableBatchTransaction).sql(sql).branch("test-branch"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Set branch name on the batch transaction");
  }

  @Test
  @DisplayName("Test validation of setting metadata on batch transaction")
  void testValidationErrorOnBatchTransactionWithMetadata() {
    when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);
    Map<String, String> metadata = new HashMap<>();
    metadata.put("key", "value");

    assertThatThrownBy(
            () ->
                Insert.into(mockEngine, mockTableBatchTransaction)
                    .sql(sql)
                    .snapshotMetadata(metadata))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Set snapshot metadata on the batch transaction");
  }

  @Test
  @DisplayName("Test validation of setting isolation level on batch transaction")
  void testValidationErrorOnBatchTransactionWithIsolationLevel() {
    when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

    assertThatThrownBy(
            () ->
                Insert.into(mockEngine, mockTableBatchTransaction)
                    .sql(sql)
                    .isolationLevel(IsolationLevel.SERIALIZABLE))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Set isolation level on the batch transaction");
  }

  @Test
  @DisplayName("Test Insert builder chain with all configurations")
  void testInsertBuilderChain() {
    Insert.SetInputDataSql builder = Insert.into(mockEngine, tableName);
    Insert.Builder configBuilder = builder.sql(sql);

    String branchName = "test-branch";
    Map<String, String> metadata = Map.of("key", "value");

    Insert.Builder resultBuilder =
        configBuilder
            .columns(columns)
            .executeSqlOnceOnly(true)
            .skipDataSorting(true)
            .branch(branchName)
            .snapshotMetadata(metadata)
            .isolationLevel(IsolationLevel.SERIALIZABLE)
            .processSourceTables(true);

    assertThat(resultBuilder)
        .extracting(
            "sql",
            "columns",
            "executeSqlOnceOnly",
            "skipDataSorting",
            "branch",
            "snapshotMetadata",
            "isolationLevel",
            "processSourceTables")
        .containsExactly(
            sql, columns, true, true, branchName, metadata, IsolationLevel.SERIALIZABLE, true);
  }

  @Test
  @DisplayName("Test Overwrite builder chain with filter expression")
  void testOverwriteBuilderWithFilter() {
    Insert.SetOverwriteByFilter overwriteBuilder = Insert.overwrite(mockEngine, tableName);
    Expression mockExpression = mock(Expression.class);
    Insert.SetInputDataSql inputBuilder = overwriteBuilder.overwriteByFilter(mockExpression);
    Insert.Builder configBuilder = inputBuilder.sql(sql);

    assertThat(configBuilder)
        .extracting("sql", "overwriteByFilter", "overwriteByFilterSql", "overwriteByFilterColumns")
        .containsExactly(sql, mockExpression, null, null);
  }

  @Test
  @DisplayName("Test Overwrite builder chain with filter SQL")
  void testOverwriteBuilderWithFilterSql() {
    String filterSql = "col1 = 'value'";
    Insert.SetOverwriteByFilter overwriteBuilder = Insert.overwrite(mockEngine, tableName);
    Insert.SetInputDataSql inputBuilder = overwriteBuilder.overwriteByFilterSql(filterSql);
    Insert.Builder configBuilder = inputBuilder.sql(sql);

    assertThat(configBuilder)
        .extracting("sql", "overwriteByFilter", "overwriteByFilterSql", "overwriteByFilterColumns")
        .containsExactly(sql, null, filterSql, null);
  }

  @Test
  @DisplayName("Test Overwrite builder chain with filter columns")
  void testOverwriteBuilderWithFilterColumns() {
    List<String> filterColumns = List.of("col1", "col2");
    Insert.SetOverwriteByFilter overwriteBuilder = Insert.overwrite(mockEngine, tableName);
    Insert.SetInputDataSql inputBuilder = overwriteBuilder.overwriteByFilterColumns(filterColumns);
    Insert.Builder configBuilder = inputBuilder.sql(sql);

    assertThat(configBuilder)
        .extracting("sql", "overwriteByFilter", "overwriteByFilterSql", "overwriteByFilterColumns")
        .containsExactly(sql, null, null, filterColumns);
  }

  @Test
  @DisplayName("Test MyBatis statement configuration")
  void testMybatisStatementConfiguration() {
    String statementId = "test.statement";
    Insert.SetInputDataSql builder = Insert.into(mockEngine, tableName);
    Insert.Builder configBuilder1 = builder.mybatisStatement(statementId);

    assertThat(configBuilder1)
        .extracting("mybatisStatementId", "mybatisStatementParameter")
        .containsExactly(statementId, null);

    Object parameter = Map.of("key", "value");
    builder = Insert.into(mockEngine, tableName);
    Insert.Builder configBuilder2 = builder.mybatisStatement(statementId, parameter);

    assertThat(configBuilder2)
        .extracting("mybatisStatementId", "mybatisStatementParameter")
        .containsExactly(statementId, parameter);
  }
}
