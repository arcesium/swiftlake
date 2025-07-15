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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.writer.TableBatchTransaction;
import java.util.Map;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DeleteTest {
  @Mock private SwiftLakeEngine mockEngine;
  @Mock private Table mockTable;
  @Mock private TableBatchTransaction mockBatchTransaction;
  @Mock private Expression mockExpression;

  private String tableName = "test_table";
  private String conditionSql = "id > 100";

  @Nested
  @DisplayName("Builder Configuration Tests")
  class BuilderConfigurationTests {

    @Test
    @DisplayName("Test builder chain with condition SQL")
    void testBuilderChainWithConditionSql() {
      Delete.Builder builder =
          Delete.from(mockEngine, mockTable)
              .conditionSql(conditionSql)
              .skipDataSorting(true)
              .branch("main")
              .snapshotMetadata(Map.of("key", "value"))
              .isolationLevel(IsolationLevel.SERIALIZABLE);

      assertThat(builder).isInstanceOf(Delete.BuilderImpl.class);
      Delete.BuilderImpl builderImpl = (Delete.BuilderImpl) builder;

      assertThat(builderImpl).extracting("conditionSql").isEqualTo(conditionSql);
      assertThat(builderImpl).extracting("condition").isNull();
      assertThat(builderImpl).extracting("skipDataSorting").isEqualTo(true);
      assertThat(builderImpl).extracting("branch").isEqualTo("main");
      assertThat(builderImpl).extracting("snapshotMetadata").isEqualTo(Map.of("key", "value"));
      assertThat(builderImpl).extracting("isolationLevel").isEqualTo(IsolationLevel.SERIALIZABLE);
    }

    @Test
    @DisplayName("Test builder chain with expression condition")
    void testBuilderChainWithExpressionCondition() {
      Delete.Builder builder =
          Delete.from(mockEngine, mockTable)
              .condition(mockExpression)
              .skipDataSorting(true)
              .branch("main")
              .snapshotMetadata(Map.of("key", "value"))
              .isolationLevel(IsolationLevel.SERIALIZABLE);

      assertThat(builder).isInstanceOf(Delete.BuilderImpl.class);
      Delete.BuilderImpl builderImpl = (Delete.BuilderImpl) builder;

      assertThat(builderImpl).extracting("condition").isEqualTo(mockExpression);
      assertThat(builderImpl).extracting("conditionSql").isNull();
      assertThat(builderImpl).extracting("skipDataSorting").isEqualTo(true);
      assertThat(builderImpl).extracting("branch").isEqualTo("main");
      assertThat(builderImpl).extracting("snapshotMetadata").isEqualTo(Map.of("key", "value"));
      assertThat(builderImpl).extracting("isolationLevel").isEqualTo(IsolationLevel.SERIALIZABLE);
    }

    @Test
    @DisplayName("Test builder chain with batch transaction")
    void testBuilderChainWithBatchTransaction() {
      when(mockBatchTransaction.getTable()).thenReturn(mockTable);

      Delete.Builder builder =
          Delete.from(mockEngine, mockBatchTransaction)
              .conditionSql(conditionSql)
              .skipDataSorting(true);

      assertThat(builder).isInstanceOf(Delete.BuilderImpl.class);
      Delete.BuilderImpl builderImpl = (Delete.BuilderImpl) builder;

      assertThat(builderImpl).extracting("conditionSql").isEqualTo(conditionSql);
      assertThat(builderImpl).extracting("table").isEqualTo(mockTable);
      assertThat(builderImpl).extracting("tableBatchTransaction").isEqualTo(mockBatchTransaction);
      assertThat(builderImpl).extracting("skipDataSorting").isEqualTo(true);
      assertThat(builderImpl).extracting("branch").isNull();
      assertThat(builderImpl).extracting("snapshotMetadata").isNull();
      assertThat(builderImpl).extracting("isolationLevel").isNull();
    }
  }

  @Nested
  @DisplayName("Initialization Tests")
  class InitializationTests {

    @Test
    @DisplayName("Test initialization with table name")
    void testInitializationWithTableName() {
      when(mockEngine.getTable(tableName, true)).thenReturn(mockTable);

      Delete.SetCondition condition = Delete.from(mockEngine, tableName);
      Delete.Builder builder = condition.conditionSql(conditionSql);

      assertThat(builder).isInstanceOf(Delete.BuilderImpl.class);
      Delete.BuilderImpl builderImpl = (Delete.BuilderImpl) builder;

      assertThat(builderImpl).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builderImpl).extracting("table").isEqualTo(mockTable);
      assertThat(builderImpl).extracting("conditionSql").isEqualTo(conditionSql);

      verify(mockEngine).getTable(tableName, true);
    }

    @Test
    @DisplayName("Test initialization with table instance")
    void testInitializationWithTable() {
      Delete.SetCondition condition = Delete.from(mockEngine, mockTable);
      Delete.Builder builder = condition.conditionSql(conditionSql);

      assertThat(builder).isInstanceOf(Delete.BuilderImpl.class);
      Delete.BuilderImpl builderImpl = (Delete.BuilderImpl) builder;

      assertThat(builderImpl).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builderImpl).extracting("table").isEqualTo(mockTable);
      assertThat(builderImpl).extracting("conditionSql").isEqualTo(conditionSql);
    }

    @Test
    @DisplayName("Test initialization with batch transaction")
    void testInitializationWithBatchTransaction() {
      when(mockBatchTransaction.getTable()).thenReturn(mockTable);

      Delete.SetCondition condition = Delete.from(mockEngine, mockBatchTransaction);
      Delete.Builder builder = condition.conditionSql(conditionSql);

      assertThat(builder).isInstanceOf(Delete.BuilderImpl.class);
      Delete.BuilderImpl builderImpl = (Delete.BuilderImpl) builder;

      assertThat(builderImpl).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builderImpl).extracting("table").isEqualTo(mockTable);
      assertThat(builderImpl).extracting("tableBatchTransaction").isEqualTo(mockBatchTransaction);
      assertThat(builderImpl).extracting("conditionSql").isEqualTo(conditionSql);
    }
  }

  @Nested
  @DisplayName("Validation Tests")
  class ValidationTests {

    @Test
    @DisplayName("Test validation of setting branch on batch transaction")
    void testValidationErrorOnBatchTransactionWithBranch() {
      when(mockBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  Delete.from(mockEngine, mockBatchTransaction)
                      .conditionSql(conditionSql)
                      .branch("test-branch"))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set branch name on the batch transaction");
    }

    @Test
    @DisplayName("Test validation of setting snapshot metadata on batch transaction")
    void testValidationErrorOnBatchTransactionWithMetadata() {
      when(mockBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  Delete.from(mockEngine, mockBatchTransaction)
                      .conditionSql(conditionSql)
                      .snapshotMetadata(Map.of("key", "value")))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set snapshot metadata on the batch transaction");
    }

    @Test
    @DisplayName("Test validation of setting isolation level on batch transaction")
    void testValidationErrorOnBatchTransactionWithIsolationLevel() {
      when(mockBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  Delete.from(mockEngine, mockBatchTransaction)
                      .conditionSql(conditionSql)
                      .isolationLevel(IsolationLevel.SERIALIZABLE))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set isolation level on the batch transaction");
    }
  }
}
