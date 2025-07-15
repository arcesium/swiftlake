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
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UpdateTest {
  @Mock private SwiftLakeEngine mockEngine;
  @Mock private Table mockTable;
  @Mock private TableBatchTransaction mockTableBatchTransaction;
  @Mock private Expression mockExpression;

  private String tableName = "test_table";
  private String conditionSql = "id > 100";
  private Map<String, Object> updateSets;

  @BeforeEach
  void setUp() {
    updateSets = new HashMap<>();
    updateSets.put("name", "New Name");
    updateSets.put("status", "Active");
    updateSets.put("count", 10);
  }

  @Nested
  @DisplayName("Builder Configuration Tests")
  class BuilderConfigurationTests {

    @Test
    @DisplayName("Test builder chain with condition expression")
    void testBuilderChainWithConditionExpression() {
      Update.Builder builder =
          Update.on(mockEngine, tableName)
              .condition(mockExpression)
              .updateSets(updateSets)
              .skipDataSorting(true)
              .branch("main")
              .snapshotMetadata(Map.of("key", "value"))
              .isolationLevel(IsolationLevel.SERIALIZABLE);

      assertThat(builder).isInstanceOf(Update.BuilderImpl.class);
      Update.BuilderImpl builderImpl = (Update.BuilderImpl) builder;

      assertThat(builderImpl).extracting("condition").isEqualTo(mockExpression);
      assertThat(builderImpl).extracting("updateSets").isEqualTo(updateSets);
      assertThat(builderImpl).extracting("skipDataSorting").isEqualTo(true);
      assertThat(builderImpl).extracting("branch").isEqualTo("main");
      assertThat(builderImpl).extracting("snapshotMetadata").isEqualTo(Map.of("key", "value"));
      assertThat(builderImpl).extracting("isolationLevel").isEqualTo(IsolationLevel.SERIALIZABLE);
    }

    @Test
    @DisplayName("Test builder chain with condition SQL")
    void testBuilderChainWithConditionSql() {
      Update.Builder builder =
          Update.on(mockEngine, tableName)
              .conditionSql(conditionSql)
              .updateSets(updateSets)
              .skipDataSorting(true);

      assertThat(builder).isInstanceOf(Update.BuilderImpl.class);
      Update.BuilderImpl builderImpl = (Update.BuilderImpl) builder;

      assertThat(builderImpl).extracting("conditionSql").isEqualTo(conditionSql);
      assertThat(builderImpl).extracting("condition").isNull();
      assertThat(builderImpl).extracting("updateSets").isEqualTo(updateSets);
      assertThat(builderImpl).extracting("skipDataSorting").isEqualTo(true);
    }
  }

  @Nested
  @DisplayName("Validation Tests")
  class ValidationTests {

    @Test
    @DisplayName("Test validation of setting branch on batch transaction")
    void testValidationErrorOnBatchTransactionWithBranch() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  Update.on(mockEngine, mockTableBatchTransaction)
                      .conditionSql(conditionSql)
                      .updateSets(updateSets)
                      .branch("test-branch"))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set branch name on the batch transaction");
    }

    @Test
    @DisplayName("Test validation of setting snapshot metadata on batch transaction")
    void testValidationErrorOnBatchTransactionWithMetadata() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  Update.on(mockEngine, mockTableBatchTransaction)
                      .conditionSql(conditionSql)
                      .updateSets(updateSets)
                      .snapshotMetadata(Map.of("key", "value")))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set snapshot metadata on the batch transaction");
    }

    @Test
    @DisplayName("Test validation of setting isolation level on batch transaction")
    void testValidationErrorOnBatchTransactionWithIsolationLevel() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  Update.on(mockEngine, mockTableBatchTransaction)
                      .conditionSql(conditionSql)
                      .updateSets(updateSets)
                      .isolationLevel(IsolationLevel.SERIALIZABLE))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set isolation level on the batch transaction");
    }

    @Test
    @DisplayName("Test validation of null or empty updateSets")
    void testValidationErrorOnNullOrEmptyUpdateSets() {
      assertThatThrownBy(
              () -> Update.on(mockEngine, tableName).conditionSql(conditionSql).updateSets(null))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Update sets cannot be null or empty");

      assertThatThrownBy(
              () ->
                  Update.on(mockEngine, tableName).conditionSql(conditionSql).updateSets(Map.of()))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Update sets cannot be null or empty");
    }
  }

  @Nested
  @DisplayName("Initialization Tests")
  class InitializationTests {

    @Test
    @DisplayName("Test initialization with table name")
    void testInitializationWithTableName() {
      when(mockEngine.getTable(tableName, true)).thenReturn(mockTable);

      Update.SetCondition condition = Update.on(mockEngine, tableName);
      Update.SetUpdateSets updateSetInterface = condition.conditionSql(conditionSql);
      Update.Builder builder = updateSetInterface.updateSets(updateSets);

      assertThat(builder).isInstanceOf(Update.BuilderImpl.class);
      Update.BuilderImpl builderImpl = (Update.BuilderImpl) builder;

      assertThat(builderImpl).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builderImpl).extracting("table").isEqualTo(mockTable);
      assertThat(builderImpl).extracting("conditionSql").isEqualTo(conditionSql);
      assertThat(builderImpl).extracting("updateSets").isEqualTo(updateSets);

      verify(mockEngine).getTable(tableName, true);
    }

    @Test
    @DisplayName("Test initialization with table instance")
    void testInitializationWithTable() {
      Update.SetCondition condition = Update.on(mockEngine, mockTable);
      Update.SetUpdateSets updateSetInterface = condition.conditionSql(conditionSql);
      Update.Builder builder = updateSetInterface.updateSets(updateSets);

      assertThat(builder).isInstanceOf(Update.BuilderImpl.class);
      Update.BuilderImpl builderImpl = (Update.BuilderImpl) builder;

      assertThat(builderImpl).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builderImpl).extracting("table").isEqualTo(mockTable);
      assertThat(builderImpl).extracting("conditionSql").isEqualTo(conditionSql);
      assertThat(builderImpl).extracting("updateSets").isEqualTo(updateSets);
    }

    @Test
    @DisplayName("Test initialization with batch transaction")
    void testInitializationWithBatchTransaction() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      Update.SetCondition condition = Update.on(mockEngine, mockTableBatchTransaction);
      Update.SetUpdateSets updateSetInterface = condition.conditionSql(conditionSql);
      Update.Builder builder = updateSetInterface.updateSets(updateSets);

      assertThat(builder).isInstanceOf(Update.BuilderImpl.class);
      Update.BuilderImpl builderImpl = (Update.BuilderImpl) builder;

      assertThat(builderImpl).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builderImpl).extracting("table").isEqualTo(mockTable);
      assertThat(builderImpl)
          .extracting("tableBatchTransaction")
          .isEqualTo(mockTableBatchTransaction);
      assertThat(builderImpl).extracting("conditionSql").isEqualTo(conditionSql);
      assertThat(builderImpl).extracting("updateSets").isEqualTo(updateSets);
    }
  }
}
