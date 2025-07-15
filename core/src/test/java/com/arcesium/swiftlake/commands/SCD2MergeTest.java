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
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import com.arcesium.swiftlake.writer.TableBatchTransaction;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
class SCD2MergeTest {
  @Mock private SwiftLakeSqlSessionFactory mockSqlSessionFactory;
  @Mock private SwiftLakeEngine mockEngine;
  @Mock private Table mockTable;
  @Mock private TableBatchTransaction mockTableBatchTransaction;
  @Mock private Expression mockExpression;

  private String tableName = "test_table";
  private String sql = "SELECT * FROM source_table";
  private List<String> keyColumns = Arrays.asList("id", "code");
  private List<String> columns = Arrays.asList("id", "code", "name", "value");
  private List<String> changeTrackingColumns = Arrays.asList("name", "value");
  private String effectiveStartColumn = "effective_start";
  private String effectiveEndColumn = "effective_end";
  private String currentFlagColumn = "is_current";
  private String operationTypeColumn = "operation";
  private String deleteOperationValue = "DELETE";
  private LocalDateTime now = LocalDateTime.now();
  private String effectiveTimestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(now);

  @BeforeEach
  void setUp() {
    when(mockEngine.getSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
  }

  @Nested
  @DisplayName("Changes Mode Builder Configuration Tests")
  class ChangesModeBuilderConfigurationTests {

    @Test
    @DisplayName("Test changes mode builder chain with table filter expression")
    void testChangesModeBuilderChainWithFilterExpression() {
      SCD2Merge.Builder builder =
          SCD2Merge.applyChanges(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceSql(sql)
              .effectiveTimestamp(effectiveTimestamp)
              .keyColumns(keyColumns)
              .operationTypeColumn(operationTypeColumn, deleteOperationValue)
              .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
              .columns(columns)
              .changeTrackingColumns(changeTrackingColumns)
              .currentFlagColumn(currentFlagColumn)
              .skipDataSorting(true)
              .executeSourceSqlOnceOnly(true)
              .processSourceTables(true);

      SCD2MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getTableFilter()).isEqualTo(mockExpression);
      assertThat(properties.getSql()).isEqualTo(sql);
      assertThat(properties.getEffectiveTimestamp()).isEqualTo(effectiveTimestamp);
      assertThat(properties.getKeyColumns()).isEqualTo(keyColumns);
      assertThat(properties.getOperationTypeColumn()).isEqualTo(operationTypeColumn);
      assertThat(properties.getDeleteOperationValue()).isEqualTo(deleteOperationValue);
      assertThat(properties.getEffectiveStartColumn()).isEqualTo(effectiveStartColumn);
      assertThat(properties.getEffectiveEndColumn()).isEqualTo(effectiveEndColumn);
      assertThat(properties.getColumns()).isEqualTo(columns);
      assertThat(properties.getChangeTrackingColumns()).isEqualTo(changeTrackingColumns);
      assertThat(properties.getCurrentFlagColumn()).isEqualTo(currentFlagColumn);
      assertThat(properties.isSkipDataSorting()).isTrue();
      assertThat(properties.isExecuteSourceSqlOnceOnly()).isTrue();
      assertThat(properties.isProcessSourceTables()).isTrue();
      assertThat(properties.getMode()).isEqualTo(SCD2MergeMode.CHANGES);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
    }

    @Test
    @DisplayName("Test builder chain with table filter SQL")
    void testBuilderChainWithFilterSql() {
      String filterSql = "id > 100";
      SCD2Merge.Builder builder =
          SCD2Merge.applyChanges(mockEngine, tableName)
              .tableFilterSql(filterSql)
              .sourceSql(sql)
              .effectiveTimestamp(effectiveTimestamp)
              .keyColumns(keyColumns)
              .operationTypeColumn(operationTypeColumn, deleteOperationValue)
              .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn);

      SCD2MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getTableFilterSql()).isEqualTo(filterSql);
      assertThat(properties.getSql()).isEqualTo(sql);
      assertThat(properties.getEffectiveTimestamp()).isEqualTo(effectiveTimestamp);
      assertThat(properties.getKeyColumns()).isEqualTo(keyColumns);
      assertThat(properties.getTableFilter()).isNull(); // Should be null since we used SQL filter
    }

    @Test
    @DisplayName("Test builder chain with table filter columns")
    void testBuilderChainWithFilterColumns() {
      List<String> filterColumns = Arrays.asList("region", "department");
      SCD2Merge.Builder builder =
          SCD2Merge.applyChanges(mockEngine, tableName)
              .tableFilterColumns(filterColumns)
              .sourceSql(sql)
              .effectiveTimestamp(effectiveTimestamp)
              .keyColumns(keyColumns)
              .operationTypeColumn(operationTypeColumn, deleteOperationValue)
              .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn);

      SCD2MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getTableFilterColumns()).isEqualTo(filterColumns);
      assertThat(properties.getSql()).isEqualTo(sql);
      assertThat(properties.getKeyColumns()).isEqualTo(keyColumns);
      assertThat(properties.getTableFilter())
          .isNull(); // Should be null since we used filter columns
      assertThat(properties.getTableFilterSql())
          .isNull(); // Should be null since we used filter columns
    }

    @Test
    @DisplayName("Test builder with MyBatis statement source")
    void testBuilderWithMybatisStatementSource() {
      String statementId = "test.statement";
      Map<String, Object> params = new HashMap<>();
      params.put("key", "value");

      SCD2Merge.Builder builder =
          SCD2Merge.applyChanges(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceMybatisStatement(statementId, params)
              .effectiveTimestamp(effectiveTimestamp)
              .keyColumns(keyColumns)
              .operationTypeColumn(operationTypeColumn, deleteOperationValue)
              .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn);

      SCD2MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getMybatisStatementId()).isEqualTo(statementId);
      assertThat(properties.getMybatisStatementParameter()).isEqualTo(params);
      assertThat(properties.getSql()).isNull(); // Should be null since we used MyBatis statement
    }

    @Test
    @DisplayName("Test effectiveTimestamp with LocalDateTime")
    void testEffectiveTimestampWithLocalDateTime() {
      SCD2Merge.Builder builder =
          SCD2Merge.applyChanges(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceSql(sql)
              .effectiveTimestamp(now)
              .keyColumns(keyColumns)
              .operationTypeColumn(operationTypeColumn, deleteOperationValue)
              .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn);

      SCD2MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getEffectiveTimestamp()).isEqualTo(effectiveTimestamp);
    }

    @Test
    @DisplayName("Test generateEffectiveTimestamp")
    void testGenerateEffectiveTimestamp() {
      SCD2Merge.Builder builder =
          SCD2Merge.applyChanges(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceSql(sql)
              .generateEffectiveTimestamp(true)
              .keyColumns(keyColumns)
              .operationTypeColumn(operationTypeColumn, deleteOperationValue)
              .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn);

      SCD2MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.isGenerateEffectiveTimestamp()).isTrue();
    }

    @Test
    @DisplayName("Test change tracking metadata")
    void testChangeTrackingMetadata() {
      Map<String, ChangeTrackingMetadata<?>> metadataMap = new HashMap<>();
      metadataMap.put("name", new ChangeTrackingMetadata<>(10.0, "N/A"));
      metadataMap.put("value", new ChangeTrackingMetadata<>(5.0, 0));

      SCD2Merge.Builder builder =
          SCD2Merge.applyChanges(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceSql(sql)
              .effectiveTimestamp(effectiveTimestamp)
              .keyColumns(keyColumns)
              .operationTypeColumn(operationTypeColumn, deleteOperationValue)
              .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
              .changeTrackingMetadata(metadataMap);

      SCD2MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getChangeTrackingMetadataMap()).containsAllEntriesOf(metadataMap);
    }

    @Test
    @DisplayName("Test single column change tracking metadata")
    void testSingleColumnChangeTrackingMetadata() {
      ChangeTrackingMetadata<Double> metadata = new ChangeTrackingMetadata<>(10.0, null);

      SCD2Merge.Builder builder =
          SCD2Merge.applyChanges(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceSql(sql)
              .effectiveTimestamp(effectiveTimestamp)
              .keyColumns(keyColumns)
              .operationTypeColumn(operationTypeColumn, deleteOperationValue)
              .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
              .changeTrackingMetadata("name", metadata);

      SCD2MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getChangeTrackingMetadataMap()).containsEntry("name", metadata);
    }

    @Test
    @DisplayName("Test duplicate column names are normalized")
    void testDuplicateColumnNamesAreNormalized() {
      List<String> columnsWithDuplicates =
          Arrays.asList("id", "code", "name", "value", "id", "code");
      List<String> expectedColumns = Arrays.asList("id", "code", "name", "value");

      SCD2Merge.Builder builder =
          SCD2Merge.applyChanges(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceSql(sql)
              .effectiveTimestamp(effectiveTimestamp)
              .keyColumns(columnsWithDuplicates)
              .operationTypeColumn(operationTypeColumn, deleteOperationValue)
              .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
              .columns(columnsWithDuplicates)
              .changeTrackingColumns(columnsWithDuplicates);

      SCD2MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getKeyColumns()).isEqualTo(expectedColumns);
      assertThat(properties.getColumns()).isEqualTo(expectedColumns);
      assertThat(properties.getChangeTrackingColumns()).isEqualTo(expectedColumns);
    }

    @Test
    @DisplayName("Test builder with branch, snapshot metadata and isolation level")
    void testBuilderWithCommitProperties() {
      String branchName = "test-branch";
      Map<String, String> metadata = Map.of("key", "value");

      SCD2Merge.BuilderImpl builder =
          (SCD2Merge.BuilderImpl)
              SCD2Merge.applyChanges(mockEngine, tableName)
                  .tableFilter(mockExpression)
                  .sourceSql(sql)
                  .effectiveTimestamp(effectiveTimestamp)
                  .keyColumns(keyColumns)
                  .operationTypeColumn(operationTypeColumn, deleteOperationValue)
                  .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
                  .branch(branchName)
                  .snapshotMetadata(metadata)
                  .isolationLevel(IsolationLevel.SNAPSHOT);

      assertThat(builder).extracting("branch").isEqualTo(branchName);
      assertThat(builder).extracting("snapshotMetadata").isEqualTo(metadata);
      assertThat(builder).extracting("isolationLevel").isEqualTo(IsolationLevel.SNAPSHOT);
    }
  }

  @Nested
  @DisplayName("Snapshot Mode Builder Configuration Tests")
  class SnapshotModeBuilderConfigurationTests {

    @Test
    @DisplayName("Test snapshot mode builder chain with table filter expression")
    void testSnapshotModeBuilderChainWithFilterExpression() {
      SCD2Merge.SnapshotModeBuilder builder =
          SCD2Merge.applySnapshot(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceSql(sql)
              .effectiveTimestamp(effectiveTimestamp)
              .keyColumns(keyColumns)
              .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
              .columns(columns)
              .changeTrackingColumns(changeTrackingColumns)
              .currentFlagColumn(currentFlagColumn)
              .skipDataSorting(true)
              .executeSourceSqlOnceOnly(true)
              .skipEmptySource(true)
              .processSourceTables(true);

      SCD2MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.getTableFilter()).isEqualTo(mockExpression);
      assertThat(properties.getSql()).isEqualTo(sql);
      assertThat(properties.getEffectiveTimestamp()).isEqualTo(effectiveTimestamp);
      assertThat(properties.getKeyColumns()).isEqualTo(keyColumns);
      assertThat(properties.getEffectiveStartColumn()).isEqualTo(effectiveStartColumn);
      assertThat(properties.getEffectiveEndColumn()).isEqualTo(effectiveEndColumn);
      assertThat(properties.getColumns()).isEqualTo(columns);
      assertThat(properties.getChangeTrackingColumns()).isEqualTo(changeTrackingColumns);
      assertThat(properties.getCurrentFlagColumn()).isEqualTo(currentFlagColumn);
      assertThat(properties.isSkipDataSorting()).isTrue();
      assertThat(properties.isExecuteSourceSqlOnceOnly()).isTrue();
      assertThat(properties.isSkipEmptySource()).isTrue();
      assertThat(properties.isProcessSourceTables()).isTrue();
      assertThat(properties.getMode()).isEqualTo(SCD2MergeMode.SNAPSHOT);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
    }

    @Test
    @DisplayName("Test snapshot mode builder with table filter SQL")
    void testSnapshotModeBuilderWithFilterSql() {
      String filterSql = "id > 100";
      SCD2Merge.SnapshotModeBuilder builder =
          SCD2Merge.applySnapshot(mockEngine, tableName)
              .tableFilterSql(filterSql)
              .sourceSql(sql)
              .effectiveTimestamp(effectiveTimestamp)
              .keyColumns(keyColumns)
              .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn);

      SCD2MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.getTableFilterSql()).isEqualTo(filterSql);
      assertThat(properties.getSql()).isEqualTo(sql);
      assertThat(properties.getEffectiveTimestamp()).isEqualTo(effectiveTimestamp);
      assertThat(properties.getKeyColumns()).isEqualTo(keyColumns);
      assertThat(properties.getTableFilter()).isNull(); // Should be null since we used SQL filter
    }

    @Test
    @DisplayName("Test snapshot mode with LocalDateTime")
    void testSnapshotModeWithLocalDateTime() {
      SCD2Merge.SnapshotModeBuilder builder =
          SCD2Merge.applySnapshot(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceSql(sql)
              .effectiveTimestamp(now)
              .keyColumns(keyColumns)
              .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn);

      SCD2MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.getEffectiveTimestamp()).isEqualTo(effectiveTimestamp);
    }

    @Test
    @DisplayName("Test snapshot mode with generate effective timestamp")
    void testSnapshotModeWithGenerateEffectiveTimestamp() {
      SCD2Merge.SnapshotModeBuilder builder =
          SCD2Merge.applySnapshot(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceSql(sql)
              .generateEffectiveTimestamp(true)
              .keyColumns(keyColumns)
              .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn);

      SCD2MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.isGenerateEffectiveTimestamp()).isTrue();
    }
  }

  @Nested
  @DisplayName("Validation Tests")
  class ValidationTests {

    @Test
    @DisplayName("Test validation of setting branch on batch transaction in changes mode")
    void testValidationErrorOnBatchTransactionWithBranchInChangesMode() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  SCD2Merge.applyChanges(mockEngine, mockTableBatchTransaction)
                      .tableFilter(mockExpression)
                      .sourceSql(sql)
                      .effectiveTimestamp(effectiveTimestamp)
                      .keyColumns(keyColumns)
                      .operationTypeColumn(operationTypeColumn, deleteOperationValue)
                      .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
                      .branch("test-branch"))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set branch name on the batch transaction");
    }

    @Test
    @DisplayName(
        "Test validation of setting snapshot metadata on batch transaction in changes mode")
    void testValidationErrorOnBatchTransactionWithMetadataInChangesMode() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  SCD2Merge.applyChanges(mockEngine, mockTableBatchTransaction)
                      .tableFilter(mockExpression)
                      .sourceSql(sql)
                      .effectiveTimestamp(effectiveTimestamp)
                      .keyColumns(keyColumns)
                      .operationTypeColumn(operationTypeColumn, deleteOperationValue)
                      .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
                      .snapshotMetadata(Map.of("key", "value")))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set snapshot metadata on the batch transaction");
    }

    @Test
    @DisplayName("Test validation of setting isolation level on batch transaction in changes mode")
    void testValidationErrorOnBatchTransactionWithIsolationLevelInChangesMode() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  SCD2Merge.applyChanges(mockEngine, mockTableBatchTransaction)
                      .tableFilter(mockExpression)
                      .sourceSql(sql)
                      .effectiveTimestamp(effectiveTimestamp)
                      .keyColumns(keyColumns)
                      .operationTypeColumn(operationTypeColumn, deleteOperationValue)
                      .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
                      .isolationLevel(IsolationLevel.SERIALIZABLE))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set isolation level on the batch transaction");
    }

    @Test
    @DisplayName("Test validation of setting branch on batch transaction in snapshot mode")
    void testValidationErrorOnBatchTransactionWithBranchInSnapshotMode() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  SCD2Merge.applySnapshot(mockEngine, mockTableBatchTransaction)
                      .tableFilter(mockExpression)
                      .sourceSql(sql)
                      .effectiveTimestamp(effectiveTimestamp)
                      .keyColumns(keyColumns)
                      .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
                      .branch("test-branch"))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set branch name on the batch transaction");
    }

    @Test
    @DisplayName(
        "Test validation of setting snapshot metadata on batch transaction in snapshot mode")
    void testValidationErrorOnBatchTransactionWithMetadataInSnapshotMode() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  SCD2Merge.applySnapshot(mockEngine, mockTableBatchTransaction)
                      .tableFilter(mockExpression)
                      .sourceSql(sql)
                      .effectiveTimestamp(effectiveTimestamp)
                      .keyColumns(keyColumns)
                      .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
                      .snapshotMetadata(Map.of("key", "value")))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set snapshot metadata on the batch transaction");
    }

    @Test
    @DisplayName("Test validation of setting isolation level on batch transaction in snapshot mode")
    void testValidationErrorOnBatchTransactionWithIsolationLevelInSnapshotMode() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  SCD2Merge.applySnapshot(mockEngine, mockTableBatchTransaction)
                      .tableFilter(mockExpression)
                      .sourceSql(sql)
                      .effectiveTimestamp(effectiveTimestamp)
                      .keyColumns(keyColumns)
                      .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
                      .isolationLevel(IsolationLevel.SERIALIZABLE))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set isolation level on the batch transaction");
    }

    @Test
    @DisplayName("Test validation of invalid effective timestamp format")
    void testValidationErrorOnInvalidEffectiveTimestampFormat() {
      assertThatThrownBy(
              () ->
                  SCD2Merge.applyChanges(mockEngine, tableName)
                      .tableFilter(mockExpression)
                      .sourceSql(sql)
                      .effectiveTimestamp("invalid-date-format")
                      .keyColumns(keyColumns)
                      .operationTypeColumn(operationTypeColumn, deleteOperationValue)
                      .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Invalid effectiveTimestamp");
    }
  }

  @Nested
  @DisplayName("Initialization Tests")
  class InitializationTests {

    @Test
    @DisplayName("Test initialization with table name in changes mode")
    void testInitializationWithTableNameInChangesMode() {
      when(mockEngine.getTable(tableName, true)).thenReturn(mockTable);

      SCD2Merge.BuilderImpl builder =
          (SCD2Merge.BuilderImpl) SCD2Merge.applyChanges(mockEngine, tableName);

      assertThat(builder).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builder).extracting("table").isEqualTo(mockTable);
      SCD2MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
      assertThat(properties.getMode()).isEqualTo(SCD2MergeMode.CHANGES);

      verify(mockEngine).getTable(tableName, true);
    }

    @Test
    @DisplayName("Test initialization with table instance in changes mode")
    void testInitializationWithTableInChangesMode() {
      SCD2Merge.BuilderImpl builder =
          (SCD2Merge.BuilderImpl) SCD2Merge.applyChanges(mockEngine, mockTable);

      assertThat(builder).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builder).extracting("table").isEqualTo(mockTable);
      SCD2MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
      assertThat(properties.getMode()).isEqualTo(SCD2MergeMode.CHANGES);
    }

    @Test
    @DisplayName("Test initialization with batch transaction in changes mode")
    void testInitializationWithBatchTransactionInChangesMode() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      SCD2Merge.BuilderImpl builder =
          (SCD2Merge.BuilderImpl) SCD2Merge.applyChanges(mockEngine, mockTableBatchTransaction);

      assertThat(builder).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builder).extracting("table").isEqualTo(mockTable);
      assertThat(builder).extracting("tableBatchTransaction").isEqualTo(mockTableBatchTransaction);
      SCD2MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
      assertThat(properties.getMode()).isEqualTo(SCD2MergeMode.CHANGES);
    }

    @Test
    @DisplayName("Test initialization with table name in snapshot mode")
    void testInitializationWithTableNameInSnapshotMode() {
      when(mockEngine.getTable(tableName, true)).thenReturn(mockTable);

      SCD2Merge.SnapshotModeBuilderImpl builder =
          (SCD2Merge.SnapshotModeBuilderImpl) SCD2Merge.applySnapshot(mockEngine, tableName);

      assertThat(builder).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builder).extracting("table").isEqualTo(mockTable);
      SCD2MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
      assertThat(properties.getMode()).isEqualTo(SCD2MergeMode.SNAPSHOT);

      verify(mockEngine).getTable(tableName, true);
    }

    @Test
    @DisplayName("Test initialization with table instance in snapshot mode")
    void testInitializationWithTableInSnapshotMode() {
      SCD2Merge.SnapshotModeBuilderImpl builder =
          (SCD2Merge.SnapshotModeBuilderImpl) SCD2Merge.applySnapshot(mockEngine, mockTable);

      assertThat(builder).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builder).extracting("table").isEqualTo(mockTable);
      SCD2MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
      assertThat(properties.getMode()).isEqualTo(SCD2MergeMode.SNAPSHOT);
    }

    @Test
    @DisplayName("Test initialization with batch transaction in snapshot mode")
    void testInitializationWithBatchTransactionInSnapshotMode() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      SCD2Merge.SnapshotModeBuilderImpl builder =
          (SCD2Merge.SnapshotModeBuilderImpl)
              SCD2Merge.applySnapshot(mockEngine, mockTableBatchTransaction);

      assertThat(builder).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builder).extracting("table").isEqualTo(mockTable);
      assertThat(builder).extracting("tableBatchTransaction").isEqualTo(mockTableBatchTransaction);
      SCD2MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
      assertThat(properties.getMode()).isEqualTo(SCD2MergeMode.SNAPSHOT);
    }
  }

  /** Helper method to access the properties field in the builder using reflection */
  private SCD2MergeProperties extractPropertiesFromBuilder(SCD2Merge.Builder builder) {
    try {
      java.lang.reflect.Field propertiesField =
          SCD2Merge.BuilderImpl.class.getDeclaredField("properties");
      propertiesField.setAccessible(true);
      return (SCD2MergeProperties) propertiesField.get((SCD2Merge.BuilderImpl) builder);
    } catch (Exception e) {
      throw new RuntimeException("Failed to access properties field", e);
    }
  }

  /** Helper method to access the properties field in the snapshot mode builder using reflection */
  private SCD2MergeProperties extractPropertiesFromSnapshotModeBuilder(
      SCD2Merge.SnapshotModeBuilder builder) {
    try {
      java.lang.reflect.Field propertiesField =
          SCD2Merge.SnapshotModeBuilderImpl.class.getDeclaredField("properties");
      propertiesField.setAccessible(true);
      return (SCD2MergeProperties) propertiesField.get((SCD2Merge.SnapshotModeBuilderImpl) builder);
    } catch (Exception e) {
      throw new RuntimeException("Failed to access properties field", e);
    }
  }
}
