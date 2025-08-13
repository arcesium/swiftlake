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
class SCD1MergeTest {
  @Mock private SwiftLakeSqlSessionFactory mockSqlSessionFactory;
  @Mock private SwiftLakeEngine mockEngine;
  @Mock private Table mockTable;
  @Mock private TableBatchTransaction mockTableBatchTransaction;
  @Mock private Expression mockExpression;

  private String tableName = "test_table";
  private String sql = "SELECT * FROM source_table";
  private List<String> keyColumns = Arrays.asList("id", "code");
  private List<String> columns = Arrays.asList("id", "code", "name", "value");

  @BeforeEach
  void setUp() {}

  @Nested
  @DisplayName("Builder Configuration Tests")
  class BuilderConfigurationTests {

    @Test
    @DisplayName("Test builder chain with table filter expression")
    void testBuilderChainWithFilterExpression() {
      when(mockEngine.getSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
      SCD1Merge.Builder builder =
          SCD1Merge.applyChanges(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceSql(sql)
              .keyColumns(keyColumns)
              .columns(columns)
              .skipDataSorting(true)
              .executeSourceSqlOnceOnly(true)
              .operationTypeColumn("operation", "DELETE")
              .processSourceTables(true);

      SCD1MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getTableFilter()).isEqualTo(mockExpression);
      assertThat(properties.getSql()).isEqualTo(sql);
      assertThat(properties.getKeyColumns()).isEqualTo(keyColumns);
      assertThat(properties.getColumns()).isEqualTo(columns);
      assertThat(properties.isSkipDataSorting()).isTrue();
      assertThat(properties.isExecuteSourceSqlOnceOnly()).isTrue();
      assertThat(properties.getOperationTypeColumn()).isEqualTo("operation");
      assertThat(properties.getDeleteOperationValue()).isEqualTo("DELETE");
      assertThat(properties.isProcessSourceTables()).isTrue();
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
    }

    @Test
    @DisplayName("Test builder chain with table filter SQL")
    void testBuilderChainWithFilterSql() {
      String filterSql = "id > 100";
      SCD1Merge.Builder builder =
          SCD1Merge.applyChanges(mockEngine, tableName)
              .tableFilterSql(filterSql)
              .sourceSql(sql)
              .keyColumns(keyColumns);

      SCD1MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getTableFilterSql()).isEqualTo(filterSql);
      assertThat(properties.getSql()).isEqualTo(sql);
      assertThat(properties.getKeyColumns()).isEqualTo(keyColumns);
      assertThat(properties.getTableFilter()).isNull(); // Should be null since we used SQL filter
    }

    @Test
    @DisplayName("Test builder chain with table filter columns")
    void testBuilderChainWithFilterColumns() {
      List<String> filterColumns = Arrays.asList("region", "department");
      SCD1Merge.Builder builder =
          SCD1Merge.applyChanges(mockEngine, tableName)
              .tableFilterColumns(filterColumns)
              .sourceSql(sql)
              .keyColumns(keyColumns);

      SCD1MergeProperties properties = extractPropertiesFromBuilder(builder);
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

      SCD1Merge.Builder builder =
          SCD1Merge.applyChanges(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceMybatisStatement(statementId, params)
              .keyColumns(keyColumns);

      SCD1MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getMybatisStatementId()).isEqualTo(statementId);
      assertThat(properties.getMybatisStatementParameter()).isEqualTo(params);
      assertThat(properties.getSql()).isNull(); // Should be null since we used MyBatis statement
    }

    @Test
    @DisplayName("Test duplicate column names are normalized")
    void testDuplicateColumnNamesAreNormalized() {
      List<String> columnsWithDuplicates =
          Arrays.asList("id", "code", "name", "value", "id", "code");
      List<String> expectedColumns = Arrays.asList("id", "code", "name", "value");

      SCD1Merge.Builder builder =
          SCD1Merge.applyChanges(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceSql(sql)
              .keyColumns(columnsWithDuplicates)
              .columns(columnsWithDuplicates);

      SCD1MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getKeyColumns()).isEqualTo(expectedColumns);
      assertThat(properties.getColumns()).isEqualTo(expectedColumns);
    }

    @Test
    @DisplayName("Test all properties are properly initialized")
    void testAllPropertiesAreProperlyInitialized() {
      when(mockEngine.getSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
      SCD1Merge.Builder builder =
          SCD1Merge.applyChanges(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceSql(sql)
              .keyColumns(keyColumns)
              .columns(columns)
              .operationTypeColumn("operation", "DELETE")
              .skipDataSorting(true)
              .executeSourceSqlOnceOnly(true)
              .processSourceTables(true);

      SCD1MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties)
          .hasFieldOrPropertyWithValue("tableFilter", mockExpression)
          .hasFieldOrPropertyWithValue("sql", sql)
          .hasFieldOrPropertyWithValue("keyColumns", keyColumns)
          .hasFieldOrPropertyWithValue("columns", columns)
          .hasFieldOrPropertyWithValue("operationTypeColumn", "operation")
          .hasFieldOrPropertyWithValue("deleteOperationValue", "DELETE")
          .hasFieldOrPropertyWithValue("skipDataSorting", true)
          .hasFieldOrPropertyWithValue("executeSourceSqlOnceOnly", true)
          .hasFieldOrPropertyWithValue("processSourceTables", true)
          .hasFieldOrPropertyWithValue("sqlSessionFactory", mockSqlSessionFactory);
    }

    @Test
    @DisplayName("Test builder with branch, snapshot metadata and isolation level")
    void testBuilderWithCommitProperties() {
      String branchName = "test-branch";
      Map<String, String> metadata = Map.of("key", "value");

      SCD1Merge.BuilderImpl builder =
          (SCD1Merge.BuilderImpl)
              SCD1Merge.applyChanges(mockEngine, tableName)
                  .tableFilter(mockExpression)
                  .sourceSql(sql)
                  .keyColumns(keyColumns)
                  .branch(branchName)
                  .snapshotMetadata(metadata)
                  .isolationLevel(IsolationLevel.SNAPSHOT);

      assertThat(builder).extracting("branch").isEqualTo(branchName);
      assertThat(builder).extracting("snapshotMetadata").isEqualTo(metadata);
      assertThat(builder).extracting("isolationLevel").isEqualTo(IsolationLevel.SNAPSHOT);
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
                  SCD1Merge.applyChanges(mockEngine, mockTableBatchTransaction)
                      .tableFilter(mockExpression)
                      .sourceSql(sql)
                      .keyColumns(keyColumns)
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
                  SCD1Merge.applyChanges(mockEngine, mockTableBatchTransaction)
                      .tableFilter(mockExpression)
                      .sourceSql(sql)
                      .keyColumns(keyColumns)
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
                  SCD1Merge.applyChanges(mockEngine, mockTableBatchTransaction)
                      .tableFilter(mockExpression)
                      .sourceSql(sql)
                      .keyColumns(keyColumns)
                      .isolationLevel(IsolationLevel.SERIALIZABLE))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set isolation level on the batch transaction");
    }
  }

  @Nested
  @DisplayName("Initialization Tests")
  class InitializationTests {

    @Test
    @DisplayName("Test initialization with table name")
    void testInitializationWithTableName() {
      when(mockEngine.getTable(tableName, true)).thenReturn(mockTable);
      when(mockEngine.getSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
      SCD1Merge.BuilderImpl builder =
          (SCD1Merge.BuilderImpl) SCD1Merge.applyChanges(mockEngine, tableName);

      assertThat(builder).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builder).extracting("table").isEqualTo(mockTable);
      SCD1MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
      assertThat(properties.isProcessSourceTables()).isEqualTo(false);
      assertThat(properties.getMode()).isEqualTo(SCD1MergeMode.CHANGES);

      verify(mockEngine).getTable(tableName, true);
    }

    @Test
    @DisplayName("Test initialization with table instance")
    void testInitializationWithTable() {
      when(mockEngine.getSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
      SCD1Merge.BuilderImpl builder =
          (SCD1Merge.BuilderImpl) SCD1Merge.applyChanges(mockEngine, mockTable);

      assertThat(builder).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builder).extracting("table").isEqualTo(mockTable);
      SCD1MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
      assertThat(properties.getMode()).isEqualTo(SCD1MergeMode.CHANGES);
    }

    @Test
    @DisplayName("Test initialization with batch transaction")
    void testInitializationWithBatchTransaction() {
      when(mockEngine.getSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      SCD1Merge.BuilderImpl builder =
          (SCD1Merge.BuilderImpl) SCD1Merge.applyChanges(mockEngine, mockTableBatchTransaction);

      assertThat(builder).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builder).extracting("table").isEqualTo(mockTable);
      assertThat(builder).extracting("tableBatchTransaction").isEqualTo(mockTableBatchTransaction);
      SCD1MergeProperties properties = extractPropertiesFromBuilder(builder);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
      assertThat(properties.getMode()).isEqualTo(SCD1MergeMode.CHANGES);
    }
  }

  @Nested
  @DisplayName("Snapshot Mode Builder Configuration Tests")
  class SnapshotModeBuilderConfigurationTests {

    @Test
    @DisplayName("Test snapshot mode builder chain with basic configuration")
    void testSnapshotModeBuilderChain() {
      when(mockEngine.getSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
      SCD1Merge.SnapshotModeBuilder builder =
          SCD1Merge.applySnapshot(mockEngine, tableName)
              .tableFilter(mockExpression)
              .sourceSql(sql)
              .keyColumns(keyColumns)
              .columns(columns)
              .skipDataSorting(true)
              .executeSourceSqlOnceOnly(true)
              .processSourceTables(true);

      SCD1MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.getTableFilter()).isEqualTo(mockExpression);
      assertThat(properties.getSql()).isEqualTo(sql);
      assertThat(properties.getKeyColumns()).isEqualTo(keyColumns);
      assertThat(properties.getColumns()).isEqualTo(columns);
      assertThat(properties.isSkipDataSorting()).isTrue();
      assertThat(properties.isExecuteSourceSqlOnceOnly()).isTrue();
      assertThat(properties.isProcessSourceTables()).isTrue();
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
      assertThat(properties.getMode()).isEqualTo(SCD1MergeMode.SNAPSHOT);
    }

    @Test
    @DisplayName("Test snapshot mode with value columns configuration")
    void testSnapshotModeWithValueColumns() {
      List<String> valueColumns = Arrays.asList("name", "value");
      SCD1Merge.SnapshotModeBuilder builder =
          SCD1Merge.applySnapshot(mockEngine, tableName)
              .tableFilterSql("id > 100")
              .sourceSql(sql)
              .keyColumns(keyColumns)
              .valueColumns(valueColumns);

      SCD1MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.getValueColumns()).isEqualTo(valueColumns);
      assertThat(properties.getMode()).isEqualTo(SCD1MergeMode.SNAPSHOT);
    }

    @Test
    @DisplayName("Test snapshot mode with value column metadata")
    void testSnapshotModeWithValueColumnMetadata() {
      ValueColumnMetadata<Double> numericMetadata = new ValueColumnMetadata<>(0.01, null);
      ValueColumnMetadata<String> stringMetadata = new ValueColumnMetadata<>(null, "N/A");

      SCD1Merge.SnapshotModeBuilder builder =
          SCD1Merge.applySnapshot(mockEngine, tableName)
              .tableFilterSql("id > 100")
              .sourceSql(sql)
              .keyColumns(keyColumns)
              .valueColumns(Arrays.asList("name", "value"))
              .valueColumnMetadata("value", numericMetadata)
              .valueColumnMetadata("name", stringMetadata);

      SCD1MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      Map<String, ValueColumnMetadata<?>> expectedMetadata = new HashMap<>();
      expectedMetadata.put("value", numericMetadata);
      expectedMetadata.put("name", stringMetadata);

      assertThat(properties.getValueColumnMetadataMap()).containsAllEntriesOf(expectedMetadata);
    }

    @Test
    @DisplayName("Test snapshot mode with bulk value column metadata")
    void testSnapshotModeWithBulkValueColumnMetadata() {
      Map<String, ValueColumnMetadata<?>> metadataMap = new HashMap<>();
      metadataMap.put("value", new ValueColumnMetadata<>(0.01, null));
      metadataMap.put("name", new ValueColumnMetadata<>(null, "N/A"));

      SCD1Merge.SnapshotModeBuilder builder =
          SCD1Merge.applySnapshot(mockEngine, tableName)
              .tableFilterSql("id > 100")
              .sourceSql(sql)
              .keyColumns(keyColumns)
              .valueColumnsMetadata(metadataMap);

      SCD1MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.getValueColumnMetadataMap()).containsAllEntriesOf(metadataMap);
    }

    @Test
    @DisplayName("Test snapshot mode with skip empty source")
    void testSnapshotModeWithSkipEmptySource() {
      SCD1Merge.SnapshotModeBuilder builder =
          SCD1Merge.applySnapshot(mockEngine, tableName)
              .tableFilterSql("id > 100")
              .sourceSql(sql)
              .keyColumns(keyColumns)
              .skipEmptySource(true);

      SCD1MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.isSkipEmptySource()).isTrue();
    }
  }

  @Nested
  @DisplayName("Snapshot Mode Initialization Tests")
  class SnapshotModeInitializationTests {

    @Test
    @DisplayName("Test snapshot mode initialization with table name")
    void testSnapshotModeInitializationWithTableName() {
      when(mockEngine.getTable(tableName, true)).thenReturn(mockTable);
      when(mockEngine.getSqlSessionFactory()).thenReturn(mockSqlSessionFactory);

      SCD1Merge.SnapshotModeBuilderImpl builder =
          (SCD1Merge.SnapshotModeBuilderImpl) SCD1Merge.applySnapshot(mockEngine, tableName);

      assertThat(builder).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builder).extracting("table").isEqualTo(mockTable);

      SCD1MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
      assertThat(properties.getMode()).isEqualTo(SCD1MergeMode.SNAPSHOT);
      assertThat(properties.getValueColumnMetadataMap()).isNotNull();
      assertThat(properties.getValueColumnMetadataMap()).isEmpty();

      verify(mockEngine).getTable(tableName, true);
    }

    @Test
    @DisplayName("Test snapshot mode initialization with table instance")
    void testSnapshotModeInitializationWithTable() {
      when(mockEngine.getSqlSessionFactory()).thenReturn(mockSqlSessionFactory);

      SCD1Merge.SnapshotModeBuilderImpl builder =
          (SCD1Merge.SnapshotModeBuilderImpl) SCD1Merge.applySnapshot(mockEngine, mockTable);

      assertThat(builder).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builder).extracting("table").isEqualTo(mockTable);

      SCD1MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
      assertThat(properties.getMode()).isEqualTo(SCD1MergeMode.SNAPSHOT);
    }

    @Test
    @DisplayName("Test snapshot mode initialization with batch transaction")
    void testSnapshotModeInitializationWithBatchTransaction() {
      when(mockEngine.getSqlSessionFactory()).thenReturn(mockSqlSessionFactory);
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      SCD1Merge.SnapshotModeBuilderImpl builder =
          (SCD1Merge.SnapshotModeBuilderImpl)
              SCD1Merge.applySnapshot(mockEngine, mockTableBatchTransaction);

      assertThat(builder).extracting("swiftLakeEngine").isEqualTo(mockEngine);
      assertThat(builder).extracting("table").isEqualTo(mockTable);
      assertThat(builder).extracting("tableBatchTransaction").isEqualTo(mockTableBatchTransaction);

      SCD1MergeProperties properties = extractPropertiesFromSnapshotModeBuilder(builder);
      assertThat(properties.getSqlSessionFactory()).isEqualTo(mockSqlSessionFactory);
      assertThat(properties.getMode()).isEqualTo(SCD1MergeMode.SNAPSHOT);
    }
  }

  @Nested
  @DisplayName("Snapshot Mode Validation Tests")
  class SnapshotModeValidationTests {

    @Test
    @DisplayName("Test snapshot mode validation of setting branch on batch transaction")
    void testSnapshotModeValidationErrorOnBatchTransactionWithBranch() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  SCD1Merge.applySnapshot(mockEngine, mockTableBatchTransaction)
                      .tableFilter(mockExpression)
                      .sourceSql(sql)
                      .keyColumns(keyColumns)
                      .branch("test-branch"))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set branch name on the batch transaction");
    }

    @Test
    @DisplayName("Test snapshot mode validation of setting snapshot metadata on batch transaction")
    void testSnapshotModeValidationErrorOnBatchTransactionWithMetadata() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  SCD1Merge.applySnapshot(mockEngine, mockTableBatchTransaction)
                      .tableFilter(mockExpression)
                      .sourceSql(sql)
                      .keyColumns(keyColumns)
                      .snapshotMetadata(Map.of("key", "value")))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set snapshot metadata on the batch transaction");
    }

    @Test
    @DisplayName("Test snapshot mode validation of setting isolation level on batch transaction")
    void testSnapshotModeValidationErrorOnBatchTransactionWithIsolationLevel() {
      when(mockTableBatchTransaction.getTable()).thenReturn(mockTable);

      assertThatThrownBy(
              () ->
                  SCD1Merge.applySnapshot(mockEngine, mockTableBatchTransaction)
                      .tableFilter(mockExpression)
                      .sourceSql(sql)
                      .keyColumns(keyColumns)
                      .isolationLevel(IsolationLevel.SERIALIZABLE))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Set isolation level on the batch transaction");
    }
  }

  /** Helper method to access the properties field in the builder using reflection */
  private SCD1MergeProperties extractPropertiesFromBuilder(SCD1Merge.Builder builder) {
    try {
      java.lang.reflect.Field propertiesField =
          SCD1Merge.BuilderImpl.class.getDeclaredField("properties");
      propertiesField.setAccessible(true);
      return (SCD1MergeProperties) propertiesField.get((SCD1Merge.BuilderImpl) builder);
    } catch (Exception e) {
      throw new RuntimeException("Failed to access properties field", e);
    }
  }

  /** Helper method to access the properties field in the snapshot mode builder using reflection */
  private SCD1MergeProperties extractPropertiesFromSnapshotModeBuilder(
      SCD1Merge.SnapshotModeBuilder builder) {
    try {
      java.lang.reflect.Field propertiesField =
          SCD1Merge.SnapshotModeBuilderImpl.class.getDeclaredField("properties");
      propertiesField.setAccessible(true);
      return (SCD1MergeProperties) propertiesField.get((SCD1Merge.SnapshotModeBuilderImpl) builder);
    } catch (Exception e) {
      throw new RuntimeException("Failed to access properties field", e);
    }
  }
}
