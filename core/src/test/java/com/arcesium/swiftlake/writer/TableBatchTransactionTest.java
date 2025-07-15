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
package com.arcesium.swiftlake.writer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.DataFile;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.metrics.CommitMetrics;
import com.arcesium.swiftlake.metrics.MetricCollector;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TableBatchTransactionTest {

  @Mock private SwiftLakeEngine mockEngine;
  @Mock private Table mockTable;
  @Mock private MetricCollector mockMetricCollector;
  @Mock private Transaction mockTransaction;
  @Mock private DataFile mockDataFile;
  @Mock private org.apache.iceberg.DataFile mockIcebergDataFile;

  private TableBatchTransaction batchTransaction;

  @BeforeEach
  void setUp() {
    when(mockEngine.getApplicationId()).thenReturn("testApp");
    when(mockEngine.getMetricCollector()).thenReturn(mockMetricCollector);
    batchTransaction =
        TableBatchTransaction.builderFor(mockEngine, mockTable)
            .branch("main")
            .snapshotMetadata(new HashMap<>())
            .isolationLevel(IsolationLevel.SERIALIZABLE)
            .build();
  }

  @Test
  void testGetTable() {
    assertThat(batchTransaction.getTable()).isEqualTo(mockTable);
  }

  @Test
  void testAdd_ValidTransaction() {
    when(mockTable.location()).thenReturn("/path/table");
    when(mockTransaction.getTable()).thenReturn(mockTable);
    when(mockTransaction.isOverwrite()).thenReturn(false);

    assertThatCode(() -> batchTransaction.add(mockTransaction)).doesNotThrowAnyException();
  }

  @Test
  void testAdd_DifferentTable() {
    when(mockTable.location()).thenReturn("/path/table");
    Table differentTable = mock(Table.class);
    when(differentTable.location()).thenReturn("/path/different-table");
    when(mockTransaction.getTable()).thenReturn(differentTable);

    assertThatThrownBy(() -> batchTransaction.add(mockTransaction))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot batch transactions of different tables");
  }

  @Test
  void testAdd_MixedTransactionTypes() {
    when(mockTable.location()).thenReturn("/path/table");
    when(mockTransaction.getTable()).thenReturn(mockTable);
    when(mockTransaction.isOverwrite()).thenReturn(false);
    batchTransaction.add(mockTransaction);

    Transaction overwriteTransaction = mock(Transaction.class);
    when(overwriteTransaction.getTable()).thenReturn(mockTable);
    when(overwriteTransaction.isOverwrite()).thenReturn(true);

    assertThatThrownBy(() -> batchTransaction.add(overwriteTransaction))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot mix the append transactions and overwrite transactions");
  }

  @Test
  void testCommit_EmptyBatch() {
    when(mockTable.name()).thenReturn("testTable");
    CommitMetrics metrics = batchTransaction.commit();
    assertThat(metrics).isNotNull();
    assertThat(metrics.getTableName()).isEqualTo("testTable");
  }

  @Test
  void testCommit_AppendTransactions() {
    configureUnpartitionedMockTable();
    when(mockTransaction.getTable()).thenReturn(mockTable);
    when(mockTransaction.isOverwrite()).thenReturn(false);
    when(mockTransaction.getNewDataFiles()).thenReturn(Arrays.asList(mockDataFile));
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);
    when(mockIcebergDataFile.recordCount()).thenReturn(10L);
    batchTransaction.add(mockTransaction);
    AppendFiles mockAppendFiles = mock(AppendFiles.class);
    when(mockTable.newAppend()).thenReturn(mockAppendFiles);

    CommitMetrics metrics = batchTransaction.commit();
    assertThat(metrics).isNotNull();
    assertThat(metrics.getTableName()).isEqualTo("testTable");
    assertThat(metrics.getAddedFilesCount()).isEqualTo(1);
    assertThat(metrics.getAddedRecordsCount()).isEqualTo(10L);
    verify(mockAppendFiles).appendFile(mockIcebergDataFile);
    verify(mockAppendFiles).commit();
  }

  @Test
  void testCommit_OverwriteTransactions() {
    configureUnpartitionedMockTable();
    when(mockTransaction.getTable()).thenReturn(mockTable);
    when(mockTransaction.isOverwrite()).thenReturn(true);
    when(mockTransaction.getNewDataFiles()).thenReturn(Arrays.asList(mockDataFile));
    when(mockIcebergDataFile.recordCount()).thenReturn(10L);
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);
    org.apache.iceberg.DataFile mockDeletedIcebergDataFile =
        mock(org.apache.iceberg.DataFile.class);
    when(mockDeletedIcebergDataFile.recordCount()).thenReturn(5L);
    when(mockDeletedIcebergDataFile.location()).thenReturn("/path/table/file1.parquet");
    when(mockTransaction.getDeletedDataFiles())
        .thenReturn(Arrays.asList(mockDeletedIcebergDataFile));
    when(mockTransaction.getConflictDetectionFilter())
        .thenReturn(Expressions.resolveExpression(Expressions.alwaysTrue()));
    batchTransaction.add(mockTransaction);

    OverwriteFiles mockOverwriteFiles = mock(OverwriteFiles.class);
    when(mockTable.newOverwrite()).thenReturn(mockOverwriteFiles);

    CommitMetrics metrics = batchTransaction.commit();

    assertThat(metrics).isNotNull();
    assertThat(metrics.getTableName()).isEqualTo("testTable");
    assertThat(metrics.getAddedFilesCount()).isEqualTo(1);
    assertThat(metrics.getAddedRecordsCount()).isEqualTo(10L);
    assertThat(metrics.getRemovedFilesCount()).isEqualTo(1);
    assertThat(metrics.getRemovedRecordsCount()).isEqualTo(5L);
    verify(mockOverwriteFiles).deleteFile(mockDeletedIcebergDataFile);
    verify(mockOverwriteFiles).addFile(mockIcebergDataFile);
    verify(mockOverwriteFiles).commit();
  }

  @Test
  void testToString() {
    when(mockTable.toString()).thenReturn("testTable");
    String result = batchTransaction.toString();
    assertThat(result).contains("testTable", "testApp", "main");
  }

  @Test
  void testBuilder() {
    TableBatchTransaction transaction =
        TableBatchTransaction.builderFor(mockEngine, "testTable")
            .branch("develop")
            .snapshotMetadata(Map.of("key", "value"))
            .isolationLevel(IsolationLevel.SNAPSHOT)
            .build();

    assertThat(transaction).isNotNull();
    assertThat(transaction).extracting("branch").isEqualTo("develop");
    assertThat(transaction).extracting("snapshotMetadata").isEqualTo(Map.of("key", "value"));
    assertThat(transaction).extracting("isolationLevel").isEqualTo(IsolationLevel.SNAPSHOT);
  }

  private void configureUnpartitionedMockTable() {
    when(mockTable.name()).thenReturn("testTable");
    when(mockTable.location()).thenReturn("/path/table");
    PartitionSpec mockSpec = mock(PartitionSpec.class);
    when(mockSpec.isPartitioned()).thenReturn(false);
    when(mockTable.spec()).thenReturn(mockSpec);
  }
}
