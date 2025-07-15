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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.DataFile;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.expressions.ResolvedExpression;
import com.arcesium.swiftlake.metrics.CommitMetrics;
import com.arcesium.swiftlake.metrics.MetricCollector;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransactionTest {

  @Mock private SwiftLakeEngine mockEngine;
  @Mock private Table mockTable;
  @Mock private TableScan mockTableScan;
  @Mock private Snapshot mockSnapshot;
  @Mock private MetricCollector mockMetricCollector;
  @Mock private AppendFiles mockAppendFiles;
  @Mock private OverwriteFiles mockOverwriteFiles;
  @Mock private DataFile mockDataFile;
  @Mock private org.apache.iceberg.DataFile mockIcebergDataFile;

  private Transaction transaction;

  @BeforeEach
  void setUp() {
    when(mockEngine.getApplicationId()).thenReturn("testApp");
    when(mockEngine.getMetricCollector()).thenReturn(mockMetricCollector);
  }

  @Test
  void testBuilderForAppend() {
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);
    when(mockIcebergDataFile.recordCount()).thenReturn(10L);

    transaction =
        Transaction.builderFor(mockEngine, mockTable)
            .newDataFiles(Arrays.asList(mockDataFile))
            .branch("main")
            .build();

    assertThat(transaction.getTable()).isEqualTo(mockTable);
    assertThat(transaction.isOverwrite()).isFalse();
    assertThat(transaction.getNewDataFiles()).containsExactly(mockDataFile);
    assertThat(transaction.getBranch()).isEqualTo("main");
  }

  @Test
  void testBuilderForOverwrite() {
    configureMockTableScanWithSnapshotAndFilter(
        123L, Expressions.resolveExpression(Expressions.alwaysTrue()));
    transaction =
        Transaction.builderForOverwrite(mockEngine, mockTableScan)
            .deletedDataFiles(Arrays.asList(mockIcebergDataFile))
            .build();

    assertThat(transaction.isOverwrite()).isTrue();
    assertThat(transaction.getFromSnapshotId()).isEqualTo(123L);
    assertThat(transaction.getConflictDetectionFilter()).isNotNull();
    assertThat(transaction.getDeletedDataFiles()).containsExactly(mockIcebergDataFile);
  }

  @Test
  void testBuilderForOverwriteWithFilter() {
    Expression filter = Expressions.equal("column", "value");
    transaction = Transaction.builderForOverwrite(mockEngine, mockTable, filter).build();

    assertThat(transaction.isOverwrite()).isTrue();
    assertThat(transaction.getOverwriteByRowFilter().toString())
        .isEqualTo(Expressions.resolveExpression(filter).toString());
  }

  @Test
  void testCommitAppend() {
    when(mockTable.name()).thenReturn("testTable");
    PartitionSpec mockSpec = mock(PartitionSpec.class);
    when(mockSpec.isPartitioned()).thenReturn(false);
    when(mockTable.spec()).thenReturn(mockSpec);
    when(mockTable.newAppend()).thenReturn(mockAppendFiles);
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);
    when(mockIcebergDataFile.recordCount()).thenReturn(100L);

    transaction =
        Transaction.builderFor(mockEngine, mockTable)
            .newDataFiles(Arrays.asList(mockDataFile))
            .build();

    CommitMetrics metrics = transaction.commit();

    verify(mockAppendFiles).appendFile(mockIcebergDataFile);
    verify(mockAppendFiles).commit();
    assertThat(metrics).isNotNull();
    assertThat(metrics.getTableName()).isEqualTo("testTable");
    assertThat(metrics.getAddedFilesCount()).isEqualTo(1);
    assertThat(metrics.getAddedRecordsCount()).isEqualTo(100L);
  }

  @Test
  void testCommitOverwrite() {
    configureUnpartitionedMockTable();
    when(mockTable.newOverwrite()).thenReturn(mockOverwriteFiles);
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);
    when(mockIcebergDataFile.recordCount()).thenReturn(100L);
    ResolvedExpression filter = Expressions.resolveExpression(Expressions.equal("column", "value"));
    configureMockTableScanWithSnapshotAndFilter(123L, filter);

    transaction =
        Transaction.builderForOverwrite(mockEngine, mockTableScan)
            .newDataFiles(Arrays.asList(mockDataFile))
            .deletedDataFiles(Arrays.asList(mockIcebergDataFile))
            .build();

    CommitMetrics metrics = transaction.commit();

    verify(mockOverwriteFiles).addFile(mockIcebergDataFile);
    verify(mockOverwriteFiles).deleteFile(mockIcebergDataFile);
    verify(mockOverwriteFiles).commit();
    verify(mockOverwriteFiles).validateFromSnapshot(123L);
    verify(mockOverwriteFiles).conflictDetectionFilter(filter.getExpression());
    verify(mockOverwriteFiles).validateNoConflictingData();
    verify(mockOverwriteFiles).validateNoConflictingDeletes();

    assertThat(metrics).isNotNull();
    assertThat(metrics.getTableName()).isEqualTo("testTable");
    assertThat(metrics.getAddedFilesCount()).isEqualTo(1);
    assertThat(metrics.getRemovedFilesCount()).isEqualTo(1);
    assertThat(metrics.getAddedRecordsCount()).isEqualTo(100L);
  }

  @Test
  void testCommitOverwriteWithSnapshotIsolationLevel() {
    configureUnpartitionedMockTable();
    when(mockTable.newOverwrite()).thenReturn(mockOverwriteFiles);
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);
    when(mockIcebergDataFile.recordCount()).thenReturn(100L);
    ResolvedExpression filter = Expressions.resolveExpression(Expressions.equal("column", "value"));
    configureMockTableScanWithSnapshotAndFilter(123L, filter);

    transaction =
        Transaction.builderForOverwrite(mockEngine, mockTableScan)
            .newDataFiles(Arrays.asList(mockDataFile))
            .deletedDataFiles(Arrays.asList(mockIcebergDataFile))
            .isolationLevel(IsolationLevel.SNAPSHOT)
            .build();

    CommitMetrics metrics = transaction.commit();

    verify(mockOverwriteFiles).addFile(mockIcebergDataFile);
    verify(mockOverwriteFiles).deleteFile(mockIcebergDataFile);
    verify(mockOverwriteFiles).commit();
    verify(mockOverwriteFiles).validateFromSnapshot(123L);
    verify(mockOverwriteFiles).conflictDetectionFilter(filter.getExpression());
    verify(mockOverwriteFiles).validateNoConflictingDeletes();

    assertThat(metrics).isNotNull();
    assertThat(metrics.getTableName()).isEqualTo("testTable");
    assertThat(metrics.getAddedFilesCount()).isEqualTo(1);
    assertThat(metrics.getRemovedFilesCount()).isEqualTo(1);
    assertThat(metrics.getAddedRecordsCount()).isEqualTo(100L);
    assertThat(metrics.getRemovedRecordsCount()).isEqualTo(100L);
  }

  @Test
  void testCommitOverwriteWithRowFilter() {
    configureUnpartitionedMockTable();
    when(mockTable.newOverwrite()).thenReturn(mockOverwriteFiles);
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);
    when(mockIcebergDataFile.recordCount()).thenReturn(100L);

    ResolvedExpression filter = Expressions.resolveExpression(Expressions.equal("column", "value"));
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);

    transaction =
        Transaction.builderForOverwrite(mockEngine, mockTable, filter)
            .newDataFiles(Arrays.asList(mockDataFile))
            .build();

    transaction.commit();

    verify(mockOverwriteFiles).overwriteByRowFilter(filter.getExpression());
    verify(mockOverwriteFiles).validateAddedFilesMatchOverwriteFilter();
    verify(mockOverwriteFiles).validateNoConflictingData();
    verify(mockOverwriteFiles).validateNoConflictingDeletes();
    verify(mockOverwriteFiles).addFile(mockIcebergDataFile);
    verify(mockOverwriteFiles).commit();
  }

  @Test
  void testCommitOverwriteWithRowFilterWithSnapshotIsolationLevel() {
    configureUnpartitionedMockTable();
    when(mockTable.newOverwrite()).thenReturn(mockOverwriteFiles);
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);
    when(mockIcebergDataFile.recordCount()).thenReturn(100L);

    ResolvedExpression filter = Expressions.resolveExpression(Expressions.equal("column", "value"));
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);

    transaction =
        Transaction.builderForOverwrite(mockEngine, mockTable, filter)
            .newDataFiles(Arrays.asList(mockDataFile))
            .isolationLevel(IsolationLevel.SNAPSHOT)
            .build();

    transaction.commit();

    verify(mockOverwriteFiles).overwriteByRowFilter(filter.getExpression());
    verify(mockOverwriteFiles).validateAddedFilesMatchOverwriteFilter();
    verify(mockOverwriteFiles).validateNoConflictingDeletes();
    verify(mockOverwriteFiles).addFile(mockIcebergDataFile);
    verify(mockOverwriteFiles).commit();
  }

  @Test
  void testCommitWithSnapshotMetadata() {
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);
    when(mockIcebergDataFile.recordCount()).thenReturn(100L);
    when(mockTable.newAppend()).thenReturn(mockAppendFiles);
    Map<String, String> metadata = new HashMap<>();
    metadata.put("key", "value");

    transaction =
        Transaction.builderFor(mockEngine, mockTable)
            .newDataFiles(Arrays.asList(mockDataFile))
            .snapshotMetadata(metadata)
            .build();

    transaction.commit();

    verify(mockAppendFiles).set("key", "value");
    verify(mockAppendFiles).commit();
  }

  @Test
  void testCommitEmptyTransaction() {
    when(mockTable.name()).thenReturn("testTable");
    transaction = Transaction.builderFor(mockEngine, mockTable).build();

    CommitMetrics metrics = transaction.commit();

    verifyNoInteractions(mockAppendFiles, mockOverwriteFiles);
    assertThat(metrics).isNotNull();
    assertThat(metrics.getTableName()).isEqualTo("testTable");
    assertThat(metrics.getAddedFilesCount()).isZero();
    assertThat(metrics.getRemovedFilesCount()).isZero();
  }

  @Test
  void testCommitAppendWithEmptyFiles() {
    when(mockTable.name()).thenReturn("testTable");
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);
    when(mockIcebergDataFile.recordCount()).thenReturn(0L);

    transaction =
        Transaction.builderFor(mockEngine, mockTable)
            .newDataFiles(Arrays.asList(mockDataFile))
            .build();

    CommitMetrics metrics = transaction.commit();

    verifyNoInteractions(mockAppendFiles, mockOverwriteFiles);
    assertThat(metrics).isNotNull();
    assertThat(metrics.getTableName()).isEqualTo("testTable");
    assertThat(metrics.getAddedFilesCount()).isZero();
    assertThat(metrics.getRemovedFilesCount()).isZero();
  }

  @Test
  void testCommitOverwriteWithEmptyAppendFiles() {
    configureUnpartitionedMockTable();
    when(mockTable.newOverwrite()).thenReturn(mockOverwriteFiles);
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);
    when(mockIcebergDataFile.recordCount()).thenReturn(0L);
    org.apache.iceberg.DataFile mockDeletedDataFile = mock(org.apache.iceberg.DataFile.class);
    when(mockDeletedDataFile.recordCount()).thenReturn(100L);
    ResolvedExpression filter = Expressions.resolveExpression(Expressions.equal("column", "value"));
    configureMockTableScanWithSnapshotAndFilter(123L, filter);

    transaction =
        Transaction.builderForOverwrite(mockEngine, mockTableScan)
            .newDataFiles(Arrays.asList(mockDataFile))
            .deletedDataFiles(Arrays.asList(mockDeletedDataFile))
            .build();

    CommitMetrics metrics = transaction.commit();

    verify(mockOverwriteFiles, never()).addFile(mockIcebergDataFile);
    verify(mockOverwriteFiles).deleteFile(mockDeletedDataFile);
    verify(mockOverwriteFiles).commit();
    verify(mockOverwriteFiles).validateFromSnapshot(123L);
    verify(mockOverwriteFiles).conflictDetectionFilter(filter.getExpression());
    verify(mockOverwriteFiles).validateNoConflictingData();
    verify(mockOverwriteFiles).validateNoConflictingDeletes();

    assertThat(metrics).isNotNull();
    assertThat(metrics.getTableName()).isEqualTo("testTable");
    assertThat(metrics.getAddedFilesCount()).isEqualTo(0);
    assertThat(metrics.getRemovedFilesCount()).isEqualTo(1);
    assertThat(metrics.getAddedRecordsCount()).isEqualTo(0L);
    assertThat(metrics.getRemovedRecordsCount()).isEqualTo(100L);
  }

  @Test
  void testInvalidAppendWithDeletedFiles() {
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);
    when(mockIcebergDataFile.recordCount()).thenReturn(100L);
    transaction =
        Transaction.builderFor(mockEngine, mockTable)
            .newDataFiles(Arrays.asList(mockDataFile))
            .deletedDataFiles(Arrays.asList(mockIcebergDataFile))
            .build();

    assertThatThrownBy(() -> transaction.commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Files cannot be deleted in append operation");
  }

  @Test
  void testGetters() {
    when(mockDataFile.getIcebergDataFile()).thenReturn(mockIcebergDataFile);
    when(mockIcebergDataFile.recordCount()).thenReturn(100L);

    Expression conflictFilter = Expressions.alwaysTrue();
    transaction =
        Transaction.builderFor(mockEngine, mockTable)
            .newDataFiles(Arrays.asList(mockDataFile))
            .deletedDataFiles(Arrays.asList(mockIcebergDataFile))
            .branch("main")
            .snapshotMetadata(Map.of("key", "value"))
            .conflictDetectionFilter(conflictFilter)
            .build();

    assertThat(transaction.getTable()).isEqualTo(mockTable);
    assertThat(transaction.getNewDataFiles()).containsExactly(mockDataFile);
    assertThat(transaction.getDeletedDataFiles()).containsExactly(mockIcebergDataFile);
    assertThat(transaction.getBranch()).isEqualTo("main");
    assertThat(transaction.getSnapshotMetadata()).containsEntry("key", "value");
    assertThat(transaction.getConflictDetectionFilter().toString())
        .isEqualTo(Expressions.resolveExpression(conflictFilter).toString());
    assertThat(transaction.getApplicationId()).isEqualTo("testApp");
  }

  private void configureMockTableScanWithSnapshotAndFilter(
      long snapshotId, ResolvedExpression filter) {
    when(mockTableScan.snapshot()).thenReturn(mockSnapshot);
    when(mockSnapshot.snapshotId()).thenReturn(snapshotId);
    when(mockTableScan.filter()).thenReturn(filter.getExpression());
    when(mockTableScan.table()).thenReturn(mockTable);
  }

  private void configureUnpartitionedMockTable() {
    when(mockTable.name()).thenReturn("testTable");
    PartitionSpec mockSpec = mock(PartitionSpec.class);
    when(mockSpec.isPartitioned()).thenReturn(false);
    when(mockTable.spec()).thenReturn(mockSpec);
  }
}
