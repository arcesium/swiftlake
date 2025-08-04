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

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.TestUtil;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;

public class SCD2MergeIntegrationTestUtil {
  public static void insertDataIntoSCD2Table(
      SwiftLakeEngine swiftLakeEngine,
      String tableName,
      Schema schema,
      List<Map<String, Object>> data,
      String effectiveStartTime) {
    insertDataIntoSCD2Table(swiftLakeEngine, tableName, schema, data, effectiveStartTime, null);
  }

  public static void insertDataIntoSCD2Table(
      SwiftLakeEngine swiftLakeEngine,
      String tableName,
      Schema schema,
      List<Map<String, Object>> data,
      String effectiveStartTime,
      String currentFlagColumn) {
    if (data == null) return;

    StringBuilder initialInsertSql = new StringBuilder();
    initialInsertSql
        .append("SELECT * REPLACE(TIMESTAMP'")
        .append(effectiveStartTime)
        .append("' AS effective_start");
    if (currentFlagColumn != null) {
      initialInsertSql.append(", true AS ").append(currentFlagColumn);
    }
    initialInsertSql.append(") FROM (").append(TestUtil.createSelectSql(data, schema)).append(")");
    swiftLakeEngine
        .insertInto(tableName)
        .sql(initialInsertSql.toString())
        .processSourceTables(false)
        .execute();
  }

  public static List<Map<String, Object>> addNullEffectiveEnd(
      List<Map<String, Object>> expectedData) {
    if (expectedData == null) return null;

    List<Map<String, Object>> expectedDataCopy = new ArrayList<>();
    expectedData.forEach(
        m -> {
          Map<String, Object> row = new HashMap<>(m);
          expectedDataCopy.add(row);
          if (!row.containsKey("effective_end")) row.put("effective_end", null);
        });

    return expectedDataCopy;
  }

  public static void performSCD2Merge(
      SwiftLakeEngine swiftLakeEngine,
      boolean isSnapshotMode,
      String tableName,
      String tableFilterSql,
      List<String> keyColumns,
      String sourceSql,
      LocalDateTime effectiveTimestamp) {
    performSCD2Merge(
        swiftLakeEngine,
        isSnapshotMode,
        tableName,
        tableFilterSql,
        keyColumns,
        sourceSql,
        effectiveTimestamp,
        null,
        Map.of(),
        null,
        "effective_start",
        "effective_end",
        false,
        "operation_type",
        "D");
  }

  public static void performSCD2Merge(
      SwiftLakeEngine swiftLakeEngine,
      boolean isSnapshotMode,
      String tableName,
      String tableFilterSql,
      List<String> keyColumns,
      String sourceSql,
      LocalDateTime effectiveTimestamp,
      List<String> changeTrackingColumns,
      Map<String, ChangeTrackingMetadata<?>> changeTrackingMetadata,
      String currentFlagColumn) {
    if (isSnapshotMode) {
      var builder =
          swiftLakeEngine
              .applySnapshotAsSCD2(tableName)
              .tableFilterSql(tableFilterSql)
              .sourceSql(sourceSql)
              .effectiveTimestamp(effectiveTimestamp)
              .keyColumns(keyColumns)
              .effectivePeriodColumns("effective_start", "effective_end")
              .processSourceTables(false);
      if (changeTrackingColumns != null) {
        builder.changeTrackingColumns(changeTrackingColumns);
      }
      if (changeTrackingMetadata != null) {
        builder.changeTrackingMetadata(changeTrackingMetadata);
      }
      if (currentFlagColumn != null) {
        builder.currentFlagColumn(currentFlagColumn);
      }

      builder.execute();
    } else {
      var builder =
          swiftLakeEngine
              .applyChangesAsSCD2(tableName)
              .tableFilterSql(tableFilterSql)
              .sourceSql(sourceSql)
              .effectiveTimestamp(effectiveTimestamp)
              .keyColumns(keyColumns)
              .operationTypeColumn("operation_type", "D")
              .effectivePeriodColumns("effective_start", "effective_end")
              .processSourceTables(false);
      if (changeTrackingColumns != null) {
        builder.changeTrackingColumns(changeTrackingColumns);
      }
      if (changeTrackingMetadata != null) {
        builder.changeTrackingMetadata(changeTrackingMetadata);
      }
      if (currentFlagColumn != null) {
        builder.currentFlagColumn(currentFlagColumn);
      }
      builder.execute();
    }
  }

  public static void performSCD2Merge(
      SwiftLakeEngine swiftLakeEngine,
      boolean isSnapshotMode,
      String tableName,
      String tableFilter,
      List<String> keyColumns,
      String sourceSql,
      LocalDateTime effectiveTimestamp,
      List<String> changeTrackingColumns,
      Map<String, ChangeTrackingMetadata<?>> changeTrackingMetadata,
      String currentFlagColumn,
      String effectiveStartColumn,
      String effectiveEndColumn,
      boolean generateEffectiveTimestamp,
      String operationTypeColumn,
      String deleteOperationValue) {

    if (isSnapshotMode) {
      var builder =
          swiftLakeEngine
              .applySnapshotAsSCD2(tableName)
              .tableFilterSql(tableFilter)
              .sourceSql(sourceSql);
      SCD2Merge.SnapshotModeSetKeyColumns setKeyColummns = null;
      if (generateEffectiveTimestamp) {
        setKeyColummns = builder.generateEffectiveTimestamp(true);
      } else {
        setKeyColummns = builder.effectiveTimestamp(effectiveTimestamp);
      }
      setKeyColummns
          .keyColumns(keyColumns)
          .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
          .processSourceTables(false)
          .currentFlagColumn(currentFlagColumn)
          .changeTrackingColumns(changeTrackingColumns)
          .changeTrackingMetadata(changeTrackingMetadata)
          .execute();
    } else {
      var builder =
          swiftLakeEngine
              .applyChangesAsSCD2(tableName)
              .tableFilterSql(tableFilter)
              .sourceSql(sourceSql);
      SCD2Merge.SetKeyColumns setKeyColummns = null;
      if (generateEffectiveTimestamp) {
        setKeyColummns = builder.generateEffectiveTimestamp(true);
      } else {
        setKeyColummns = builder.effectiveTimestamp(effectiveTimestamp);
      }
      setKeyColummns
          .keyColumns(keyColumns)
          .operationTypeColumn(operationTypeColumn, deleteOperationValue)
          .effectivePeriodColumns(effectiveStartColumn, effectiveEndColumn)
          .currentFlagColumn(currentFlagColumn)
          .changeTrackingColumns(changeTrackingColumns)
          .changeTrackingMetadata(changeTrackingMetadata)
          .processSourceTables(false)
          .execute();
    }
  }
}
