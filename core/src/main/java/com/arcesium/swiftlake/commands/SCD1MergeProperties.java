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

import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import java.util.List;
import java.util.Map;

/** Represents properties for SCD1 (Slowly Changing Dimension Type 1) merge operations. */
public class SCD1MergeProperties {
  private SCD1MergeMode mode;
  private Expression tableFilter;
  private String tableFilterSql;
  private List<String> tableFilterColumns;
  private String sql;
  private String mybatisStatementId;
  private Object mybatisStatementParameter;
  private boolean processSourceTables;
  private SwiftLakeSqlSessionFactory sqlSessionFactory;
  private boolean executeSourceSqlOnceOnly;
  private List<String> keyColumns;
  private List<String> valueColumns;
  private Map<String, ValueColumnMetadata<?>> valueColumnMetadataMap;
  private List<String> columns;
  private String operationTypeColumn;
  private String deleteOperationValue;
  private boolean skipEmptySource;
  private boolean skipDataSorting;

  // Internal
  private String sourceTableName;
  private String destinationTableName;
  private String boundaryCondition;
  private String diffsFilePath;
  private List<String> allColumns;
  private boolean appendOnly;
  private String compression;
  private String modifiedFileNamesFilePath;
  private Map<String, Double> valueColumnMaxDeltaValues;
  private Map<String, String> valueColumnNullReplacements;

  public SCD1MergeProperties() {}

  public SCD1MergeMode getMode() {
    return mode;
  }

  public void setMode(SCD1MergeMode mode) {
    this.mode = mode;
  }

  public Expression getTableFilter() {
    return tableFilter;
  }

  public void setTableFilter(Expression tableFilter) {
    this.tableFilter = tableFilter;
  }

  public String getTableFilterSql() {
    return tableFilterSql;
  }

  public void setTableFilterSql(String tableFilterSql) {
    this.tableFilterSql = tableFilterSql;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public List<String> getKeyColumns() {
    return keyColumns;
  }

  public void setKeyColumns(List<String> keyColumns) {
    this.keyColumns = keyColumns;
  }

  public List<String> getValueColumns() {
    return valueColumns;
  }

  public void setValueColumns(List<String> valueColumns) {
    this.valueColumns = valueColumns;
  }

  public Map<String, ValueColumnMetadata<?>> getValueColumnMetadataMap() {
    return valueColumnMetadataMap;
  }

  public void setValueColumnMetadataMap(
      Map<String, ValueColumnMetadata<?>> valueColumnMetadataMap) {
    this.valueColumnMetadataMap = valueColumnMetadataMap;
  }

  public List<String> getColumns() {
    return columns;
  }

  public void setColumns(List<String> columns) {
    this.columns = columns;
  }

  public String getOperationTypeColumn() {
    return operationTypeColumn;
  }

  public void setOperationTypeColumn(String operationTypeColumn) {
    this.operationTypeColumn = operationTypeColumn;
  }

  public String getDeleteOperationValue() {
    return deleteOperationValue;
  }

  public void setDeleteOperationValue(String deleteOperationValue) {
    this.deleteOperationValue = deleteOperationValue;
  }

  public String getMybatisStatementId() {
    return mybatisStatementId;
  }

  public void setMybatisStatementId(String mybatisStatementId) {
    this.mybatisStatementId = mybatisStatementId;
  }

  public Object getMybatisStatementParameter() {
    return mybatisStatementParameter;
  }

  public void setMybatisStatementParameter(Object mybatisStatementParameter) {
    this.mybatisStatementParameter = mybatisStatementParameter;
  }

  public String getSourceTableName() {
    return sourceTableName;
  }

  public void setSourceTableName(String sourceTableName) {
    this.sourceTableName = sourceTableName;
  }

  public String getDestinationTableName() {
    return destinationTableName;
  }

  public void setDestinationTableName(String destinationTableName) {
    this.destinationTableName = destinationTableName;
  }

  public String getDiffsFilePath() {
    return diffsFilePath;
  }

  public void setDiffsFilePath(String diffsFilePath) {
    this.diffsFilePath = diffsFilePath;
  }

  public List<String> getAllColumns() {
    return allColumns;
  }

  public void setAllColumns(List<String> allColumns) {
    this.allColumns = allColumns;
  }

  public boolean isAppendOnly() {
    return appendOnly;
  }

  public void setAppendOnly(boolean appendOnly) {
    this.appendOnly = appendOnly;
  }

  public String getBoundaryCondition() {
    return boundaryCondition;
  }

  public void setBoundaryCondition(String boundaryCondition) {
    this.boundaryCondition = boundaryCondition;
  }

  public List<String> getTableFilterColumns() {
    return tableFilterColumns;
  }

  public void setTableFilterColumns(List<String> tableFilterColumns) {
    this.tableFilterColumns = tableFilterColumns;
  }

  public String getCompression() {
    return compression;
  }

  public void setCompression(String compression) {
    this.compression = compression;
  }

  public boolean isExecuteSourceSqlOnceOnly() {
    return executeSourceSqlOnceOnly;
  }

  public void setExecuteSourceSqlOnceOnly(boolean executeSourceSqlOnceOnly) {
    this.executeSourceSqlOnceOnly = executeSourceSqlOnceOnly;
  }

  public boolean isSkipEmptySource() {
    return skipEmptySource;
  }

  public void setSkipEmptySource(boolean skipEmptySource) {
    this.skipEmptySource = skipEmptySource;
  }

  public boolean isSkipDataSorting() {
    return skipDataSorting;
  }

  public void setSkipDataSorting(boolean skipDataSorting) {
    this.skipDataSorting = skipDataSorting;
  }

  public SwiftLakeSqlSessionFactory getSqlSessionFactory() {
    return sqlSessionFactory;
  }

  public void setSqlSessionFactory(SwiftLakeSqlSessionFactory sqlSessionFactory) {
    this.sqlSessionFactory = sqlSessionFactory;
  }

  public String getModifiedFileNamesFilePath() {
    return modifiedFileNamesFilePath;
  }

  public void setModifiedFileNamesFilePath(String modifiedFileNamesFilePath) {
    this.modifiedFileNamesFilePath = modifiedFileNamesFilePath;
  }

  public boolean isProcessSourceTables() {
    return processSourceTables;
  }

  public void setProcessSourceTables(boolean processSourceTables) {
    this.processSourceTables = processSourceTables;
  }

  public Map<String, Double> getValueColumnMaxDeltaValues() {
    return valueColumnMaxDeltaValues;
  }

  public void setValueColumnMaxDeltaValues(Map<String, Double> valueColumnMaxDeltaValues) {
    this.valueColumnMaxDeltaValues = valueColumnMaxDeltaValues;
  }

  public Map<String, String> getValueColumnNullReplacements() {
    return valueColumnNullReplacements;
  }

  public void setValueColumnNullReplacements(Map<String, String> valueColumnNullReplacements) {
    this.valueColumnNullReplacements = valueColumnNullReplacements;
  }

  @Override
  public String toString() {
    return "MergeProperties{"
        + "mode="
        + mode
        + ", tableFilter="
        + tableFilter
        + ", tableFilterSql='"
        + tableFilterSql
        + '\''
        + ", tableFilterColumns="
        + tableFilterColumns
        + ", sql='"
        + sql
        + '\''
        + ", mybatisStatementId='"
        + mybatisStatementId
        + '\''
        + ", mybatisStatementParameter="
        + mybatisStatementParameter
        + ", processSourceTables="
        + processSourceTables
        + ", executeSourceSqlOnceOnly="
        + executeSourceSqlOnceOnly
        + ", keyColumns="
        + keyColumns
        + ", valueColumns="
        + valueColumns
        + ", valueColumnMetadataMap="
        + valueColumnMetadataMap
        + ", columns="
        + columns
        + ", operationColumnName='"
        + operationTypeColumn
        + '\''
        + ", deleteOperationValue='"
        + deleteOperationValue
        + '\''
        + ", skipEmptySource="
        + skipEmptySource
        + ", skipDataSorting="
        + skipDataSorting
        + ", boundaryCondition='"
        + boundaryCondition
        + '\''
        + ", appendOnly="
        + appendOnly
        + ", compression='"
        + compression
        + '\''
        + '}';
  }
}
