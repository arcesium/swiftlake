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

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.commands.WriteUtil;
import com.arcesium.swiftlake.common.ArrayStructLike;
import com.arcesium.swiftlake.common.DataFile;
import com.arcesium.swiftlake.common.DataImportUtil;
import com.arcesium.swiftlake.common.FileUtil;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.dao.PartitionedDataDao;
import com.arcesium.swiftlake.sql.SchemaEvolution;
import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A class for writing partitioned data files to an Iceberg table. */
public class PartitionedDataFileWriter extends BaseDataFileWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedDataFileWriter.class);
  private static final String PARTITION_DATA_ESCAPE = "#";
  private static final String PARTITION_DATA_NULL_VALUE = PARTITION_DATA_ESCAPE + "n";
  private static final String PARTITION_DATA_UUID_VALUE = PARTITION_DATA_ESCAPE + "u";
  private static final Integer PARTITION_VALUE_MAX_LENGTH = 150;
  private final String tmpDir;
  private final String tmpDirBaseFolder;
  private final PartitionedDataDao partitionedDataDao;
  private final String currentFlagColumn;
  private final String effectiveStartColumn;
  private final String effectiveEndColumn;
  private final String partitionDataSql;
  private final String sql;
  private final SchemaEvolution schemaEvolution;
  private final Schema schema;
  private final SortOrder sortOrder;
  private final Long targetFileSizeBytes;
  private final Long rowGroupSize;

  /**
   * Constructs a new PartitionedDataFileWriter.
   *
   * @param swiftLakeEngine The SwiftLake engine instance
   * @param table The table to write to
   * @param currentFlagColumn The column indicating current/active records
   * @param effectiveStartColumn The column for effective start date
   * @param effectiveEndColumn The column for effective end date
   * @param partitionDataSql SQL for retrieving partition data
   * @param sql The main SQL query
   * @param schema The schema of the data
   * @param sortOrder The sort order for the data
   * @param targetFileSizeBytes The target file size in bytes
   * @param rowGroupSize The size of row groups
   */
  private PartitionedDataFileWriter(
      SwiftLakeEngine swiftLakeEngine,
      Table table,
      String currentFlagColumn,
      String effectiveStartColumn,
      String effectiveEndColumn,
      String partitionDataSql,
      String sql,
      Schema schema,
      SortOrder sortOrder,
      Long targetFileSizeBytes,
      Long rowGroupSize) {
    super(swiftLakeEngine, table);
    this.partitionedDataDao = new PartitionedDataDao(swiftLakeEngine);
    this.currentFlagColumn = currentFlagColumn;
    this.effectiveStartColumn = effectiveStartColumn;
    this.effectiveEndColumn = effectiveEndColumn;
    this.partitionDataSql = partitionDataSql;
    this.sql = sql;
    this.schemaEvolution = swiftLakeEngine.getSchemaEvolution();
    this.schema = schema;
    this.sortOrder = sortOrder;
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.rowGroupSize = rowGroupSize;

    this.tmpDirBaseFolder = getUniqueName("pdfw");
    this.tmpDir = swiftLakeEngine.getLocalDir() + "/" + tmpDirBaseFolder;
    new File(this.tmpDir).mkdirs();
  }

  /**
   * Writes the data files and returns a list of the written files.
   *
   * @return A list of DataFile objects representing the written files
   */
  @Override
  public List<DataFile> write() {
    try {
      // Create distinct partitions data table
      PartitionData partitionData = createDistinctPartitionsDataTable();
      if (partitionData == null) {
        LOGGER.info("Nothing to write.");
        return new ArrayList<>();
      }
      // Write data files based on partition data
      return writeDataFiles(partitionData);
    } finally {
      // Clean up temporary directory
      try {
        FileUtils.deleteDirectory(new File(tmpDir));
      } catch (IOException e) {
        LOGGER.warn("Unable to delete the temporary directory {}", tmpDir);
      }
    }
  }

  /**
   * Creates a distinct partitions data table.
   *
   * @return PartitionData object containing partition information
   */
  private PartitionData createDistinctPartitionsDataTable() {
    List<Types.NestedField> partitionFieldsExcludingKdColumns = new ArrayList<>();
    Set<String> singleBucketColumns = new HashSet<>();
    boolean currentFlagColumnPartitionExists = false;
    boolean kedPartitionExists = false;
    PartitionSpec partitionSpec = table.spec();
    String singleBucketTransformStr = "bucket[1]";
    for (PartitionField pf : partitionSpec.fields()) {
      Types.NestedField field = table.schema().findField(pf.sourceId());
      if (currentFlagColumn != null && field.name().equalsIgnoreCase(currentFlagColumn)) {
        currentFlagColumnPartitionExists = true;
        if (!pf.transform().isIdentity()) {
          throw new ValidationException(
              "Partition transform of the column %s should be identity.", field.name());
        }
      } else if (effectiveStartColumn != null
          && field.name().equalsIgnoreCase(effectiveStartColumn)) {
        throw new ValidationException("Partitioning on the column %s not supported.", field.name());
      } else if (effectiveEndColumn != null && field.name().equalsIgnoreCase(effectiveEndColumn)) {
        if (!pf.transform().toString().equalsIgnoreCase(singleBucketTransformStr)) {
          throw new ValidationException(
              "Partitioning transform of the column %s should be %s.",
              field.name(), singleBucketTransformStr);
        }
        kedPartitionExists = true;
      } else if (pf.transform().toString().equalsIgnoreCase(singleBucketTransformStr)) {
        singleBucketColumns.add(field.name());
      } else {
        partitionFieldsExcludingKdColumns.add(field);
      }
    }

    List<String> projectedPartitionColumnsExcludingKdColumns =
        schemaEvolution.getProjectionListWithTypeCasting(partitionFieldsExcludingKdColumns, null);

    List<Map<String, Object>> impactedPartitions =
        partitionedDataDao.getDistinctPartitions(
            partitionDataSql,
            projectedPartitionColumnsExcludingKdColumns,
            currentFlagColumnPartitionExists ? currentFlagColumn : null,
            kedPartitionExists ? effectiveEndColumn : null,
            singleBucketColumns);

    if (kedPartitionExists) {
      singleBucketColumns.add(effectiveEndColumn);
    }
    List<Pair<Map<String, Object>, StructLike>> partitionDataList =
        impactedPartitions.stream()
            .map(
                partitionValues ->
                    Pair.of(
                        partitionValues,
                        getPartitionValuesStruct(table, partitionValues, singleBucketColumns)))
            .collect(Collectors.toList());

    if (partitionDataList.isEmpty()) {
      return null;
    } else if (partitionDataList.size() == 1) {
      return new PartitionData(partitionDataList.get(0), null, null, null);
    } else {
      Instant start = Instant.now();
      Pair<String, Map<String, String>> data =
          createPartitionsDataFile(table, partitionDataList, singleBucketColumns);
      LOGGER.debug(
          "Successfully created partition data file {}. TimeTaken: {} ms ",
          data.getLeft(),
          Duration.between(start, Instant.now()).toMillis());
      return new PartitionData(null, data.getLeft(), singleBucketColumns, data.getValue());
    }
  }

  /**
   * Retrieves the list of partition column names from the table schema.
   *
   * @return A list of partition column names.
   */
  private List<String> getPartitionColumns() {
    return table.spec().fields().stream()
        .map(pf -> table.schema().findField(pf.sourceId()).name())
        .collect(Collectors.toList());
  }

  /**
   * Writes data files based on the provided partition data.
   *
   * @param partitionData The partition data to be written.
   * @return A list of DataFile objects representing the written files.
   */
  private List<DataFile> writeDataFiles(PartitionData partitionData) {
    if (partitionData.partitionDataFile != null) {
      return writeDataFilesWithMultiplePartitions(partitionData);
    } else {
      return writeDataFilesWithSinglePartition(partitionData.singlePartitionData);
    }
  }

  /**
   * Writes data files for a single partition.
   *
   * @param partitionData A pair containing partition key-value map and StructLike representation.
   * @return A list of DataFile objects representing the written files.
   */
  private List<DataFile> writeDataFilesWithSinglePartition(
      Pair<Map<String, Object>, StructLike> partitionData) {
    try (Connection connection = swiftLakeEngine.createConnection(false);
        Statement stmt = connection.createStatement()) {
      return uploadDataFilesForPartitionKey(table, sql, stmt, partitionData.getRight());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Writes data files for multiple partitions.
   *
   * @param partitionData The partition data containing multiple partitions.
   * @return A list of DataFile objects representing the written files.
   */
  private List<DataFile> writeDataFilesWithMultiplePartitions(PartitionData partitionData) {
    List<String> partitionColumns = getPartitionColumns();
    String outputPathBaseFolder = getUniqueName("data");
    String outputPath = tmpDir + "/" + outputPathBaseFolder;

    // Write partitioned data to temporary location
    partitionedDataDao.write(
        sql,
        partitionData.partitionDataFile,
        outputPath,
        compression,
        getProjectedColumns(),
        partitionColumns,
        effectiveEndColumn,
        partitionData.singleBucketColumns);

    if (FileUtil.isEmptyFolder(outputPath)) return new ArrayList<>();

    // Upload debug files if necessary
    WriteUtil.uploadDebugFiles(
        fileIO, swiftLakeEngine.getDebugFileUploadPath(), outputPath, outputPathBaseFolder);

    String prefix = "__";
    String suffix = "__";

    // Prepare DuckDB SQL for reading partitioned data
    StringBuilder dataPath = new StringBuilder();
    dataPath.append(outputPath).append("/*".repeat(partitionColumns.size())).append("/*.parquet");
    StringBuilder hiveTypesForDistinctPartitions = new StringBuilder();
    hiveTypesForDistinctPartitions.append("{");
    boolean isFirst = true;
    for (Pair<String, Type> pair : getResultTypesForPartitionColumns(table)) {
      if (!isFirst) {
        hiveTypesForDistinctPartitions.append(",");
      }
      isFirst = false;
      hiveTypesForDistinctPartitions
          .append("'")
          .append(prefix)
          .append(pair.getKey())
          .append(suffix)
          .append("': '")
          .append(swiftLakeEngine.getSchemaEvolution().getPrimitiveTypeExpr(pair.getValue()))
          .append("'");
    }
    hiveTypesForDistinctPartitions.append("}");

    StringBuilder tableName = new StringBuilder();
    tableName
        .append("read_parquet(['")
        .append(dataPath)
        .append("'], hive_partitioning=1, union_by_name=True, hive_types=")
        .append(hiveTypesForDistinctPartitions)
        .append(")");
    StringBuilder tableNameForDistinctPartitions = new StringBuilder();
    tableNameForDistinctPartitions
        .append("read_parquet(['")
        .append(dataPath)
        .append("'], hive_partitioning=1, union_by_name=True, hive_types=")
        .append(hiveTypesForDistinctPartitions)
        .append(")");

    // Get distinct partition values
    List<Map<String, Object>> distinctPartitions =
        partitionedDataDao.getDistinctPartitionValuesFromTable(
            tableNameForDistinctPartitions.toString(), partitionColumns);

    List<Future<List<DataFile>>> futures = new ArrayList<>();

    // Process each partition concurrently
    for (Map<String, Object> partitionKey : distinctPartitions) {
      futures.add(
          swiftLakeEngine
              .getPartitionWriterThreadPool()
              .submit(
                  () -> {
                    String condition =
                        getDuckDBFilterForPartitionTransforms(table, partitionKey, prefix, suffix)
                            .stream()
                            .collect(Collectors.joining(" AND "));

                    String sql = "SELECT * FROM " + tableName + " WHERE " + condition;
                    StructLike partitionStruct =
                        getPartitionValuesStructForTransformedValue(
                            table,
                            partitionKey,
                            partitionData.transformValueLookupMap,
                            prefix,
                            suffix);

                    try (Connection connection = swiftLakeEngine.createConnection(false);
                        Statement stmt = connection.createStatement()) {
                      return uploadDataFilesForPartitionKey(table, sql, stmt, partitionStruct);
                    } catch (SQLException e) {
                      throw new RuntimeException(e);
                    }
                  }));
    }
    List<DataFile> newFiles = new ArrayList<>();
    // Collect results from all futures
    futures.forEach(
        (f) -> {
          try {
            newFiles.addAll(f.get());
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          } catch (ExecutionException e) {
            throw new RuntimeException(e);
          }
        });

    return newFiles;
  }

  /**
   * Creates a new Builder instance for PartitionedDataFileWriter.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param table The Table instance
   * @param partitionDataSql SQL for partition data
   * @param sql The main SQL query
   * @return A new Builder instance
   */
  public static Builder builderFor(
      SwiftLakeEngine swiftLakeEngine, Table table, String partitionDataSql, String sql) {
    return new Builder(swiftLakeEngine, table, partitionDataSql, sql);
  }

  /** Builder class for PartitionedDataFileWriter. */
  public static class Builder {
    private final SwiftLakeEngine swiftLakeEngine;
    private final Table table;
    private final String partitionDataSql;
    private final String sql;
    private String currentFlagColumn;
    private String effectiveStartColumn;
    private String effectiveEndColumn;
    private Schema schema;
    private SortOrder sortOrder;
    private Long targetFileSizeBytes;
    private Long rowGroupSize;

    /**
     * Private constructor for Builder.
     *
     * @param swiftLakeEngine The SwiftLakeEngine instance
     * @param table The Table instance
     * @param partitionDataSql SQL for partition data
     * @param sql The main SQL query
     */
    private Builder(
        SwiftLakeEngine swiftLakeEngine, Table table, String partitionDataSql, String sql) {
      this.swiftLakeEngine = swiftLakeEngine;
      this.table = table;
      this.partitionDataSql = partitionDataSql;
      this.sql = sql;
      this.schema = table.schema();
      this.sortOrder = table.sortOrder();
      Map<String, String> props = table.properties();
      this.targetFileSizeBytes =
          PropertyUtil.propertyAsLong(
              props,
              TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
              TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
      this.rowGroupSize =
          PropertyUtil.propertyAsLong(
              props, PARQUET_ROW_GROUP_SIZE, PARQUET_ROW_GROUP_SIZE_DEFAULT);
    }

    /**
     * Sets the current flag column.
     *
     * @param currentFlagColumn The name of the current flag column
     * @return This Builder instance
     */
    public Builder currentFlagColumn(String currentFlagColumn) {
      this.currentFlagColumn = currentFlagColumn;
      return this;
    }

    /**
     * Sets the effective start column.
     *
     * @param effectiveStartColumn The name of the effective start column
     * @return This Builder instance
     */
    public Builder effectiveStartColumn(String effectiveStartColumn) {
      this.effectiveStartColumn = effectiveStartColumn;
      return this;
    }

    /**
     * Sets the effective end column.
     *
     * @param effectiveEndColumn The name of the effective end column
     * @return This Builder instance
     */
    public Builder effectiveEndColumn(String effectiveEndColumn) {
      this.effectiveEndColumn = effectiveEndColumn;
      return this;
    }

    /**
     * Sets the sort order.
     *
     * @param sortOrder The SortOrder instance
     * @return This Builder instance
     */
    public Builder sortOrder(SortOrder sortOrder) {
      this.sortOrder = sortOrder;
      return this;
    }

    /**
     * Skips data sorting if the parameter is true.
     *
     * @param skipDataSorting Whether to skip data sorting
     * @return This Builder instance
     */
    public Builder skipDataSorting(boolean skipDataSorting) {
      if (skipDataSorting) {
        this.sortOrder = null;
      }
      return this;
    }

    /**
     * Sets the target file size in bytes.
     *
     * @param targetFileSizeBytes The target file size in bytes
     * @return This Builder instance
     */
    public Builder targetFileSizeBytes(Long targetFileSizeBytes) {
      this.targetFileSizeBytes = targetFileSizeBytes;
      return this;
    }

    /**
     * Sets the row group size.
     *
     * @param rowGroupSize The row group size
     * @return This Builder instance
     */
    public Builder rowGroupSize(Long rowGroupSize) {
      this.rowGroupSize = rowGroupSize;
      return this;
    }

    /**
     * Builds and returns a new PartitionedDataFileWriter instance.
     *
     * @return A new PartitionedDataFileWriter instance
     */
    public PartitionedDataFileWriter build() {
      return new PartitionedDataFileWriter(
          swiftLakeEngine,
          table,
          currentFlagColumn,
          effectiveStartColumn,
          effectiveEndColumn,
          partitionDataSql,
          sql,
          schema,
          sortOrder,
          targetFileSizeBytes,
          rowGroupSize);
    }
  }

  /**
   * Creates a StructLike object representing partition values for a given table.
   *
   * @param table The Iceberg table
   * @param partitionValues Map of partition column names to their values
   * @param singleBucketColumns Set of column names that should be treated as single bucket
   * @return StructLike object representing partition values
   * @throws ValidationException if partition data is invalid
   */
  private StructLike getPartitionValuesStruct(
      Table table, Map<String, Object> partitionValues, Set<String> singleBucketColumns) {
    PartitionSpec spec = table.spec();
    if (spec.isPartitioned() && partitionValues == null) {
      throw new ValidationException("Provide partition data for partitioned table.");
    } else if (!spec.isPartitioned() && partitionValues != null) {
      throw new ValidationException("Cannot add partition data for an un-partitioned table");
    }

    if (partitionValues == null) return null;

    Schema schema = table.schema();
    int size = spec.fields().size();

    Object[] values = new Object[size];
    for (int i = 0; i < size; i += 1) {
      PartitionField pf = spec.fields().get(i);
      Types.NestedField field = schema.findField(pf.sourceId());
      if (field == null)
        throw new ValidationException(
            "Could not find the partition column in the schema for %s", pf.name());

      if (!partitionValues.containsKey(field.name()))
        throw new ValidationException(
            "Could not find the value for the partition column %s", field.name());

      Object value = partitionValues.get(field.name());

      if (singleBucketColumns.contains(field.name())) {
        values[i] = (value == null ? null : 0);
      } else {
        if (value != null) {
          if (field.type().typeId() == Type.TypeID.DATE
              || field.type().typeId() == Type.TypeID.TIME
              || field.type().typeId() == Type.TypeID.TIMESTAMP) {
            String stringValue =
                WriteUtil.getFormattedValueForDateTimes(field.type().asPrimitiveType(), value);
            value = Literal.of(stringValue).to(field.type()).value();
          }
        }
        Accessor<StructLike> accessor = schema.accessorForField(pf.sourceId());
        ValidationException.check(accessor != null, "Cannot build accessor for field: %s", field);
        SerializableFunction transform = pf.transform().bind(accessor.type());

        values[i] = transform.apply(value);
      }
    }
    return new ArrayStructLike(values);
  }

  /**
   * Creates a partition data file for the given table and partition data.
   *
   * @param table The Iceberg table
   * @param partitionDataList List of partition data pairs
   * @param singleBucketColumns Set of column names that should be treated as single bucket
   * @return Pair of SQL query string and transform value lookup map
   */
  private Pair<String, Map<String, String>> createPartitionsDataFile(
      Table table,
      List<Pair<Map<String, Object>, StructLike>> partitionDataList,
      Set<String> singleBucketColumns) {
    PartitionSpec spec = table.spec();
    Schema schema = table.schema();
    Class<?>[] javaClasses = spec.javaClasses();
    List<PartitionField> fields = spec.fields();

    List<Pair<String, Type>> columns = new ArrayList<>();

    String transformColumnSuffix = "_transformed";
    for (PartitionField pf : fields) {
      Types.NestedField f = schema.findField(pf.sourceId());
      Type resultType = getResultTypeForPartitionTransformValues(pf, f);
      columns.add(
          Pair.of(
              f.name(),
              singleBucketColumns.contains(f.name()) ? Types.BooleanType.get() : f.type()));
      columns.add(Pair.of(f.name() + transformColumnSuffix, resultType));
    }

    List<Map<String, Object>> data = new ArrayList<>();
    Map<String, String> transformValueLookupMap = new HashMap<>();
    for (Pair<Map<String, Object>, StructLike> partitionDataPair : partitionDataList) {
      StructLike partitionData = partitionDataPair.getRight();
      Map<String, Object> record = new HashMap<>();
      data.add(record);
      for (int i = 0; i < fields.size(); i++) {
        PartitionField pf = fields.get(i);
        Types.NestedField f = schema.findField(pf.sourceId());
        Object value = partitionDataPair.getLeft().get(f.name());
        record.put(f.name(), value);

        Object transformValue = partitionData.get(i, javaClasses[i]);
        Type resultType = getResultTypeForPartitionTransformValues(pf, f);
        if (resultType.typeId() == Type.TypeID.STRING) {
          transformValue = encodePartitionValue((String) transformValue);
          if (partitionValueUrlEncodedLength((String) transformValue)
              > PARTITION_VALUE_MAX_LENGTH) {
            if (transformValueLookupMap.containsKey(transformValue)) {
              transformValue = transformValueLookupMap.get(transformValue);
            } else {
              String uuid = PARTITION_DATA_UUID_VALUE + UUID.randomUUID();
              transformValueLookupMap.put((String) transformValue, uuid);
              transformValue = uuid;
            }
          }
        }
        record.put(f.name() + transformColumnSuffix, transformValue);
      }
    }

    String fileName = getUniqueName("partition_data") + ".parquet";
    String filePath = tmpDir + "/" + fileName;
    DataImportUtil.writeMapsToParquetFile(columns, data, filePath);
    WriteUtil.uploadDebugFile(
        fileIO, swiftLakeEngine.getDebugFileUploadPath(), filePath, tmpDirBaseFolder, fileName);
    return Pair.of(
        "(SELECT * FROM read_parquet('" + filePath + "'))",
        transformValueLookupMap.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey)));
  }

  /**
   * Encodes partition values to handle special cases like null and empty values.
   *
   * @param value The partition value to encode
   * @return Encoded partition value
   */
  private String encodePartitionValue(String value) {
    if (value == null) {
      value = PARTITION_DATA_NULL_VALUE;
    } else if (value.startsWith(PARTITION_DATA_ESCAPE) || value.equalsIgnoreCase("null")) {
      value = PARTITION_DATA_ESCAPE + value;
    }
    return value;
  }

  private int partitionValueUrlEncodedLength(String value) {
    if (value == null) {
      return 0;
    }
    return URLEncoder.encode(value, StandardCharsets.UTF_8).length();
  }

  /**
   * Decodes encoded partition values.
   *
   * @param value The encoded partition value
   * @return Decoded partition value
   */
  private String decodePartitionValue(String value) {
    if (value == null) {
      return null;
    }

    if (value.equals(PARTITION_DATA_NULL_VALUE)) {
      return null;
    }

    if (value.startsWith(PARTITION_DATA_ESCAPE)) {
      return value.substring(PARTITION_DATA_ESCAPE.length());
    }

    return value;
  }

  /**
   * Generates DuckDB filter expressions for partition transforms.
   *
   * @param table The Iceberg table
   * @param values Map of column names to their values
   * @param namePrefix Prefix to be added to column names
   * @param nameSuffix Suffix to be added to column names
   * @return List of filter expressions
   */
  private List<String> getDuckDBFilterForPartitionTransforms(
      Table table, Map<String, Object> values, String namePrefix, String nameSuffix) {
    PartitionSpec spec = table.spec();
    Schema schema = table.schema();
    List<String> filters = new ArrayList<>();
    for (PartitionField pf : spec.fields()) {
      Types.NestedField f = schema.findField(pf.sourceId());
      Type resultType = getResultTypeForPartitionTransformValues(pf, f);
      String colName = namePrefix + f.name() + nameSuffix;
      if (values.containsKey(colName)) {
        Object value = values.get(colName);
        if (value == null) {
          filters.add(String.format("%s IS NULL", colName));
        } else {
          String stringValue =
              schemaEvolution.getPrimitiveTypeValueForSql(resultType.asPrimitiveType(), value);
          filters.add(String.format("%s = %s", colName, stringValue));
        }
      }
    }

    return filters;
  }

  /**
   * Gets result types for partition columns of a table.
   *
   * @param table The Iceberg table
   * @return List of pairs of column names and their result types
   */
  private List<Pair<String, Type>> getResultTypesForPartitionColumns(Table table) {
    Schema schema = table.schema();
    List<Pair<String, Type>> resultTypes = new ArrayList<>();
    for (PartitionField pf : table.spec().fields()) {
      Types.NestedField f = schema.findField(pf.sourceId());
      resultTypes.add(Pair.of(f.name(), getResultTypeForPartitionTransformValues(pf, f)));
    }
    return resultTypes;
  }

  /**
   * Determines the result type for partition transform values.
   *
   * @param partitionField The partition field
   * @param sourceField The source field
   * @return The result type
   */
  private Type getResultTypeForPartitionTransformValues(
      PartitionField partitionField, Types.NestedField sourceField) {
    Type resultType = partitionField.transform().getResultType(sourceField.type());
    if (resultType.typeId() == Type.TypeID.DATE) {
      resultType = Types.IntegerType.get();
    } else if (resultType.typeId() == Type.TypeID.TIME
        || resultType.typeId() == Type.TypeID.TIMESTAMP) {
      resultType = Types.LongType.get();
    }

    return resultType;
  }

  /**
   * Uploads data files for a given partition key.
   *
   * @param table The Iceberg table
   * @param sql SQL query to execute
   * @param stmt Statement object for executing SQL
   * @param partitionKey StructLike object representing the partition key
   * @return List of uploaded DataFile objects
   */
  private List<DataFile> uploadDataFilesForPartitionKey(
      Table table, String sql, Statement stmt, StructLike partitionKey) {
    List<String> localFilePaths =
        executeSqlAndCreateLocalDataFiles(
            stmt,
            sql,
            tmpDir,
            schema,
            sortOrder,
            targetFileSizeBytes,
            null,
            false,
            rowGroupSize,
            null);
    List<DataFile> dataFileDetails =
        localFilePaths.stream()
            .map(
                localFilePath -> {
                  String newDataFilePath =
                      getNewFilePath(table, partitionKey, getUniqueParquetFileName());
                  return new DataFile(partitionKey, localFilePath, newDataFilePath);
                })
            .collect(Collectors.toList());

    List<DataFile> dataFiles = this.prepareNewDataFiles(table, dataFileDetails, sortOrder);
    fileIO.uploadFiles(dataFiles);
    return dataFiles;
  }

  /**
   * Creates a StructLike object for transformed partition values.
   *
   * @param table The Iceberg table
   * @param partitionTransformValues Map of transformed partition values
   * @param transformValueLookupMap Lookup map for transform values
   * @param prefix Prefix to be added to column names
   * @param suffix Suffix to be added to column names
   * @return StructLike object representing transformed partition values
   */
  private StructLike getPartitionValuesStructForTransformedValue(
      Table table,
      Map<String, Object> partitionTransformValues,
      Map<String, String> transformValueLookupMap,
      String prefix,
      String suffix) {
    if (partitionTransformValues == null) return null;

    Schema schema = table.schema();
    PartitionSpec spec = table.spec();
    int size = spec.fields().size();
    Object[] values = new Object[size];
    for (int i = 0; i < size; i += 1) {
      PartitionField pf = spec.fields().get(i);
      Types.NestedField field = schema.findField(pf.sourceId());
      if (field == null)
        throw new ValidationException(
            "Could not find the partition column in the schema for %s", pf.name());

      String name = prefix + field.name() + suffix;
      if (!partitionTransformValues.containsKey(name))
        throw new ValidationException(
            "Could not find the value for the partition column %s", field.name());

      Type resultType = getResultTypeForPartitionTransformValues(pf, field);
      if (resultType.typeId() == Type.TypeID.STRING) {
        String value = (String) partitionTransformValues.get(name);
        if (transformValueLookupMap != null && transformValueLookupMap.containsKey(value)) {
          value = transformValueLookupMap.get(value);
        }
        values[i] = decodePartitionValue(value);
      } else {
        values[i] = partitionTransformValues.get(name);
      }
    }
    return new ArrayStructLike(values);
  }

  private static final class PartitionData {
    private final Pair<Map<String, Object>, StructLike> singlePartitionData;
    private final String partitionDataFile;
    private final Set<String> singleBucketColumns;
    private final Map<String, String> transformValueLookupMap;

    private PartitionData(
        Pair<Map<String, Object>, StructLike> singlePartitionData,
        String partitionDataFile,
        Set<String> singleBucketColumns,
        Map<String, String> transformValueLookupMap) {
      this.singlePartitionData = singlePartitionData;
      this.partitionDataFile = partitionDataFile;
      this.singleBucketColumns = singleBucketColumns;
      this.transformValueLookupMap = transformValueLookupMap;
    }
  }
}
