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
package com.arcesium.swiftlake.common;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.mybatis.type.SwiftLakeDuckDBTime;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

/**
 * Utility class for importing data into DuckDB and files. Provides methods to write data from
 * various sources to DuckDB tables and Parquet files.
 */
public class DataImportUtil {
  private static final int BATCH_SIZE = 1000;

  /**
   * Writes data from a ResultSet to a Parquet file.
   *
   * @param resultSet The ResultSet containing the data to write.
   * @param filePath The path where the Parquet file will be written.
   */
  public static void writeResultSetToParquetFile(ResultSet resultSet, String filePath) {
    writeResultSetToParquetFile(resultSet, filePath, null);
  }

  /**
   * Writes data from a ResultSet to a Parquet file.
   *
   * @param resultSet The ResultSet containing the data to write.
   * @param filePath The path where the Parquet file will be written.
   * @param properties Additional properties for the Parquet writer.
   */
  public static void writeResultSetToParquetFile(
      ResultSet resultSet, String filePath, Map<String, String> properties) {
    try {
      Schema schema = createSchema(resultSet);
      DataWriter<GenericRecord> dataWriter = null;
      GenericRecord record = GenericRecord.create(schema);
      int recordCount = 0;
      try {
        while (resultSet.next()) {
          Map<String, Object> valuesMap = new HashMap<>();
          for (org.apache.iceberg.types.Types.NestedField field : schema.asStruct().fields()) {
            valuesMap.put(
                field.name(), convertToParquetType(getObjectFromResultSet(resultSet, field)));
          }
          if (recordCount == 0) {
            dataWriter = getDataWriter(schema, filePath, properties);
          }
          dataWriter.write(record.copy(valuesMap));
          recordCount++;
        }
      } finally {
        if (dataWriter != null) {
          dataWriter.close();
        }
      }

      if (recordCount == 0) {
        createEmptyParquetFile(filePath, schema);
      }
    } catch (IOException | SQLException ex) {
      throw new SwiftLakeException(
          ex, "An error occurred while writing ResultSet to parquet file.");
    }
  }

  /**
   * Writes data from a List of Maps to a Parquet file.
   *
   * @param columns List of column names and types.
   * @param data List of Maps containing the data to write.
   * @param filePath The path where the Parquet file will be written.
   */
  public static void writeMapsToParquetFile(
      List<Pair<String, Type>> columns, List<Map<String, Object>> data, String filePath) {
    writeMapsToParquetFile(columns, data, filePath, null);
  }

  /**
   * Writes data from a List of Maps to a Parquet file.
   *
   * @param columns List of column names and types.
   * @param data List of Maps containing the data to write.
   * @param filePath The path where the Parquet file will be written.
   * @param properties Additional properties for the Parquet writer.
   */
  public static void writeMapsToParquetFile(
      List<Pair<String, Type>> columns,
      List<Map<String, Object>> data,
      String filePath,
      Map<String, String> properties) {
    try {
      Schema schema = createSchemaForColumns(columns);
      if (data.isEmpty()) {
        createEmptyParquetFile(filePath, schema);
        return;
      }
      DataWriter<GenericRecord> dataWriter = getDataWriter(schema, filePath, properties);
      GenericRecord record = GenericRecord.create(schema);
      try {
        for (Map<String, Object> valuesMap : data) {
          dataWriter.write(record.copy(convertToParquetType(valuesMap)));
        }
      } finally {
        dataWriter.close();
      }
    } catch (IOException ex) {
      throw new SwiftLakeException(ex, "An error occurred while writing to parquet file.");
    }
  }

  /**
   * Writes data from a List of objects to a Parquet file using DataColumnAccessors.
   *
   * @param <T> The type of objects in the data list.
   * @param columnAccessors List of DataColumnAccessors for accessing data fields.
   * @param data List of objects containing the data to write.
   * @param filePath The path where the Parquet file will be written.
   */
  public static <T> void writeDataToParquetFile(
      List<DataColumnAccessor> columnAccessors, List<T> data, String filePath) {
    writeDataToParquetFile(columnAccessors, data, filePath, null);
  }

  /**
   * Writes data from a List of objects to a Parquet file using DataColumnAccessors.
   *
   * @param <T> The type of objects in the data list.
   * @param columnAccessors List of DataColumnAccessors for accessing data fields.
   * @param data List of objects containing the data to write.
   * @param filePath The path where the Parquet file will be written.
   * @param properties Additional properties for the Parquet writer.
   */
  public static <T> void writeDataToParquetFile(
      List<DataColumnAccessor> columnAccessors,
      List<T> data,
      String filePath,
      Map<String, String> properties) {
    try {
      Schema schema = createSchema(columnAccessors);
      if (data.isEmpty()) {
        createEmptyParquetFile(filePath, schema);
        return;
      }
      DataWriter<GenericRecord> dataWriter = getDataWriter(schema, filePath, properties);
      GenericRecord record = GenericRecord.create(schema);
      try {
        for (T dataRecord : data) {
          Map<String, Object> valuesMap = new HashMap<>();
          for (DataColumnAccessor columnMeta : columnAccessors) {
            valuesMap.put(columnMeta.getName(), columnMeta.getValue(dataRecord));
          }

          dataWriter.write(record.copy(valuesMap));
        }
      } finally {
        dataWriter.close();
      }
    } catch (IOException ex) {
      throw new SwiftLakeException(ex, "An error occurred while writing to parquet file.");
    }
  }

  /**
   * Creates a DuckDB table based on the structure of a ResultSet.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param tableName The name of the table to be created
   * @param resultSet The ResultSet containing the table structure
   * @param temporaryTable If true, creates a temporary table
   * @param replaceExistingTable If true, replaces an existing table with the same name
   */
  public static void createDuckDBTable(
      SwiftLakeEngine swiftLakeEngine,
      String tableName,
      ResultSet resultSet,
      boolean temporaryTable,
      boolean replaceExistingTable) {
    createDuckDBTable(
        swiftLakeEngine,
        tableName,
        createSchema(resultSet).columns().stream()
            .map(e -> Pair.of(e.name(), e.type()))
            .collect(Collectors.toList()),
        temporaryTable,
        replaceExistingTable);
  }

  /**
   * Creates a DuckDB table based on the structure of a ResultSet using a provided Connection.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param tableName The name of the table to be created
   * @param resultSet The ResultSet containing the table structure
   * @param temporaryTable If true, creates a temporary table
   * @param replaceExistingTable If true, replaces an existing table with the same name
   * @param connection The Connection to use for creating the table
   */
  public static void createDuckDBTable(
      SwiftLakeEngine swiftLakeEngine,
      String tableName,
      ResultSet resultSet,
      boolean temporaryTable,
      boolean replaceExistingTable,
      Connection connection) {
    createDuckDBTable(
        swiftLakeEngine,
        tableName,
        createSchema(resultSet).columns().stream()
            .map(e -> Pair.of(e.name(), e.type()))
            .collect(Collectors.toList()),
        temporaryTable,
        replaceExistingTable,
        connection);
  }

  /**
   * Creates a DuckDB table based on a list of DataColumnAccessors.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param tableName The name of the table to be created
   * @param mappers List of DataColumnAccessors defining the table structure
   * @param temporaryTable If true, creates a temporary table
   * @param replaceExistingTable If true, replaces an existing table with the same name
   */
  public static void createDuckDBTableForColumnMappers(
      SwiftLakeEngine swiftLakeEngine,
      String tableName,
      List<DataColumnAccessor> mappers,
      boolean temporaryTable,
      boolean replaceExistingTable) {
    createDuckDBTable(
        swiftLakeEngine,
        tableName,
        mappers.stream().map(e -> Pair.of(e.getName(), e.getType())).collect(Collectors.toList()),
        temporaryTable,
        replaceExistingTable);
  }

  /**
   * Creates a DuckDB table based on a list of column name and type pairs.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param tableName The name of the table to be created
   * @param columns List of Pairs containing column names and types
   * @param temporaryTable If true, creates a temporary table
   * @param replaceExistingTable If true, replaces an existing table with the same name
   * @throws SwiftLakeException if an error occurs while creating the table
   */
  public static void createDuckDBTable(
      SwiftLakeEngine swiftLakeEngine,
      String tableName,
      List<Pair<String, Type>> columns,
      boolean temporaryTable,
      boolean replaceExistingTable) {
    try (Connection connection = swiftLakeEngine.createDuckDBConnection()) {
      createDuckDBTable(
          swiftLakeEngine, tableName, columns, temporaryTable, replaceExistingTable, connection);
    } catch (SQLException ex) {
      throw new SwiftLakeException(ex, "An error occurred while creating table.");
    }
  }

  /**
   * Creates a DuckDB table based on a list of column name and type pairs using a provided
   * Connection.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance
   * @param tableName The name of the table to be created
   * @param columns List of Pairs containing column names and types
   * @param temporaryTable If true, creates a temporary table
   * @param replaceExistingTable If true, replaces an existing table with the same name
   * @param connection The Connection to use for creating the table
   * @throws SwiftLakeException if an error occurs while creating the table
   * @throws ValidationException if a nested type is encountered in the columns list
   */
  public static void createDuckDBTable(
      SwiftLakeEngine swiftLakeEngine,
      String tableName,
      List<Pair<String, Type>> columns,
      boolean temporaryTable,
      boolean replaceExistingTable,
      Connection connection) {
    String columnsString =
        columns.stream()
            .map(
                e -> {
                  if (e.getRight().isNestedType()) {
                    throw new ValidationException("Nested type is not supported");
                  }
                  return e.getLeft()
                      + " "
                      + swiftLakeEngine.getSchemaEvolution().getTypeExpr(e.getRight());
                })
            .collect(Collectors.joining(","));

    try (Statement stmt = connection.createStatement()) {
      stmt.executeUpdate(
          String.format(
              "CREATE %s %s TABLE %s (%s);",
              replaceExistingTable ? "OR REPLACE" : "",
              temporaryTable ? "TEMP" : "",
              tableName,
              columnsString));
    } catch (SQLException ex) {
      throw new SwiftLakeException(ex, "An error occurred while creating table.");
    }
  }

  /**
   * Writes data from a ResultSet to a DuckDB table.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param resultSet The ResultSet containing the data to write.
   * @param tableName The name of the table to write to.
   */
  public static void writeResultSetToDuckDBTable(
      SwiftLakeEngine swiftLakeEngine, ResultSet resultSet, String tableName) {
    try (Connection connection = swiftLakeEngine.createDuckDBConnection(); ) {
      writeResultSetToDuckDBTable(resultSet, tableName, connection);
    } catch (SQLException ex) {
      throw new SwiftLakeException(ex, "An error occurred while writing ResultSet to table.");
    }
  }

  /**
   * Writes data from a ResultSet to a DuckDB table using a provided connection.
   *
   * @param resultSet The ResultSet containing the data to write.
   * @param tableName The name of the table to write to.
   * @param connection The DuckDB connection to use.
   */
  public static void writeResultSetToDuckDBTable(
      ResultSet resultSet, String tableName, Connection connection) {
    Schema schema = createSchema(resultSet);
    validateSchema(schema);
    List<org.apache.iceberg.types.Types.NestedField> fields = schema.asStruct().fields();
    List<String> columns = fields.stream().map(f -> f.name()).collect(Collectors.toList());

    try (PreparedStatement stmt =
        connection.prepareStatement(getInsertIntoSql(tableName, columns))) {
      int batchCount = 0;
      while (resultSet.next()) {
        for (int i = 0; i < fields.size(); i++) {
          stmt.setObject(i + 1, convertToDBType(getObjectFromResultSet(resultSet, fields.get(i))));
        }
        stmt.addBatch();
        batchCount++;
        if (batchCount >= BATCH_SIZE) {
          stmt.executeBatch();
          batchCount = 0;
        }
      }
      if (batchCount > 0) {
        stmt.executeBatch();
      }
    } catch (SQLException ex) {
      throw new SwiftLakeException(ex, "An error occurred while writing ResultSet to table.");
    }
  }

  /**
   * Writes data from a List of Maps to a DuckDB table.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param columns List of column names and types.
   * @param data List of Maps containing the data to write.
   * @param tableName The name of the table to write to.
   */
  public static void writeMapsToDuckDBTable(
      SwiftLakeEngine swiftLakeEngine,
      List<Pair<String, Type>> columns,
      List<Map<String, Object>> data,
      String tableName) {
    try (Connection connection = swiftLakeEngine.createDuckDBConnection(); ) {
      writeMapsToDuckDBTable(columns, data, tableName, connection);
    } catch (SQLException ex) {
      throw new SwiftLakeException(ex, "An error occurred while writing to table.");
    }
  }

  /**
   * Writes data from a List of Maps to a DuckDB table using a provided connection.
   *
   * @param columns List of column names and types.
   * @param data List of Maps containing the data to write.
   * @param tableName The name of the table to write to.
   * @param connection The DuckDB connection to use.
   */
  public static void writeMapsToDuckDBTable(
      List<Pair<String, Type>> columns,
      List<Map<String, Object>> data,
      String tableName,
      Connection connection) {
    if (data.isEmpty()) {
      return;
    }
    Schema schema = createSchemaForColumns(columns);
    validateSchema(schema);
    List<String> columnNames =
        schema.asStruct().fields().stream().map(f -> f.name()).collect(Collectors.toList());

    try (PreparedStatement stmt =
        connection.prepareStatement(getInsertIntoSql(tableName, columnNames))) {
      int c = 0;
      for (Map<String, Object> valuesMap : data) {
        for (int i = 0; i < columnNames.size(); i++) {
          stmt.setObject(i + 1, convertToDBType(valuesMap.get(columnNames.get(i))));
        }
        stmt.addBatch();
        c++;
        if (c % BATCH_SIZE == 0) {
          stmt.executeBatch();
        }
      }
      stmt.executeBatch();
    } catch (SQLException ex) {
      throw new SwiftLakeException(ex, "An error occurred while writing to table.");
    }
  }

  /**
   * Writes data from a List of objects to a DuckDB table using DataColumnAccessors.
   *
   * @param <T> The type of objects in the data list.
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param columnAccessors List of DataColumnAccessors for accessing data fields.
   * @param data List of objects containing the data to write.
   * @param tableName The name of the table to write to.
   */
  public static <T> void writeDataToDuckDBTable(
      SwiftLakeEngine swiftLakeEngine,
      List<DataColumnAccessor> columnAccessors,
      List<T> data,
      String tableName) {
    try (Connection connection = swiftLakeEngine.createDuckDBConnection(); ) {
      writeDataToDuckDBTable(columnAccessors, data, tableName, connection);
    } catch (SQLException ex) {
      throw new SwiftLakeException(ex, "An error occurred while writing to table.");
    }
  }

  /**
   * Writes data from a List of objects to a DuckDB table using DataColumnAccessors and a provided
   * connection.
   *
   * @param <T> The type of objects in the data list.
   * @param columnAccessors List of DataColumnAccessors for accessing data fields.
   * @param data List of objects containing the data to write.
   * @param tableName The name of the table to write to.
   * @param connection The DuckDB connection to use.
   */
  public static <T> void writeDataToDuckDBTable(
      List<DataColumnAccessor> columnAccessors,
      List<T> data,
      String tableName,
      Connection connection) {
    if (data.isEmpty()) {
      return;
    }
    Schema schema = createSchema(columnAccessors);
    validateSchema(schema);
    List<String> columnNames =
        schema.asStruct().fields().stream().map(f -> f.name()).collect(Collectors.toList());

    try (PreparedStatement stmt =
        connection.prepareStatement(getInsertIntoSql(tableName, columnNames))) {
      int c = 0;
      for (T dataRecord : data) {
        for (int i = 0; i < columnAccessors.size(); i++) {
          stmt.setObject(i + 1, convertToDBType(columnAccessors.get(i).getValue(dataRecord)));
        }
        stmt.addBatch();
        c++;
        if (c % BATCH_SIZE == 0) {
          stmt.executeBatch();
        }
      }
      stmt.executeBatch();
    } catch (SQLException ex) {
      throw new SwiftLakeException(ex, "An error occurred while writing to table.");
    }
  }

  private static String getInsertIntoSql(String tableName, List<String> columns) {
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO ");
    sql.append(tableName);
    sql.append("(");
    sql.append(columns.stream().collect(Collectors.joining(",")));
    sql.append(") VALUES (");
    sql.append(columns.stream().map(c -> "?").collect(Collectors.joining(",")));
    sql.append(");");

    return sql.toString();
  }

  private static void validateSchema(Schema schema) {
    List<org.apache.iceberg.types.Types.NestedField> nestedFields =
        schema.asStruct().fields().stream()
            .filter(f -> f.type().isNestedType())
            .collect(Collectors.toList());
    if (!nestedFields.isEmpty()) {
      throw new ValidationException("Nested type is not supported");
    }
  }

  private static DataWriter<GenericRecord> getDataWriter(
      Schema schema, String filePath, Map<String, String> properties) throws IOException {
    OutputFile file = org.apache.iceberg.Files.localOutput(filePath);
    Parquet.DataWriteBuilder builder =
        Parquet.writeData(file)
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned());
    if (properties != null) {
      builder.setAll(properties);
    }
    return builder.build();
  }

  /**
   * Creates an empty Parquet file with the specified schema.
   *
   * @param filePath The path where the empty Parquet file will be created
   * @param schema The Iceberg schema to use for the Parquet file
   * @throws SwiftLakeException If an error occurs during file creation
   */
  private static void createEmptyParquetFile(String filePath, Schema schema) {
    ValidationException.check(
        filePath != null && !filePath.isEmpty(), "File path cannot be null or empty");
    ValidationException.check(schema != null, "Schema cannot be null");
    try {
      ParquetFileWriter writer =
          new ParquetFileWriter(
              HadoopOutputFile.fromPath(new Path(filePath), new Configuration()),
              ParquetSchemaUtil.convert(schema, "table"),
              ParquetFileWriter.Mode.OVERWRITE,
              128 * 1024 * 1024,
              0,
              64,
              Integer.MAX_VALUE,
              true);
      writer.start();
      writer.end(Map.of());
    } catch (IOException ex) {
      throw new SwiftLakeException(ex, "Failed to create empty Parquet file at path: %s", filePath);
    }
  }

  private static Schema createSchema(ResultSet resultSet) {
    try {
      int id = 0;
      List<org.apache.iceberg.types.Types.NestedField> fields = new ArrayList<>();
      if (resultSet != null) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
          String columnName = resultSetMetaData.getColumnName(i);
          int sqlColumnType = resultSetMetaData.getColumnType(i);
          fields.add(
              org.apache.iceberg.types.Types.NestedField.optional(
                  ++id,
                  columnName,
                  getIcebergType(
                      sqlColumnType,
                      resultSetMetaData.getPrecision(i),
                      resultSetMetaData.getScale(i))));
        }
      }
      return new Schema(fields);
    } catch (SQLException ex) {
      throw new SwiftLakeException(ex, "An error occurred while creating schema");
    }
  }

  private static Schema createSchema(List<DataColumnAccessor> columnMetadata) {
    int id = 0;
    List<org.apache.iceberg.types.Types.NestedField> fields = new ArrayList<>();
    for (DataColumnAccessor columnMeta : columnMetadata) {
      fields.add(
          org.apache.iceberg.types.Types.NestedField.optional(
              ++id, columnMeta.getName(), columnMeta.getType()));
    }
    return new Schema(fields);
  }

  private static Schema createSchemaForColumns(List<Pair<String, Type>> columnMetadata) {
    int id = 0;
    List<org.apache.iceberg.types.Types.NestedField> fields = new ArrayList<>();
    for (Pair<String, Type> columnMeta : columnMetadata) {
      fields.add(
          org.apache.iceberg.types.Types.NestedField.optional(
              ++id, columnMeta.getLeft(), columnMeta.getRight()));
    }
    return new Schema(fields);
  }

  private static Type getIcebergType(int sqlColumnType, int precision, int scale) {
    switch (sqlColumnType) {
      case Types.BIT:
      case Types.BOOLEAN:
        return org.apache.iceberg.types.Types.BooleanType.get();

      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        return org.apache.iceberg.types.Types.IntegerType.get();
      case Types.BIGINT:
        return org.apache.iceberg.types.Types.LongType.get();

      case Types.ROWID:
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.SQLXML:
        return org.apache.iceberg.types.Types.StringType.get();

      case Types.REAL:
        return org.apache.iceberg.types.Types.FloatType.get();

      case Types.DOUBLE:
      case Types.FLOAT:
        return org.apache.iceberg.types.Types.DoubleType.get();

      case Types.DECIMAL:
      case Types.NUMERIC:
        return org.apache.iceberg.types.Types.DecimalType.of(precision, scale);

      case Types.DATE:
        return org.apache.iceberg.types.Types.DateType.get();

      case Types.TIME:
        return org.apache.iceberg.types.Types.TimeType.get();

      case Types.TIMESTAMP:
        return org.apache.iceberg.types.Types.TimestampType.withoutZone();
    }

    throw new ValidationException("Unsupported column type %s", sqlColumnType);
  }

  private static Map<String, Object> convertToParquetType(Map<String, Object> map) {
    if (map == null) {
      return null;
    }
    Map<String, Object> updatedMap = new HashMap<>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      updatedMap.put(entry.getKey(), convertToParquetType(entry.getValue()));
    }
    return updatedMap;
  }

  private static Object convertToParquetType(Object obj) {
    if (obj instanceof java.sql.Time) {
      throw new ValidationException("Unsupported type java.sql.Time");
    }

    if (obj instanceof Short shortObj) {
      return shortObj.intValue();
    }
    if (obj instanceof java.sql.Date date) {
      return date.toLocalDate();
    }

    if (obj instanceof java.sql.Timestamp timestamp) {
      return timestamp.toLocalDateTime();
    }

    return obj;
  }

  private static Object getObjectFromResultSet(
      ResultSet resultSet, org.apache.iceberg.types.Types.NestedField field) throws SQLException {
    Object value = resultSet.getObject(field.name());
    if (field.type().typeId() == Type.TypeID.TIME
        && value != null
        && !(value instanceof LocalTime)) {
      value = resultSet.getObject(field.name(), LocalTime.class);
    }
    return value;
  }

  private static Object convertToDBType(Object obj) {
    if (obj instanceof Short shortObj) {
      return shortObj.intValue();
    }

    if (obj instanceof LocalDate localDate) {
      return java.sql.Date.valueOf(localDate);
    }

    if (obj instanceof LocalTime localTime) {
      return new SwiftLakeDuckDBTime(localTime);
    }

    return obj;
  }
}
