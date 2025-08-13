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
package com.arcesium.swiftlake;

import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.io.SwiftLakeHadoopFileIO;
import com.arcesium.swiftlake.sql.TableScanResult;
import com.nimbusds.jose.util.Pair;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.duckdb.DuckDBArray;
import org.duckdb.DuckDBStruct;

public class TestUtil {
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();
  private static volatile SwiftLakeEngine swiftLakeEngine;

  public static SwiftLakeEngine getSwiftLakeEngine() {
    if (swiftLakeEngine == null) {
      synchronized (TestUtil.class) {
        if (swiftLakeEngine == null) {
          String basePath = System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID();
          swiftLakeEngine = getSwiftLakeEngineBuilder(basePath, 50, 2).build();
        }
      }
    }
    return swiftLakeEngine;
  }

  public static SwiftLakeEngine createSwiftLakeEngine(String tablesBasePath) {
    return getSwiftLakeEngineBuilder(tablesBasePath, 50, 2).build();
  }

  public static SwiftLakeEngine.Builder getSwiftLakeEngineBuilder(
      String tablesBasePath, int memoryLimitInMiB, int threads) {
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.CATALOG_IMPL, HadoopCatalog.class.getName());
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, tablesBasePath);
    properties.put(CatalogProperties.FILE_IO_IMPL, SwiftLakeHadoopFileIO.class.getName());
    return SwiftLakeEngine.builderFor("test")
        .memoryLimitInMiB(memoryLimitInMiB)
        .threads(threads)
        .allowFullTableScan(true)
        .catalog(CatalogUtil.buildIcebergCatalog("catalog", properties, new Configuration()));
  }

  public static void dropIcebergTable(SwiftLakeEngine swiftLakeEngine, String table) {
    dropIcebergTables(swiftLakeEngine, Arrays.asList(table));
  }

  public static void dropIcebergTables(SwiftLakeEngine swiftLakeEngine, List<String> tables) {
    if (tables == null) return;
    tables.forEach(
        table -> swiftLakeEngine.getCatalog().dropTable(TableIdentifier.parse(table), true));
  }

  public static List<Map<String, Object>> getRecordsFromTable(
      SwiftLakeEngine swiftLakeEngine, String tableName) {
    return executeQuery(swiftLakeEngine, "SELECT * FROM " + tableName);
  }

  public static List<Map<String, Object>> executeQuery(
      SwiftLakeEngine swiftLakeEngine, String sql) {
    return executeQuery(swiftLakeEngine, null, sql, null);
  }

  public static List<Map<String, Object>> executeQuery(
      SwiftLakeEngine swiftLakeEngine, String preSql, String sql, String postSql) {
    try (Connection connection = swiftLakeEngine.createConnection();
        Statement statement = connection.createStatement(); ) {
      if (preSql != null) {
        statement.executeUpdate(preSql);
      }
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        return getRecords(resultSet);
      } finally {
        if (postSql != null) {
          statement.executeUpdate(postSql);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<Map<String, Object>> getRecords(ResultSet resultSet) {
    List<Map<String, Object>> data = new ArrayList<>();
    try {
      while (resultSet.next()) {
        Map<String, Object> record = new HashMap<>();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
          String name = resultSetMetaData.getColumnName(i);
          Object value = resultSet.getObject(name);
          if (value instanceof Timestamp timestamp) {
            value = timestamp.toLocalDateTime();
          } else if (value instanceof Date date) {
            value = date.toLocalDate();
          }
          record.put(name, convertValue(value));
        }
        data.add(record);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return data;
  }

  private static Object convertValue(Object value) {
    if (value == null) {
      return null;
    }
    try {
      if (value instanceof DuckDBArray duckDBArray) {
        return Arrays.asList((Object[]) duckDBArray.getArray()).stream()
            .map(v -> convertValue(v))
            .collect(Collectors.toList());
      } else if (value instanceof DuckDBStruct duckDBStruct) {
        return duckDBStruct.getMap().entrySet().stream()
            .map(e -> Pair.of(e.getKey(), convertValue(e.getValue())))
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
      } else if (value instanceof Map map) {
        Map<Object, Object> newMap = new HashMap<>();
        for (var key : map.keySet()) {
          newMap.put(key, convertValue(map.get(key)));
        }
        return newMap;
      } else {
        return value;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getTableNameForBranch(TableIdentifier tableId, String branchName) {
    return String.format("%s.\"%s$branch_%s\"", tableId.namespace(), tableId.name(), branchName);
  }

  public static String getTableNameForTag(TableIdentifier tableId, String tagName) {
    return String.format("%s.\"%s$tag_%s\"", tableId.namespace(), tableId.name(), tagName);
  }

  public static String getTableNameForTimestamp(TableIdentifier tableId, LocalDateTime timestamp) {
    return String.format("%s.\"%s$timestamp_%s\"", tableId.namespace(), tableId.name(), timestamp);
  }

  public static String getTableNameForSnapshot(TableIdentifier tableId, long snapshotId) {
    return String.format("%s.\"%s$snapshot_%s\"", tableId.namespace(), tableId.name(), snapshotId);
  }

  public static String createSelectSql(List<Map<String, Object>> data, Schema schema) {
    return createSelectSql(data, schema.asStruct().fields());
  }

  public static String createSelectSql(
      List<Map<String, Object>> data, List<Types.NestedField> fields) {
    StringBuilder sql = new StringBuilder("SELECT * FROM (VALUES ");
    if (data == null || data.isEmpty()) {
      Map<String, Object> row = new HashMap<>();
      for (var field : fields) {
        row.put(field.name(), null);
      }
      sql.append(createValueString(row, fields));
    } else {
      for (int i = 0; i < data.size(); i++) {
        if (i > 0) sql.append(", ");
        sql.append(createValueString(data.get(i), fields));
      }
    }
    sql.append(") AS t");
    sql.append(getColumnProjection(fields));
    if (data == null || data.isEmpty()) {
      sql.append(" WHERE FALSE");
    }
    return sql.toString();
  }

  public static String createChangeSql(List<Map<String, Object>> data, Schema schema) {
    return createChangeSql(data, schema, "operation_type");
  }

  public static String createChangeSql(
      List<Map<String, Object>> data, Schema schema, String operationTypeColumn) {
    List<Types.NestedField> fields = new ArrayList<>(schema.asStruct().fields());
    if (operationTypeColumn != null) {
      fields.add(Types.NestedField.required(1000000, operationTypeColumn, Types.StringType.get()));
    }
    return createSelectSql(data, fields);
  }

  public static String createSnapshotSql(List<Map<String, Object>> data, Schema schema) {
    List<Types.NestedField> fields =
        new ArrayList<>(
            schema.asStruct().fields().stream()
                .filter(
                    f ->
                        !f.name().equalsIgnoreCase("effective_start")
                            && !f.name().equalsIgnoreCase("effective_end"))
                .collect(Collectors.toList()));

    return createSelectSql(data, fields);
  }

  private static String getColumnProjection(List<Types.NestedField> fields) {
    return fields.stream()
        .map(f -> escapeColumnName(f.name()))
        .collect(Collectors.joining(",", "(", ")"));
  }

  public static String escapeColumnName(String columnName) {
    ValidationException.checkNotNull(columnName, "Column name cannot be null");
    // Replace any double quotes with escaped double quotes
    String escaped = columnName.replace("\"", "\"\"");
    // Wrap with double quotes
    return "\"" + escaped + "\"";
  }

  private static String createValueString(Map<String, Object> row, List<Types.NestedField> fields) {
    StringBuilder valueStr = new StringBuilder("(");
    int i = 0;
    for (var field : fields) {
      Object value = row.get(field.name());
      if (i++ > 0) valueStr.append(", ");
      valueStr.append(createValueString(value, field));
    }
    valueStr.append(")");
    return valueStr.toString();
  }

  private static String createValueString(Object value, Types.NestedField field) {
    StringBuilder valueStr = new StringBuilder();
    if (value == null) {
      valueStr.append("NULL");
    } else if (value instanceof String) {
      valueStr.append("'").append(value.toString().replace("'", "''")).append("'");
    } else if (value instanceof LocalDate) {
      valueStr.append("DATE '").append(value).append("'");
    } else if (value instanceof LocalDateTime ldt) {
      valueStr
          .append("TIMESTAMP '")
          .append(ldt.format(DateTimeFormatter.ISO_DATE_TIME))
          .append("'");
    } else if (value instanceof Float floatValue) {
      if (Float.isNaN(floatValue)) {
        valueStr.append("'nan'::FLOAT");
      } else if (Float.isInfinite(floatValue)) {
        if (floatValue > 0) {
          valueStr.append("'inf'::FLOAT");
        } else {
          valueStr.append("'-inf'::FLOAT");
        }
      } else {
        return Float.toString(floatValue);
      }
    } else if (value instanceof Double doubleValue) {
      if (Double.isNaN(doubleValue)) {
        valueStr.append("'nan'::DOUBLE");
      } else if (Double.isInfinite(doubleValue)) {
        if (doubleValue > 0) {
          valueStr.append("'inf'::DOUBLE");
        } else {
          valueStr.append("'-inf'::DOUBLE");
        }
      } else {
        return Double.toString(doubleValue);
      }
    } else if (value instanceof BigDecimal) {
      valueStr.append(((BigDecimal) value).toPlainString());
    } else if (value instanceof OffsetDateTime odt) {
      valueStr
          .append("TIMESTAMP WITH TIME ZONE '")
          .append(odt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
          .append("'");
    } else if (value instanceof LocalTime) {
      valueStr.append("TIME '").append(value).append("'");
    } else if (value instanceof Map map && field.type().isStructType()) {
      valueStr.append("{");
      int i = 0;
      for (var nestedField : field.type().asStructType().fields()) {
        Object nestedValue = map.get(nestedField.name());
        if (i++ > 0) valueStr.append(", ");
        valueStr.append("'").append(nestedField.name()).append("':");
        valueStr.append(createValueString(nestedValue, nestedField));
      }
      valueStr.append("}");
    } else if (value instanceof Map && field.type().isMapType()) {
      valueStr.append("MAP{").append(createMapString((Map<?, ?>) value)).append("}");
    } else if (value instanceof List) {
      valueStr.append("ARRAY[").append(createListString((List<?>) value)).append("]");
    } else {
      valueStr.append(value);
    }
    return valueStr.toString();
  }

  private static String createMapString(Map<?, ?> map) {
    StringBuilder mapStr = new StringBuilder();
    int i = 0;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (i++ > 0) mapStr.append(", ");
      mapStr.append("'").append(entry.getKey()).append("': ").append(entry.getValue());
    }
    return mapStr.toString();
  }

  private static String createListString(List<?> list) {
    StringBuilder listStr = new StringBuilder();
    for (int i = 0; i < list.size(); i++) {
      if (i > 0) listStr.append(", ");
      listStr.append("'").append(list.get(i)).append("'");
    }
    return listStr.toString();
  }

  public static void compressGzipFile(String sourceFile, String gzipFile) {
    try (FileInputStream fis = new FileInputStream(sourceFile);
        FileOutputStream fos = new FileOutputStream(gzipFile);
        GZIPOutputStream gzipOS = new GZIPOutputStream(fos)) {
      byte[] buffer = new byte[1024];
      int len;
      while ((len = fis.read(buffer)) > 0) {
        gzipOS.write(buffer, 0, len);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Schema createComplexSchema() {
    int id = 0;
    return new Schema(
        Types.NestedField.optional(++id, "int_1", Types.IntegerType.get()),
        Types.NestedField.optional(++id, "float_1", Types.FloatType.get()),
        Types.NestedField.optional(++id, "string_1", Types.StringType.get()),
        Types.NestedField.optional(
            ++id,
            "struct_1",
            Types.StructType.of(
                Arrays.asList(
                    Types.NestedField.optional(++id, "struct_1_string_1", Types.StringType.get()),
                    Types.NestedField.optional(
                        ++id, "struct_1_decimal_1", Types.DecimalType.of(20, 14)),
                    Types.NestedField.optional(
                        ++id,
                        "struct_1_struct_1",
                        Types.StructType.of(
                            Arrays.asList(
                                Types.NestedField.optional(
                                    ++id, "struct_1_struct_1_date_1", Types.DateType.get()))))))),
        Types.NestedField.optional(
            ++id,
            "list_1",
            Types.ListType.ofOptional(
                ++id,
                Types.StructType.of(
                    Arrays.asList(
                        Types.NestedField.optional(++id, "list_1_string_1", Types.StringType.get()),
                        Types.NestedField.optional(++id, "list_1_double_1", Types.DoubleType.get()),
                        Types.NestedField.optional(
                            ++id,
                            "list_1_struct_1",
                            Types.StructType.of(
                                Arrays.asList(
                                    Types.NestedField.optional(
                                        ++id,
                                        "list_1_struct_1_date_1",
                                        Types.DateType.get())))))))),
        Types.NestedField.optional(
            ++id,
            "list_2",
            Types.ListType.ofOptional(
                ++id, Types.ListType.ofOptional(++id, Types.IntegerType.get()))),
        Types.NestedField.optional(
            ++id,
            "map_1",
            Types.MapType.ofOptional(
                ++id,
                ++id,
                Types.StringType.get(),
                Types.StructType.of(
                    Arrays.asList(
                        Types.NestedField.optional(++id, "map_1_string_1", Types.StringType.get()),
                        Types.NestedField.optional(++id, "map_1_double_1", Types.FloatType.get()),
                        Types.NestedField.optional(
                            ++id,
                            "map_1_struct_1",
                            Types.StructType.of(
                                Arrays.asList(
                                    Types.NestedField.optional(
                                        ++id,
                                        "map_1_struct_1_date_1",
                                        Types.DateType.get())))))))));
  }

  /** Read records from a table in the exact order they appear in the Parquet files */
  public static List<Map<String, Object>> getRecordsFromTableInFileOrder(
      SwiftLakeEngine swiftLakeEngine, String tableName) {
    List<Map<String, Object>> records = new ArrayList<>();
    Table table = swiftLakeEngine.getTable(tableName);

    TableScanResult scanResult =
        swiftLakeEngine
            .getIcebergScanExecutor()
            .executeTableScan(table, Expressions.alwaysTrue(), true, true, null, null, null, false);

    try (Connection conn = swiftLakeEngine.createConnection(false);
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                "SELECT * EXCLUDE(filename, file_row_number) FROM "
                    + scanResult.getSql()
                    + " ORDER BY filename, file_row_number")) {
      return getRecords(rs);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** Compare two lists of maps */
  public static boolean compareDataInOrder(
      List<Map<String, Object>> list1, List<Map<String, Object>> list2) {
    if (list1.size() != list2.size()) {
      return false;
    }

    for (int i = 0; i < list1.size(); i++) {
      Map<String, Object> map1 = new HashMap<>(list1.get(i));
      Map<String, Object> map2 = new HashMap<>(list2.get(i));

      if (!map1.equals(map2)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Parses a timestamp string in "yyyy-MM-dd HH:mm:ss" format to a LocalDateTime object.
   *
   * @param timestamp The timestamp string to parse
   * @return A LocalDateTime object representing the parsed timestamp
   */
  public static LocalDateTime parseTimestamp(String timestamp) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    return LocalDateTime.parse(timestamp, formatter);
  }
}
