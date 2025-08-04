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
package com.arcesium.swiftlake.sql;

import com.arcesium.swiftlake.common.DateTimeUtil;
import com.arcesium.swiftlake.common.ParquetUtil;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.io.SwiftLakeFileIO;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.False;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.True;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.expressions.UnboundTransform;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;

/** Handles schema evolution for Iceberg tables when reading Parquet files. */
public class SchemaEvolution {
  // Constants for DuckDB SQL syntax and formatting
  private static final String DUCK_DB_TYPE_CAST_OPERATOR = "::";
  private static final String DUCK_DB_TYPE_KEY_VALUE_SEPARATOR = ":";
  private static final String DUCK_DB_TYPE_NESTED_FIELD_SEPARATOR = ".";
  private static final String DUCK_DB_ALL_COLUMNS_PROJECTION = "*";
  private static final String LOCAL_ITERATION_VAR_NAME = "__v%d__";
  private static final String DUCK_DB_UNION_ALL_BY_NAME = "UNION ALL BY NAME";
  private static final String NULL_VALUE_STRING = "NULL";

  /**
   * Generates SQL for selecting data from Parquet files.
   *
   * @param table The Iceberg table
   * @param dataFiles List of data files to read
   * @param includeFileName Whether to include file name in the result
   * @param includeFileRowNumber Whether to include file row number in the result
   * @return SQL statement for selecting data
   */
  public String getSelectSQLForDataFiles(
      Table table, List<String> dataFiles, boolean includeFileName, boolean includeFileRowNumber) {
    return getSelectSQLForDataFiles(
        table, dataFiles, null, true, includeFileName, includeFileRowNumber);
  }

  /**
   * Generates SQL for selecting data from local and remote Parquet files.
   *
   * @param table The Iceberg table
   * @param localFiles List of local data files
   * @param remoteFiles List of remote data files
   * @param includeFileName Whether to include file name in the result
   * @param includeFileRowNumber Whether to include file row number in the result
   * @return SQL statement for selecting data
   */
  public String getSelectSQLForDataFiles(
      Table table,
      List<String> localFiles,
      List<String> remoteFiles,
      boolean includeFileName,
      boolean includeFileRowNumber) {
    return getSelectSQLForDataFiles(
        table, localFiles, remoteFiles, true, includeFileName, includeFileRowNumber);
  }

  /**
   * Generates SQL for selecting data from local and remote Parquet files with additional options.
   *
   * @param table The Iceberg table
   * @param localFiles List of local data files
   * @param remoteFiles List of remote data files
   * @param handleEmptyFiles Whether to handle empty file lists
   * @param includeFileName Whether to include file name in the result
   * @param includeFileRowNumber Whether to include file row number in the result
   * @return SQL statement for selecting data
   */
  public String getSelectSQLForDataFiles(
      Table table,
      List<String> localFiles,
      List<String> remoteFiles,
      boolean handleEmptyFiles,
      boolean includeFileName,
      boolean includeFileRowNumber) {
    // Check if file lists are empty
    boolean emptyFileList =
        (localFiles == null || localFiles.isEmpty())
            && (remoteFiles == null || remoteFiles.isEmpty());
    if (!handleEmptyFiles && emptyFileList) {
      return null;
    }

    Schema schema = table.schema();
    Type tableSchemaType = createTypeForComparison(schema.asStruct());
    // Handle empty file list case
    if (emptyFileList) {
      // (SELECT <<projection>> WHERE FALSE)
      StringBuilder sql = new StringBuilder();
      sql.append("(SELECT ");
      StringBuilder projection =
          getProjectionExprAdjustedForSchemaEvolution(
              tableSchemaType.asStructType(), Types.StructType.of(new ArrayList<>()));
      if (projection == null) {
        sql.append(DUCK_DB_ALL_COLUMNS_PROJECTION);
      } else {
        sql.append(projection);
      }
      if (includeFileName) {
        sql.append(", NULL::STRING AS filename");
      }
      if (includeFileRowNumber) {
        sql.append(", NULL::LONG AS file_row_number");
      }
      sql.append(" WHERE FALSE)");
      return sql.toString();
    }

    Map<Type, List<String>> dataFilesWithDifferentSchema = new HashMap<>();
    List<String> dataFilesWithSameSchema = new ArrayList<>();
    SwiftLakeFileIO fileIO = (SwiftLakeFileIO) table.io();
    List<Pair<String, InputFile>> inputFiles = new ArrayList<>();
    // Collect local and remote input files
    if (localFiles != null && !localFiles.isEmpty()) {
      inputFiles.addAll(
          localFiles.stream()
              .map(path -> Pair.of(path, org.apache.iceberg.Files.localInput(path)))
              .collect(Collectors.toList()));
    }
    if (remoteFiles != null && !remoteFiles.isEmpty()) {
      inputFiles.addAll(
          remoteFiles.stream()
              .map(path -> Pair.of(path, fileIO.newInputFile(path, false)))
              .collect(Collectors.toList()));
    }

    // Process each input file
    for (Pair<String, InputFile> inputFilePair : inputFiles) {
      InputFile inputFile = inputFilePair.getRight();
      String dataFile = inputFilePair.getLeft();
      Schema fileSchema = null;
      try (ParquetFileReader reader = ParquetUtil.getParquetFileReader(inputFile)) {
        MessageType parquetTypeWithIds = reader.getFileMetaData().getSchema();
        fileSchema = ParquetSchemaUtil.convertAndPrune(parquetTypeWithIds);
      } catch (Exception e) {
        throw new SwiftLakeException(e, "Failed to read footer of file: %s", inputFile);
      }

      Type fileSchemaType = createTypeForComparison(fileSchema.asStruct());

      // Group files by schema
      if (!tableSchemaType.equals(fileSchemaType)) {
        List<String> similarFiles = dataFilesWithDifferentSchema.get(fileSchemaType);
        if (similarFiles == null) {
          similarFiles = new ArrayList<>();
          dataFilesWithDifferentSchema.put(fileSchemaType, similarFiles);
        }

        similarFiles.add(dataFile);
      } else {
        dataFilesWithSameSchema.add(dataFile);
      }
    }

    // Generate SQL statements
    StringBuilder sql = new StringBuilder();
    sql.append("(");
    boolean isFirst = true;
    if (!dataFilesWithSameSchema.isEmpty()) {
      appendSql(
          sql,
          DUCK_DB_ALL_COLUMNS_PROJECTION,
          dataFilesWithSameSchema,
          includeFileName,
          includeFileRowNumber);
      isFirst = false;
    }

    for (Map.Entry<Type, List<String>> entry : dataFilesWithDifferentSchema.entrySet()) {
      if (!isFirst) {
        sql.append("  ").append(DUCK_DB_UNION_ALL_BY_NAME).append("  ");
      }
      isFirst = false;
      StringBuilder projection =
          getProjectionExprAdjustedForSchemaEvolution(
              tableSchemaType.asStructType(), entry.getKey().asStructType());
      if (projection == null) {
        projection = new StringBuilder(DUCK_DB_ALL_COLUMNS_PROJECTION);
      } else {
        if (includeFileName) {
          projection.append(", filename");
        }
        if (includeFileRowNumber) {
          projection.append(", file_row_number");
        }
      }

      appendSql(sql, projection, entry.getValue(), includeFileName, includeFileRowNumber);
    }
    sql.append(")");
    return sql.toString();
  }

  private void appendSql(
      StringBuilder sql,
      CharSequence projection,
      List<String> files,
      boolean includeFileName,
      boolean includeFileRowNumber) {
    if (includeFileName || includeFileRowNumber) {
      // SELECT <<projection>> FROM read_parquet([<<files>>], hive_partitioning=0,
      // union_by_name=True,
      // filename=<<file_name>>, file_row_number=<<file_row_number>>)
      sql.append("SELECT ").append(projection).append(" FROM read_parquet([");
      appendFileNames(sql, files);
      sql.append("], hive_partitioning=0, union_by_name=True, filename=")
          .append(includeFileName)
          .append(", file_row_number=")
          .append(includeFileRowNumber)
          .append(")");
    } else {
      // SELECT <<projection>> FROM read_parquet([<<files>>], hive_partitioning=0,
      // union_by_name=True)
      sql.append("SELECT ").append(projection).append(" FROM read_parquet([");
      appendFileNames(sql, files);
      sql.append("], hive_partitioning=0, union_by_name=True)");
    }
  }

  private void appendFileNames(StringBuilder sql, List<String> values) {
    boolean isFirst = true;
    for (String value : values) {
      if (!isFirst) {
        sql.append(",");
      }
      sql.append("'").append(value).append("'");
      isFirst = false;
    }
  }

  /**
   * Generates a list of projection expressions with type casting for given fields and columns.
   *
   * @param fields List of NestedField objects representing the schema fields
   * @param columns List of column names to include in the projection (optional)
   * @return List of projection expressions with type casting
   */
  public List<String> getProjectionListWithTypeCasting(
      List<Types.NestedField> fields, List<String> columns) {
    // Create a set of columns if provided, otherwise set to null
    Set<String> columnsSet = null;
    if (columns != null && !columns.isEmpty()) {
      columnsSet = new HashSet<>(columns);
    }

    List<String> projections = new ArrayList<>();
    for (Types.NestedField tableField : fields) {
      // Generate projection expression with type casting
      StringBuilder expr = new StringBuilder();
      expr.append(
              columnsSet == null || columnsSet.contains(tableField.name())
                  ? tableField.name()
                  : NULL_VALUE_STRING)
          .append(DUCK_DB_TYPE_CAST_OPERATOR)
          .append(getTypeExpr(tableField.type()))
          .append(" AS ")
          .append(tableField.name());
      projections.add(expr.toString());
    }
    return projections;
  }

  /**
   * Generates a single projection expression string with type casting for given fields and columns.
   *
   * @param fields List of NestedField objects representing the schema fields
   * @param columns List of column names to include in the projection (optional)
   * @return Single projection expression string with type casting
   */
  public String getProjectionExprWithTypeCasting(
      List<Types.NestedField> fields, List<String> columns) {
    List<String> projections = getProjectionListWithTypeCasting(fields, columns);
    return projections.stream().collect(Collectors.joining(", "));
  }

  /**
   * Generates a projection expression adjusted for schema evolution between table and file schemas.
   *
   * @param tableSchemaType StructType representing the table schema
   * @param fileSchemaType StructType representing the file schema
   * @return Projection expression adjusted for schema evolution
   */
  private StringBuilder getProjectionExprAdjustedForSchemaEvolution(
      Types.StructType tableSchemaType, Types.StructType fileSchemaType) {
    // Create a map of file schema fields by field ID
    Map<Integer, Types.NestedField> fileSchemaFieldsMap =
        fileSchemaType.fields().stream()
            .collect(Collectors.toMap(Types.NestedField::fieldId, Function.identity()));
    List<Types.NestedField> fields = tableSchemaType.fields();
    boolean schemaChanged = fields.size() != fileSchemaType.fields().size();
    StringBuilder projection = new StringBuilder();
    for (int i = 0; i < fields.size(); i++) {
      Types.NestedField tableField = fields.get(i);
      Types.NestedField fileField = fileSchemaFieldsMap.get(tableField.fieldId());
      boolean typeChanged =
          this.getProjectionExprForField(projection, tableField, fileField, "", 1);
      // If expression changed, mark schema as changed and add alias
      if (typeChanged || !fileField.name().equals(tableField.name())) {
        schemaChanged = true;
        projection.append(" AS ").append(tableField.name());
      }

      if (i != fields.size() - 1) {
        projection.append(", ");
      }
    }

    return (schemaChanged ? projection : null);
  }

  /**
   * Generates a projection expression for a single field, handling type differences and nested
   * types.
   *
   * @param projection StringBuilder to append the projection expression
   * @param firstField NestedField representing the table field
   * @param secondField NestedField representing the file field (can be null)
   * @param secondFieldParent Parent field name for nested fields
   * @param depth Current depth in nested structure
   * @return boolean indicating whether the schema has changed
   */
  private boolean getProjectionExprForField(
      StringBuilder projection,
      Types.NestedField firstField,
      Types.NestedField secondField,
      String secondFieldParent,
      int depth) {
    boolean schemaChanged = false;
    if (secondField != null) {
      String fullFileFieldPath = concatFieldName(secondFieldParent, secondField.name());
      boolean typeEquals = firstField.type().equals(secondField.type());
      if (!typeEquals) {
        schemaChanged = true;
        if (secondField.type().isNestedType()) {
          getProjectionExprForNestedType(
              projection,
              firstField.type().asNestedType(),
              secondField.type().asNestedType(),
              fullFileFieldPath,
              depth);
        } else {
          projection
              .append(fullFileFieldPath)
              .append(DUCK_DB_TYPE_CAST_OPERATOR)
              .append(getPrimitiveTypeExpr(firstField.type().asPrimitiveType()));
        }
      } else {
        projection.append(fullFileFieldPath);
      }
    } else {
      schemaChanged = true;
      projection
          .append(NULL_VALUE_STRING)
          .append(DUCK_DB_TYPE_CAST_OPERATOR)
          .append(getTypeExpr(firstField.type()));
    }

    return schemaChanged;
  }

  /**
   * Generates a type expression for a given Type object.
   *
   * @param type Type object to generate expression for
   * @return Type expression string
   */
  public String getTypeExpr(Type type) {
    if (type.isPrimitiveType()) return getPrimitiveTypeExpr(type.asPrimitiveType());
    if (type.isNestedType()) return getNestedTypeExpr(type.asNestedType());
    return null;
  }

  /**
   * Generates a type expression for a nested type (struct, list, or map).
   *
   * @param type NestedType object to generate expression for
   * @return Nested type expression string
   */
  private String getNestedTypeExpr(Type.NestedType type) {
    if (type.isStructType()) {
      StringBuilder expr = new StringBuilder();
      expr.append("STRUCT(");
      boolean isFirst = true;
      for (Types.NestedField field : type.fields()) {
        if (!isFirst) {
          expr.append(",");
        }
        isFirst = false;
        expr.append(field.name()).append(" ").append(getTypeExpr(field.type()));
      }
      expr.append(")");
      return expr.toString();
    }

    if (type.isListType()) {
      StringBuilder expr = new StringBuilder();
      expr.append(getTypeExpr(type.fields().get(0).type())).append("[]");
      return expr.toString();
    }

    if (type.isMapType()) {
      List<Types.NestedField> fields = type.fields();
      StringBuilder expr = new StringBuilder();
      expr.append("MAP(")
          .append(getTypeExpr(fields.get(0).type()))
          .append(", ")
          .append(getTypeExpr(fields.get(1).type()))
          .append(")");
      return expr.toString();
    }
    return null;
  }

  /**
   * Generates a type expression for a primitive type.
   *
   * @param type Type object representing a primitive type
   * @return Primitive type expression string
   */
  public String getPrimitiveTypeExpr(Type type) {
    Type.TypeID typeID = type.typeId();
    // Return appropriate type string based on TypeID
    if (typeID == Type.TypeID.BOOLEAN) return "BOOLEAN";
    if (typeID == Type.TypeID.INTEGER) return "INTEGER";
    if (typeID == Type.TypeID.LONG) return "LONG";
    if (typeID == Type.TypeID.FLOAT) return "FLOAT";
    if (typeID == Type.TypeID.DOUBLE) return "DOUBLE";
    if (typeID == Type.TypeID.DECIMAL) {
      Types.DecimalType decimalType = (Types.DecimalType) type;
      return String.format("DECIMAL(%d,%d)", decimalType.precision(), decimalType.scale());
    }
    if (typeID == Type.TypeID.DATE) return "DATE";
    if (typeID == Type.TypeID.TIME) return "TIME";
    if (typeID == Type.TypeID.TIMESTAMP) {
      if (((Types.TimestampType) type).shouldAdjustToUTC()) return "TIMESTAMPTZ";
      else return "TIMESTAMP";
    }
    if (typeID == Type.TypeID.STRING) return "STRING";
    if (typeID == Type.TypeID.UUID) return "UUID";
    if (typeID == Type.TypeID.BINARY) return "BINARY";

    return null;
  }

  /**
   * Concatenates parent and child field names for nested fields.
   *
   * @param parent Parent field name
   * @param name Child field name
   * @return Concatenated field name
   */
  private String concatFieldName(String parent, String name) {
    if (parent == null || parent.isBlank()) return name;
    else return parent + DUCK_DB_TYPE_NESTED_FIELD_SEPARATOR + name;
  }

  /**
   * Generates a projection expression for nested types, handling struct, list, and map types.
   *
   * @param projection StringBuilder to append the projection expression
   * @param firstNestedType NestedType representing the table field type
   * @param secondNestedType NestedType representing the file field type
   * @param secondTypeParent Parent field name for nested fields
   * @param depth Current depth in nested structure
   */
  private void getProjectionExprForNestedType(
      StringBuilder projection,
      Type.NestedType firstNestedType,
      Type.NestedType secondNestedType,
      String secondTypeParent,
      int depth) {

    if (firstNestedType.isStructType()) {
      if (!secondNestedType.isStructType()) {
        throw new ValidationException("Data type mismatch. It should be a Struct Type.");
      }

      Map<Integer, Types.NestedField> secondFieldsMap =
          secondNestedType.fields().stream()
              .collect(Collectors.toMap(Types.NestedField::fieldId, Function.identity()));
      List<Types.NestedField> fields = firstNestedType.fields();
      projection.append("{");
      for (int i = 0; i < fields.size(); i++) {
        Types.NestedField firstField = fields.get(i);
        Types.NestedField secondField = secondFieldsMap.get(firstField.fieldId());
        projection.append(firstField.name()).append(DUCK_DB_TYPE_KEY_VALUE_SEPARATOR);
        getProjectionExprForField(projection, firstField, secondField, secondTypeParent, depth + 1);
        if (i != fields.size() - 1) {
          projection.append(",");
        }
      }
      projection.append("}");
    } else if (firstNestedType.isListType()) {
      if (!secondNestedType.isListType()) {
        throw new ValidationException("Data type mismatch. It should be a List Type.");
      }

      Types.NestedField firstField = firstNestedType.fields().get(0);
      Types.NestedField secondField = secondNestedType.fields().get(0);

      // "[newType for iterationVarName in secondTypeParent]"
      projection.append("[");
      String iterationVarName = String.format(LOCAL_ITERATION_VAR_NAME, depth);
      getProjectionExprForField(
          projection,
          Types.NestedField.optional(firstField.fieldId(), iterationVarName, firstField.type()),
          Types.NestedField.optional(secondField.fieldId(), iterationVarName, secondField.type()),
          "",
          depth + 1);
      projection
          .append(" for ")
          .append(iterationVarName)
          .append(" in ")
          .append(secondTypeParent)
          .append("]");
    } else if (firstNestedType.isMapType()) {
      if (!secondNestedType.isMapType()) {
        throw new ValidationException("Data type mismatch. It should be a Map Type.");
      }

      // map_from_entries([{key: newKeyType, value: newValueType} for iterationVarName in
      // map_entries(secondTypeParent)])
      List<Types.NestedField> firstFields = firstNestedType.fields();
      List<Types.NestedField> secondFields = secondNestedType.fields();
      String iterationVarName = String.format(LOCAL_ITERATION_VAR_NAME, depth);

      projection.append("map_from_entries([{key: ");
      getProjectionExprForField(
          projection, firstFields.get(0), secondFields.get(0), iterationVarName, depth + 1);
      projection.append(", value: ");
      getProjectionExprForField(
          projection, firstFields.get(1), secondFields.get(1), iterationVarName, depth + 1);
      projection
          .append("} for ")
          .append(iterationVarName)
          .append(" in map_entries(")
          .append(secondTypeParent)
          .append(")])");
    } else {
      throw new ValidationException("Unsupported Type %s.", firstNestedType);
    }
  }

  /**
   * Generates a DuckDB SQL filter expression for a given table and filter.
   *
   * @param table The table to apply the filter to
   * @param filter The filter expression
   * @return A SQL string representing the filter
   */
  public String getDuckDBFilterSql(
      Table table, com.arcesium.swiftlake.expressions.Expression filter) {
    StringBuilder sql = new StringBuilder();
    processExpression(
        sql, table.schema(), Expressions.resolveExpression(filter).getExpression(), null);
    return sql.toString();
  }

  /**
   * Generates a DuckDB SQL filter expression for a given table, filter, and table alias.
   *
   * @param table The table to apply the filter to
   * @param filter The filter expression
   * @param tableAlias The alias for the table in the SQL query
   * @return A SQL string representing the filter
   */
  public String getDuckDBFilterSql(
      Table table, com.arcesium.swiftlake.expressions.Expression filter, String tableAlias) {
    StringBuilder sql = new StringBuilder();
    processExpression(
        sql, table.schema(), Expressions.resolveExpression(filter).getExpression(), tableAlias);
    return sql.toString();
  }

  // Recursively processes the expression tree to build the SQL filter
  private void processExpression(
      StringBuilder sql, Schema schema, Expression filter, String tableAlias) {
    if (filter instanceof And andOp) {
      sql.append("(");
      processExpression(sql, schema, andOp.left(), tableAlias);
      sql.append(" AND ");
      processExpression(sql, schema, andOp.right(), tableAlias);
      sql.append(")");
    } else if (filter instanceof Or orOp) {
      sql.append("(");
      processExpression(sql, schema, orOp.left(), tableAlias);
      sql.append(" OR ");
      processExpression(sql, schema, orOp.right(), tableAlias);
      sql.append(")");
    } else if (filter instanceof Not notOp) {
      sql.append("( NOT (");
      processExpression(sql, schema, notOp.child(), tableAlias);
      sql.append("))");
    } else if (filter instanceof UnboundPredicate<?> unboundPredicate) {
      processUnboundPredicate(sql, (UnboundPredicate<Object>) unboundPredicate, schema, tableAlias);
    } else if (filter instanceof True) {
      sql.append("True");
    } else if (filter instanceof False) {
      sql.append("False");
    } else {
      throw new ValidationException("Unsupported expression type %s", filter.getClass());
    }
  }

  // Processes an unbound predicate and converts it to a SQL condition
  private void processUnboundPredicate(
      StringBuilder sql, UnboundPredicate<Object> predicate, Schema schema, String tableAlias) {
    if (predicate.term() instanceof UnboundTransform)
      throw new ValidationException("UnboundTransform is not supported");

    String columnName = ((UnboundTerm) predicate.term()).ref().name();
    String qualifiedColumnName = columnName;
    if (tableAlias != null) qualifiedColumnName = tableAlias + "." + columnName;
    StringBuilder literalSql = null;

    if (predicate.literals() != null) {
      literalSql = new StringBuilder();
      if (predicate.op() == Expression.Operation.STARTS_WITH
          || predicate.op() == Expression.Operation.NOT_STARTS_WITH) {
        Object value = predicate.literal().value();
        if (!(value instanceof String string)) {
          throw new ValidationException("Invalid literal provided for %s", predicate.op());
        }
        literalSql.append("'").append(string.replace("'", "''")).append("%'");
      } else {
        Type type = schema.findField(columnName).type();
        boolean isFirst = true;
        for (Literal<Object> literal : predicate.literals()) {
          if (!isFirst) {
            literalSql.append(", ");
          }
          isFirst = false;
          literalSql.append(getPrimitiveTypeValueForSql(type.asPrimitiveType(), literal.value()));
        }
      }
    }

    switch (predicate.op()) {
      case IS_NULL -> sql.append(qualifiedColumnName).append(" IS NULL");
      case NOT_NULL -> sql.append(qualifiedColumnName).append(" IS NOT NULL");
      case IS_NAN -> sql.append("isnan(").append(qualifiedColumnName).append(")");
      case NOT_NAN -> sql.append("(NOT isnan(").append(qualifiedColumnName).append("))");
      case LT -> sql.append(qualifiedColumnName).append(" < ").append(literalSql);
      case LT_EQ -> sql.append(qualifiedColumnName).append(" <= ").append(literalSql);
      case GT -> sql.append(qualifiedColumnName).append(" > ").append(literalSql);
      case GT_EQ -> sql.append(qualifiedColumnName).append(" >= ").append(literalSql);
      case EQ -> sql.append(qualifiedColumnName).append(" = ").append(literalSql);
      case NOT_EQ -> sql.append(qualifiedColumnName).append(" != ").append(literalSql);
      case STARTS_WITH -> sql.append(qualifiedColumnName).append(" LIKE ").append(literalSql);
      case NOT_STARTS_WITH ->
          sql.append(qualifiedColumnName).append(" NOT LIKE ").append(literalSql);
      case IN -> sql.append(qualifiedColumnName).append(" IN (").append(literalSql).append(")");
      case NOT_IN ->
          sql.append(qualifiedColumnName).append(" NOT IN (").append(literalSql).append(")");
      default ->
          throw new ValidationException("Unsupported predicate: operation %s", predicate.op());
    }
  }

  /**
   * Converts a primitive type value to its SQL representation.
   *
   * @param type The primitive type
   * @param value The value to convert
   * @return A SQL string representation of the value
   */
  public String getPrimitiveTypeValueForSql(Type.PrimitiveType type, Object value) {
    return "("
        + getPrimitiveTypeStringValueForSql(type, value, true)
        + ")"
        + DUCK_DB_TYPE_CAST_OPERATOR
        + getPrimitiveTypeExpr(type);
  }

  /**
   * Converts a primitive type value to its string representation for SQL.
   *
   * @param type The primitive type
   * @param value The value to convert
   * @return A string representation of the value
   */
  public String getPrimitiveTypeStringValueForSql(
      Type.PrimitiveType type, Object value, boolean shouldQuoteStrings) {
    Type.TypeID typeID = type.typeId();

    if (value == null) {
      return NULL_VALUE_STRING;
    } else if (value instanceof String stringValue) {
      return shouldQuoteStrings ? ("'" + stringValue.replace("'", "''") + "'") : stringValue;
    } else if (typeID == Type.TypeID.DATE) {
      ValidationException.check(value instanceof LocalDate, "Invalid date value %s", value);
      String stringValue = DateTimeUtil.formatLocalDate((LocalDate) value);
      return shouldQuoteStrings ? ("'" + stringValue + "'") : stringValue;
    } else if (typeID == Type.TypeID.TIME) {
      ValidationException.check(value instanceof LocalTime, "Invalid time value %s", value);
      String stringValue = DateTimeUtil.formatLocalTimeWithMicros((LocalTime) value);
      return shouldQuoteStrings ? ("'" + stringValue + "'") : stringValue;
    } else if (typeID == Type.TypeID.TIMESTAMP) {
      if (((Types.TimestampType) type).shouldAdjustToUTC()) {
        ValidationException.check(
            value instanceof OffsetDateTime, "Invalid timestamptz value %s", value);
        String stringValue = DateTimeUtil.formatOffsetDateTimeWithMicros((OffsetDateTime) value);
        return shouldQuoteStrings ? ("'" + stringValue + "'") : stringValue;
      } else {
        if (value instanceof Timestamp) {
          value = ((Timestamp) value).toLocalDateTime();
        }
        ValidationException.check(
            value instanceof LocalDateTime, "Invalid timestamp value %s", value);
        String stringValue = DateTimeUtil.formatLocalDateTimeWithMicros((LocalDateTime) value);
        return shouldQuoteStrings ? ("'" + stringValue + "'") : stringValue;
      }
    } else if (typeID == Type.TypeID.FLOAT && value instanceof Float floatValue) {
      if (Float.isNaN(floatValue)) {
        return shouldQuoteStrings ? "'nan'" : "nan";
      } else if (Float.isInfinite(floatValue)) {
        if (floatValue > 0) {
          return shouldQuoteStrings ? "'inf'" : "inf";
        } else {
          return shouldQuoteStrings ? "'-inf'" : "-inf";
        }
      } else {
        return Float.toString(floatValue);
      }
    } else if (typeID == Type.TypeID.DOUBLE && value instanceof Double doubleValue) {
      if (Double.isNaN(doubleValue)) {
        return shouldQuoteStrings ? "'nan'" : "nan";
      } else if (Double.isInfinite(doubleValue)) {
        if (doubleValue > 0) {
          return shouldQuoteStrings ? "'inf'" : "inf";
        } else {
          return shouldQuoteStrings ? "'-inf'" : "-inf";
        }
      } else {
        return Double.toString(doubleValue);
      }
    } else {
      return value.toString();
    }
  }

  /**
   * Creates a new type for comparison operations.
   *
   * @param type The original type
   * @return A new type suitable for comparison
   * @throws ValidationException if the type is not supported
   */
  public Type createTypeForComparison(Type type) {
    if (type.isPrimitiveType()) {
      return type;
    } else if (type.isStructType()) {
      return copyStructTypeForComparison(type.asStructType());
    } else if (type.isMapType()) {
      return copyMapTypeForComparison(type.asMapType());
    } else if (type.isListType()) {
      return copyListTypeForComparison(type.asListType());
    } else {
      throw new ValidationException("Could not handle type %s", type);
    }
  }

  // Creates a copy of a struct type for comparison
  private Types.StructType copyStructTypeForComparison(Types.StructType structType) {
    List<Types.NestedField> fields =
        structType.fields().stream()
            .map(
                f ->
                    Types.NestedField.optional(
                        f.fieldId(), f.name(), createTypeForComparison(f.type())))
            .collect(Collectors.toList());

    return Types.StructType.of(fields);
  }

  // Creates a copy of a map type for comparison
  private Types.MapType copyMapTypeForComparison(Types.MapType mapType) {
    return Types.MapType.ofOptional(
        mapType.keyId(),
        mapType.valueId(),
        createTypeForComparison(mapType.keyType()),
        createTypeForComparison(mapType.valueType()));
  }

  // Creates a copy of a list type for comparison
  private Types.ListType copyListTypeForComparison(Types.ListType listType) {
    return Types.ListType.ofOptional(
        listType.elementId(), createTypeForComparison(listType.elementType()));
  }

  /**
   * Escapes a column name for safe SQL use. This method quotes the column name with double quotes
   * and escapes any embedded double quotes.
   *
   * @param columnName The column name to escape
   * @return The escaped and quoted column name
   */
  public String escapeColumnName(String columnName) {
    ValidationException.checkNotNull(columnName, "Column name cannot be null");
    // Replace any double quotes with escaped double quotes
    String escaped = columnName.replace("\"", "\"\"");
    // Wrap with double quotes
    return "\"" + escaped + "\"";
  }
}
