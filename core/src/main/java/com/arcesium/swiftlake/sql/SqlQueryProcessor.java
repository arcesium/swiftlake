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

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.DateTimeUtil;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.mybatis.type.SwiftLakeDuckDBTime;
import com.google.common.base.Splitter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Database;
import net.sf.jsqlparser.schema.Server;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Processes SQL queries, handling various data types and expressions. */
public class SqlQueryProcessor {
  private final SwiftLakeEngine swiftLakeEngine;

  /**
   * Constructs a SqlQueryProcessor with the given SwiftLakeEngine.
   *
   * @param swiftLakeEngine The SwiftLake engine to use for processing queries.
   */
  public SqlQueryProcessor(SwiftLakeEngine swiftLakeEngine) {
    this.swiftLakeEngine = swiftLakeEngine;
  }

  /**
   * Parses a condition expression string into a SwiftLake Expression.
   *
   * @param exprStr The condition expression string to parse.
   * @param schema The schema to use for parsing.
   * @param params Map of parameters for the expression.
   * @return A SwiftLake Expression representing the parsed condition.
   * @throws SwiftLakeException if an error occurs during parsing.
   */
  public com.arcesium.swiftlake.expressions.Expression parseConditionExpression(
      String exprStr, Schema schema, Map<Integer, Object> params) {
    try {
      Expression expr = CCJSqlParserUtil.parseCondExpression(exprStr, false);
      return getExpressionForSqlExpression(expr, schema, params, null, null);
    } catch (JSQLParserException ex) {
      throw new SwiftLakeException(ex, "An error occurred while parsing sql");
    }
  }

  /**
   * Processes a SQL query without parameters.
   *
   * @param sql The SQL query to process.
   * @param connection The database connection.
   * @return A pair containing the transformed SQL and a list of table filters.
   */
  public Pair<String, List<TableFilter>> process(String sql, Connection connection) {
    return process(sql, null, connection, null);
  }

  /**
   * Processes a SQL query with parameters and optional Iceberg tables.
   *
   * @param sql The SQL query to process.
   * @param params Map of parameters for the query.
   * @param connection The SwiftLake connection.
   * @param tables List of Iceberg tables to use.
   * @return A pair containing the transformed SQL and a list of table filters.
   * @throws SwiftLakeException if an error occurs during processing.
   */
  public Pair<String, List<TableFilter>> process(
      String sql,
      Map<Integer, Object> params,
      Connection connection,
      List<org.apache.iceberg.Table> tables) {
    try {
      List<TableFilter> tableFilters = new ArrayList<>();
      StringBuilder transformedSql = new StringBuilder();

      List<Pair<String, Boolean>> parseBlocks = splitSqlIntoParseBlocks(sql);
      // Validate parameters
      if (params != null && !params.isEmpty() && parseBlocks.size() > 1) {
        throw new ValidationException(
            "Parameters are not supported when multiple parse blocks exist.");
      }
      // Create a map of table names to Iceberg tables
      Map<String, org.apache.iceberg.Table> tablesMap =
          (tables == null)
              ? null
              : tables.stream()
                  .collect(Collectors.toMap(table -> getTableName(table), table -> table));

      // Process each parse block
      for (int i = 0; i < parseBlocks.size(); i++) {
        Pair<String, Boolean> parseBlock = parseBlocks.get(i);
        if (parseBlock.getRight()) {
          // Parse and process the SQL statement
          Statement statement = CCJSqlParserUtil.parse(parseBlock.getLeft());
          new TablesNamesFinder() {
            @Override
            public void visit(PlainSelect plainSelect) {
              super.visit(plainSelect);
              TableFilter tableFilter =
                  processFromItem(
                      plainSelect.getFromItem(),
                      plainSelect.getWhere(),
                      plainSelect.getJoins(),
                      params,
                      connection,
                      tablesMap);
              if (tableFilter != null) {
                tableFilters.add(tableFilter);
              }
            }
          }.getTables(statement);
          transformedSql.append(statement.toString());
          if (i != parseBlocks.size() - 1) {
            transformedSql.append("\n");
          }
        } else {
          // Append unparsed block as-is
          transformedSql.append(parseBlock.getLeft());
        }
      }
      return Pair.of(transformedSql.toString(), tableFilters);
    } catch (JSQLParserException ex) {
      throw new SwiftLakeException(ex, "An error occurred while parsing sql");
    }
  }

  /**
   * Splits the SQL into parse blocks based on SwiftLake markers.
   *
   * @param sql The SQL string to split.
   * @return A list of pairs containing the SQL blocks and whether they should be parsed.
   */
  private List<Pair<String, Boolean>> splitSqlIntoParseBlocks(String sql) {
    List<Pair<String, Boolean>> parseBlocks = new ArrayList<>();
    StringBuilder currentBlock = new StringBuilder();
    boolean insideBlock = false;
    Iterable<String> lines = Splitter.on('\n').split(sql);
    for (String line : lines) {
      if (line.contains(SwiftLakeEngine.SWIFTLAKE_PARSE_BEGIN_MARKER)) {
        if (currentBlock.length() > 0) {
          parseBlocks.add(Pair.of(currentBlock.toString(), false));
          currentBlock.setLength(0);
        }
        insideBlock = true;
      } else if (line.contains(SwiftLakeEngine.SWIFTLAKE_PARSE_END_MARKER)) {
        parseBlocks.add(Pair.of(currentBlock.toString(), insideBlock));
        currentBlock.setLength(0);
        insideBlock = false;
      } else {
        currentBlock.append(line).append("\n");
      }
    }

    if (currentBlock.length() > 0) {
      parseBlocks.add(Pair.of(currentBlock.toString(), false));
    }

    return (parseBlocks.size() > 1) ? parseBlocks : Arrays.asList(Pair.of(sql, true));
  }

  /**
   * Processes a FromItem to create a TableFilter.
   *
   * @param fromItem The FromItem to process.
   * @param whereCondition The WHERE condition of the query.
   * @param joins The JOIN clauses of the query.
   * @param params Map of parameters for the query.
   * @param connection The SwiftLake connection.
   * @param tablesMap Map of table names to Iceberg tables.
   * @return A TableFilter if the FromItem is valid, null otherwise.
   */
  private TableFilter processFromItem(
      FromItem fromItem,
      Expression whereCondition,
      List<Join> joins,
      Map<Integer, Object> params,
      Connection connection,
      Map<String, org.apache.iceberg.Table> tablesMap) {
    if (!(fromItem instanceof Table sqlTable)) {
      return null;
    }

    if (sqlTable.getNameParts().size() < 2) {
      return null;
    }

    // Handle quoted identifiers
    sqlTable.setName(removeDoubleQuotes(sqlTable.getName()));
    sqlTable.setSchemaName(removeDoubleQuotes(sqlTable.getSchemaName()));
    if (sqlTable.getDatabase() != null && sqlTable.getDatabase().getDatabaseName() != null) {
      Server server = null;
      if (sqlTable.getDatabase().getServer() != null) {
        server =
            new Server(
                removeDoubleQuotes(sqlTable.getDatabase().getServer().getFullyQualifiedName()));
      }
      sqlTable.setDatabase(
          new Database(server, removeDoubleQuotes(sqlTable.getDatabase().getDatabaseName())));
    }

    Pair<String, TimeTravelOptions> timeTravelOptionsPair =
        parseTimeTravelOptions(sqlTable.getName());
    TimeTravelOptions timeTravelOptions = null;
    if (timeTravelOptionsPair != null) {
      sqlTable.setName(timeTravelOptionsPair.getLeft());
      timeTravelOptions = timeTravelOptionsPair.getRight();
    }

    String tableName = sqlTable.getFullyQualifiedName();
    org.apache.iceberg.Table table = null;
    if (tablesMap != null && tablesMap.containsKey(tableName)) {
      table = tablesMap.get(tableName);
    } else {
      table = swiftLakeEngine.getTable(tableName);
    }

    com.arcesium.swiftlake.expressions.Expression condition = null;
    Set<String> joinColumns = new HashSet<>();
    if (joins != null) {
      for (Join join : joins) {
        if (join.getOnExpressions() == null || join.getOnExpressions().isEmpty()) {
          throw new ValidationException(
              "Join condition is missing for table %s", join.getFromItem());
        }
        if (join.getOnExpressions().size() > 1) {
          throw new ValidationException(
              "Multiple join conditions are present for table %s", join.getFromItem());
        }
        if (!(join.isInnerJoin() || join.isRight())) {
          continue;
        }

        Pair<com.arcesium.swiftlake.expressions.Expression, Set<String>> result = null;
        if (connection == null) {
          try (Connection newConnection = swiftLakeEngine.createDuckDBConnection()) {
            result =
                getExpressionForJoin(
                    join, table.schema(), sqlTable.getAlias().getName(), params, newConnection);
          } catch (SQLException e) {
            throw new SwiftLakeException(e, "An error occurred while closing connection.");
          }
        } else {
          result =
              getExpressionForJoin(
                  join, table.schema(), sqlTable.getAlias().getName(), params, connection);
        }
        condition =
            (condition == null) ? result.getLeft() : Expressions.and(condition, result.getLeft());
        joinColumns.addAll(result.getRight());
      }
    }

    if (whereCondition != null) {
      // Parse condition sql
      com.arcesium.swiftlake.expressions.Expression expr =
          getExpressionForSqlExpression(
              whereCondition,
              table.schema(),
              params,
              sqlTable.getAlias() == null ? null : sqlTable.getAlias().getName(),
              joinColumns);

      if (condition == null) {
        condition = expr;
      } else {
        condition = Expressions.and(condition, expr);
      }
    }

    if (condition == null) {
      condition = Expressions.alwaysTrue();
    }

    String placeholder = "<<" + UUID.randomUUID() + ">>";
    TableFilter tableFilter = new TableFilter(tableName, condition, placeholder);
    tableFilter.setTable(table);
    tableFilter.setTimeTravelOptions(timeTravelOptions);

    // Replace table name with placeholder
    sqlTable.setDatabase(new Database(null));
    sqlTable.setSchemaName(null);
    sqlTable.setName(placeholder);

    return tableFilter;
  }

  /**
   * Parses time travel options from a table name.
   *
   * @param tableName The table name potentially containing time travel options.
   * @return A pair containing the actual table name and parsed TimeTravelOptions, or null if no
   *     options are present.
   */
  public Pair<String, TimeTravelOptions> parseTimeTravelOptions(String tableName) {
    int index = tableName.indexOf('$');
    if (index == -1 || index == tableName.length() - 1) {
      return null;
    }
    String optionStr = tableName.substring(index + 1);
    LocalDateTime timestamp = null;
    String branchOrTagName = null;
    Long snapshotId = null;

    // Parse different time travel options
    if (optionStr.startsWith("branch_")) {
      branchOrTagName = optionStr.substring("branch_".length());
    } else if (optionStr.startsWith("tag_")) {
      branchOrTagName = optionStr.substring("tag_".length());
    } else if (optionStr.startsWith("timestamp_")) {
      timestamp =
          DateTimeUtil.parseLocalDateTimeToMicros(optionStr.substring("timestamp_".length()));
    } else if (optionStr.startsWith("snapshot_")) {
      String snapshotStr = optionStr.substring("snapshot_".length());
      try {
        snapshotId = Long.valueOf(snapshotStr);
      } catch (NumberFormatException ex) {
        throw new ValidationException("Invalid snapshot Id %s .", snapshotStr);
      }
    } else {
      throw new ValidationException("Invalid time travel option %s.", optionStr);
    }

    TimeTravelOptions options = new TimeTravelOptions(timestamp, branchOrTagName, snapshotId);
    return Pair.of(tableName.substring(0, index), options);
  }

  /**
   * Removes double quotes from the beginning and end of a string if present.
   *
   * @param value The input string.
   * @return The string with double quotes removed, or the original string if no quotes were
   *     present.
   */
  private String removeDoubleQuotes(String value) {
    if (value != null && value.startsWith("\"") && value.endsWith("\"")) {
      return value.substring(1, value.length() - 1);
    }
    return value;
  }

  /**
   * Constructs the full table name from an Iceberg Table object.
   *
   * @param table The Iceberg Table object.
   * @return The full table name as a dot-separated string.
   */
  public String getTableName(org.apache.iceberg.Table table) {
    StringJoiner name = new StringJoiner(".");
    TableIdentifier id = TableIdentifier.parse(table.name());
    String catalogName = swiftLakeEngine.getCatalog().name();
    for (int i = 0; i < id.namespace().length(); i++) {
      String part = id.namespace().level(i);
      // Exclude the catalog name
      if (i == 0 && part.equals(catalogName)) {
        continue;
      }
      name.add(part);
    }
    name.add(id.name());
    return name.toString();
  }

  /**
   * Generates a SwiftLake expression for a join condition.
   *
   * @param join The SQL join object.
   * @param schema The schema of the table.
   * @param tableAlias The alias of the table.
   * @param params Map of parameters.
   * @param connection The database connection.
   * @return A pair containing the expression and a set of join columns.
   */
  private Pair<com.arcesium.swiftlake.expressions.Expression, Set<String>> getExpressionForJoin(
      Join join,
      Schema schema,
      String tableAlias,
      Map<Integer, Object> params,
      Connection connection) {
    FromItem fromItem = join.getFromItem();
    ValidationException.check(fromItem instanceof Table, "Unsupported Join table %s", fromItem);

    String sourceSqlTable = fromItem.toString();
    List<Map<String, Object>> data = null;
    Set<String> joinColumns = null;
    try (java.sql.Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(getDataSql(sourceSqlTable))) {
      Pair<List<Map<String, Object>>, List<String>> result = getRecordsFromResultSet(resultSet);
      data = result.getLeft();
      joinColumns = new HashSet<>(result.getRight());
    } catch (SQLException e) {
      throw new SwiftLakeException(
          e, "An error occurred while creating condition expression for join.");
    }

    if (data.isEmpty()) {
      return Pair.of(Expressions.alwaysFalse(), joinColumns);
    }

    // Determine the starting parameter index
    int startParamIndex = 1;
    if (params != null && !params.isEmpty()) {
      startParamIndex =
          params.entrySet().stream().map(e -> e.getKey()).max(Integer::compareTo).get() + 1;
    }

    Map<Integer, String> paramNames = new HashMap<>();
    Expression joinCondition = join.getOnExpressions().stream().findFirst().get();
    // Create a copy of joinCondition
    try {
      joinCondition = CCJSqlParserUtil.parseCondExpression(joinCondition.toString(), false);
    } catch (JSQLParserException e) {
      throw new SwiftLakeException(e, "An error occurred while parsing sql");
    }
    parameterizeJoinCondition(
        joinCondition,
        schema,
        tableAlias,
        join.getFromItem().getAlias().getName(),
        joinColumns,
        paramNames,
        startParamIndex,
        (expr) -> join.setOnExpressions(List.of(expr)));

    com.arcesium.swiftlake.expressions.Expression joinExpr = Expressions.alwaysFalse();
    for (Map<String, Object> record : data) {
      Map<Integer, Object> paramsForRecord = new HashMap<>();
      if (params != null && !params.isEmpty()) {
        paramsForRecord.putAll(params);
      }

      for (Map.Entry<Integer, String> entry : paramNames.entrySet()) {
        paramsForRecord.put(entry.getKey(), record.get(entry.getValue()));
      }

      com.arcesium.swiftlake.expressions.Expression recordExpr =
          getExpressionForSqlExpression(
              joinCondition, schema, paramsForRecord, tableAlias, joinColumns);
      joinExpr = Expressions.or(joinExpr, recordExpr);

      if (paramNames.isEmpty()) {
        break;
      }
    }

    return Pair.of(joinExpr, joinColumns);
  }

  /**
   * Generates SQL to select all data from a table.
   *
   * @param tableName The name of the table to select from.
   * @return The SQL string to select all data from the table.
   */
  public String getDataSql(String tableName) {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT * FROM ");
    sb.append(tableName);
    return sb.toString();
  }

  /**
   * Extracts records and column names from a ResultSet.
   *
   * @param resultSet The ResultSet to process.
   * @return A pair containing a list of records and a list of column names.
   */
  private Pair<List<Map<String, Object>>, List<String>> getRecordsFromResultSet(
      ResultSet resultSet) {
    try {
      List<Map<String, Object>> data = new ArrayList<>();
      List<String> columnNames = new ArrayList<>();
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int columnCount = resultSetMetaData.getColumnCount();
      // Extract column names
      for (int i = 1; i <= columnCount; i++) {
        columnNames.add(resultSetMetaData.getColumnName(i));
      }
      // Extract data
      while (resultSet.next()) {
        Map<String, Object> record = new HashMap<>();
        for (String name : columnNames) {
          record.put(name, resultSet.getObject(name));
        }
        data.add(record);
      }
      return Pair.of(data, columnNames);
    } catch (SQLException ex) {
      throw new SwiftLakeException(ex, "An error occurred while reading data from result set.");
    }
  }

  /**
   * Converts a SQL expression to a SwiftLake expression.
   *
   * @param expr The SQL expression to convert
   * @param schema The schema of the table
   * @param params Map of parameter values
   * @param alias The alias of the main table
   * @param joinTableColumns Set of columns from the joined table
   * @return The equivalent SwiftLake expression
   * @throws ValidationException if the expression is not supported or invalid
   */
  public com.arcesium.swiftlake.expressions.Expression getExpressionForSqlExpression(
      Expression expr,
      Schema schema,
      Map<Integer, Object> params,
      String alias,
      Set<String> joinTableColumns) {
    if (expr instanceof Parenthesis parenthesis) {
      // Handle parentheses by processing the inner expression
      return getExpressionForSqlExpression(
          parenthesis.getExpression(), schema, params, alias, joinTableColumns);
    } else if (expr instanceof AndExpression andExpr) {
      // Handle AND expressions
      com.arcesium.swiftlake.expressions.Expression leftNested =
          getExpressionForSqlExpression(
              andExpr.getLeftExpression(), schema, params, alias, joinTableColumns);
      com.arcesium.swiftlake.expressions.Expression rightNested =
          getExpressionForSqlExpression(
              andExpr.getRightExpression(), schema, params, alias, joinTableColumns);
      return Expressions.and(leftNested, rightNested);
    } else if (expr instanceof OrExpression orExpr) {
      // Handle OR expressions
      com.arcesium.swiftlake.expressions.Expression leftNested =
          getExpressionForSqlExpression(
              orExpr.getLeftExpression(), schema, params, alias, joinTableColumns);
      com.arcesium.swiftlake.expressions.Expression rightNested =
          getExpressionForSqlExpression(
              orExpr.getRightExpression(), schema, params, alias, joinTableColumns);
      return Expressions.or(leftNested, rightNested);
    } else if (expr instanceof NotExpression notExpr) {
      // Handle NOT expressions
      com.arcesium.swiftlake.expressions.Expression nestedExpr =
          getExpressionForSqlExpression(
              notExpr.getExpression(), schema, params, alias, joinTableColumns);
      return Expressions.not(nestedExpr);
    } else if (expr instanceof IsNullExpression isNullExpr) {
      // Handle IS NULL and IS NOT NULL expressions
      if (isColumn(isNullExpr.getLeftExpression())) {
        Column col = (Column) isNullExpr.getLeftExpression();
        validateColumn(col, schema, alias, joinTableColumns);
        if (isNullExpr.isNot()) {
          return Expressions.notNull(col.getColumnName());
        } else {
          return Expressions.isNull(col.getColumnName());
        }
      } else {
        throw new ValidationException("Unsupported IsNullExpression %s", isNullExpr);
      }
    } else if (expr instanceof IsDistinctExpression isDistinctExpr) {
      // Handle IS DISTINCT FROM and IS NOT DISTINCT FROM expressions
      Pair<Boolean, Pair<String, Object>> result =
          getOperands(
              isDistinctExpr.getLeftExpression(),
              isDistinctExpr.getRightExpression(),
              schema,
              params,
              alias,
              joinTableColumns);
      Pair<String, Object> parts = result.getRight();
      if (parts.getRight() == null) {
        if (isDistinctExpr.isNot()) {
          return Expressions.isNull(parts.getLeft());
        } else {
          return Expressions.notNull(parts.getLeft());
        }
      } else {
        if (isDistinctExpr.isNot()) {
          return Expressions.equal(parts.getLeft(), parts.getRight());
        } else {
          return Expressions.notEqual(parts.getLeft(), parts.getRight());
        }
      }
    } else if (expr instanceof EqualsTo equalsToExpr) {
      // Handle equality expressions
      Pair<Boolean, Pair<String, Object>> result =
          getOperandsForComparison(equalsToExpr, schema, params, alias, joinTableColumns);
      Pair<String, Object> parts = result.getRight();
      return Expressions.equal(parts.getLeft(), parts.getRight());
    } else if (expr instanceof NotEqualsTo notEqualsToExpr) {
      // Handle inequality expressions
      Pair<Boolean, Pair<String, Object>> result =
          getOperandsForComparison(notEqualsToExpr, schema, params, alias, joinTableColumns);
      Pair<String, Object> parts = result.getRight();
      return Expressions.notEqual(parts.getLeft(), parts.getRight());
    } else if (expr instanceof GreaterThan greaterThanExpr) {
      // Handle greater than expressions
      Pair<Boolean, Pair<String, Object>> result =
          getOperandsForComparison(greaterThanExpr, schema, params, alias, joinTableColumns);
      Pair<String, Object> parts = result.getRight();
      if (result.getLeft()) {
        return Expressions.lessThan(parts.getLeft(), parts.getRight());
      } else {
        return Expressions.greaterThan(parts.getLeft(), parts.getRight());
      }
    } else if (expr instanceof GreaterThanEquals greaterThanEqualsExpr) {
      // Handle greater than or equal to expressions
      Pair<Boolean, Pair<String, Object>> result =
          getOperandsForComparison(greaterThanEqualsExpr, schema, params, alias, joinTableColumns);
      Pair<String, Object> parts = result.getRight();
      if (result.getLeft()) {
        return Expressions.lessThanOrEqual(parts.getLeft(), parts.getRight());
      } else {
        return Expressions.greaterThanOrEqual(parts.getLeft(), parts.getRight());
      }
    } else if (expr instanceof MinorThan minorThanExpr) {
      // Handle less than expressions
      Pair<Boolean, Pair<String, Object>> result =
          getOperandsForComparison(minorThanExpr, schema, params, alias, joinTableColumns);
      Pair<String, Object> parts = result.getRight();
      if (result.getLeft()) {
        return Expressions.greaterThan(parts.getLeft(), parts.getRight());
      } else {
        return Expressions.lessThan(parts.getLeft(), parts.getRight());
      }
    } else if (expr instanceof MinorThanEquals minorThanEqualsExpr) {
      // Handle less than or equal to expressions
      Pair<Boolean, Pair<String, Object>> result =
          getOperandsForComparison(minorThanEqualsExpr, schema, params, alias, joinTableColumns);
      Pair<String, Object> parts = result.getRight();
      if (result.getLeft()) {
        return Expressions.greaterThanOrEqual(parts.getLeft(), parts.getRight());
      } else {
        return Expressions.lessThanOrEqual(parts.getLeft(), parts.getRight());
      }
    } else if (expr instanceof Between betweenExpr) {
      // Handle BETWEEN expressions
      Pair<Boolean, Pair<String, Object>> startResult =
          getOperands(
              betweenExpr.getLeftExpression(),
              betweenExpr.getBetweenExpressionStart(),
              schema,
              params,
              alias,
              joinTableColumns);
      com.arcesium.swiftlake.expressions.Expression leftSwiftLakeExpr = null;
      Pair<String, Object> startParts = startResult.getRight();
      if (startResult.getLeft()) {
        leftSwiftLakeExpr =
            Expressions.lessThanOrEqual(startParts.getLeft(), startParts.getRight());
      } else {
        leftSwiftLakeExpr =
            Expressions.greaterThanOrEqual(startParts.getLeft(), startParts.getRight());
      }

      Pair<Boolean, Pair<String, Object>> endResult =
          getOperands(
              betweenExpr.getLeftExpression(),
              betweenExpr.getBetweenExpressionEnd(),
              schema,
              params,
              alias,
              joinTableColumns);
      com.arcesium.swiftlake.expressions.Expression rightSwiftLakeExpr = null;
      Pair<String, Object> endParts = endResult.getRight();
      if (endResult.getLeft()) {
        rightSwiftLakeExpr =
            Expressions.greaterThanOrEqual(endParts.getLeft(), endParts.getRight());
      } else {
        rightSwiftLakeExpr = Expressions.lessThanOrEqual(endParts.getLeft(), endParts.getRight());
      }
      var andExpr = Expressions.and(leftSwiftLakeExpr, rightSwiftLakeExpr);
      if (betweenExpr.isNot()) {
        return Expressions.not(andExpr);
      } else {
        return andExpr;
      }
    } else if (expr instanceof InExpression inExpr) {
      // Handle IN expressions
      Pair<Boolean, Pair<String, Object>> result =
          getOperands(
              inExpr.getLeftExpression(),
              inExpr.getRightExpression(),
              schema,
              params,
              alias,
              joinTableColumns);
      ValidationException.check(!result.getLeft(), "Unsupported expression " + expr);
      Pair<String, Object> parts = result.getRight();
      Object[] values =
          parts.getRight() == null ? null : ((List<Object>) parts.getRight()).toArray();
      if (inExpr.isNot()) {
        return Expressions.notIn(parts.getLeft(), values);
      } else {
        return Expressions.in(parts.getLeft(), values);
      }
    } else if (expr instanceof Column col) {
      // Handle boolean expression
      if (isBooleanValue(col)) {
        return getBooleanValue(col) ? Expressions.alwaysTrue() : Expressions.alwaysFalse();
      } else {
        validateColumn(col, schema, alias, joinTableColumns);
        return Expressions.equal(col.getColumnName(), true);
      }
    } else if (expr instanceof JdbcParameter) {
      Boolean value = (Boolean) getIcebergValueForSqlValue(expr, Types.BooleanType.get(), params);
      return Expressions.fromBoolean(value);
    } else if (expr instanceof NullValue) {
      return Expressions.fromBoolean(null);
    } else {
      throw new ValidationException("Unsupported Expression %s", expr);
    }
  }

  /**
   * Resolves a column to either the main table or join table.
   *
   * @param column The column to resolve
   * @param schema The schema of the main table
   * @param alias The alias of the main table
   * @param joinTableAlias The alias of the join table
   * @param joinTableColumns Set of columns from the join table
   * @return A pair of resolved columns (main table column, join table column)
   */
  private Pair<Column, Column> resolveColumn(
      Column column,
      Schema schema,
      String alias,
      String joinTableAlias,
      Set<String> joinTableColumns) {
    if (alias != null && joinTableAlias != null) {
      ValidationException.check(
          !alias.equalsIgnoreCase(joinTableAlias), "Ambiguous table alias '%s'", alias);
    }

    // Two possibilities
    // 1. first part is table alias
    // 2. first part is struct column name
    Pair<String, String> nameParts = getColumnNameParts(column);
    String columnTableAlias = nameParts.getLeft();
    String fullColumnName = nameParts.getRight();

    Boolean isMainTableOrJoinTableColumn = null;
    if (columnTableAlias != null) {
      if (columnTableAlias.equalsIgnoreCase(alias) && schema.findField(fullColumnName) != null) {
        isMainTableOrJoinTableColumn = true;
      } else if (columnTableAlias.equalsIgnoreCase(joinTableAlias)
          && joinTableColumns.contains(fullColumnName)) {
        isMainTableOrJoinTableColumn = false;
      }
    }

    if (isMainTableOrJoinTableColumn == null) {
      if (columnTableAlias != null) {
        fullColumnName = columnTableAlias + "." + fullColumnName;
        columnTableAlias = null;
      }

      Types.NestedField nestedField = schema.findField(fullColumnName);
      if (nestedField != null && joinTableColumns.contains(fullColumnName)) {
        throw new ValidationException("Ambiguous reference to column name '%s'", fullColumnName);
      } else if (nestedField != null) {
        isMainTableOrJoinTableColumn = true;
      } else if (joinTableColumns.contains(fullColumnName)) {
        isMainTableOrJoinTableColumn = false;
      } else {
        throw new ValidationException("Column does not exist '%s'", fullColumnName);
      }
    }

    column.setTable(columnTableAlias == null ? null : new Table(columnTableAlias));
    column.setColumnName(fullColumnName);

    return Pair.of(
        isMainTableOrJoinTableColumn ? column : null, isMainTableOrJoinTableColumn ? null : column);
  }

  private Pair<Boolean, Pair<String, Object>> getOperandsForComparison(
      ComparisonOperator operator,
      Schema schema,
      Map<Integer, Object> params,
      String alias,
      Set<String> joinTableColumns) {
    return getOperands(
        operator.getLeftExpression(),
        operator.getRightExpression(),
        schema,
        params,
        alias,
        joinTableColumns);
  }

  /**
   * Extracts operands from a condition.
   *
   * @param left The left expression
   * @param right The right expression
   * @param schema The schema of the table
   * @param params Map of parameter indices to their values
   * @param alias The alias of the main table
   * @param joinTableColumns Set of columns from the join table
   * @return A pair containing the order changed flag and a pair of column name and its value
   */
  private Pair<Boolean, Pair<String, Object>> getOperands(
      Expression left,
      Expression right,
      Schema schema,
      Map<Integer, Object> params,
      String alias,
      Set<String> joinTableColumns) {
    Column column = null;
    Expression valueExpr = null;
    boolean orderChanged = false;
    left = unwrapParentheses(left);
    right = unwrapParentheses(right);

    // Determine which expression is the column and which is the value
    if (left instanceof Column leftCol) {
      if (!isBooleanValue(leftCol)) {
        column = leftCol;
        valueExpr = right;
      } else {
        valueExpr = left;
        orderChanged = true;
      }
    }

    if (right instanceof Column rightCol) {
      if (!isBooleanValue(rightCol)) {
        column = rightCol;
        valueExpr = left;
        orderChanged = true;
      } else {
        valueExpr = right;
      }
    }

    Types.NestedField nestedField = validateColumn(column, schema, alias, joinTableColumns);
    Type dataType = nestedField.type();

    // Handle list of values or single value
    if (valueExpr instanceof ExpressionList) {
      return Pair.of(
          orderChanged,
          Pair.of(
              column.getColumnName(),
              ((ExpressionList<?>) valueExpr)
                  .stream()
                      .map(e -> getIcebergValueForSqlValue(e, dataType, params))
                      .collect(Collectors.toList())));
    } else {
      Object value = getIcebergValueForSqlValue(valueExpr, dataType, params);
      return Pair.of(orderChanged, Pair.of(column.getColumnName(), value));
    }
  }

  private Expression unwrapParentheses(Expression expression) {
    if (expression == null) {
      return null;
    }
    while (expression instanceof Parenthesis parenthesis) {
      expression = parenthesis.getExpression();
    }
    return expression;
  }

  /**
   * Converts a SQL value to its corresponding Iceberg value.
   *
   * @param expr The SQL expression
   * @param dataType The Iceberg data type
   * @param params Map of parameter indices to their values
   * @return The converted Iceberg value
   */
  private Object getIcebergValueForSqlValue(
      Expression expr, Type dataType, Map<Integer, Object> params) {
    ValidationException.check(dataType.isPrimitiveType(), "Data type should be a primitive type");
    ValidationException.check(expr != null && !isColumn(expr), "Invalid value %s", expr);
    try {
      Object paramValue = null;
      boolean isJdbcParam = false;
      if (expr instanceof JdbcParameter jdbcParameter) {
        isJdbcParam = true;
        int index = jdbcParameter.getIndex();
        if (params == null || !params.containsKey(index)) {
          throw new ValidationException(
              "Invalid parameter index %s provided for prepared statement", index);
        }
        paramValue = params.get(index);
      }

      if (expr instanceof NullValue || (isJdbcParam && paramValue == null)) {
        return null;
      }

      Type.TypeID typeId = dataType.typeId();
      if (typeId == Type.TypeID.STRING) {
        if (isJdbcParam) {
          ValidationException.check(
              paramValue instanceof String, "Invalid string value %s", paramValue);
          return paramValue;
        } else {
          ValidationException.check(expr instanceof StringValue, "Invalid string value %s", expr);
          return ((StringValue) expr).getNotExcapedValue();
        }
      } else if (typeId == Type.TypeID.BOOLEAN) {
        if (isJdbcParam) {
          ValidationException.check(
              paramValue instanceof Boolean, "Invalid boolean value %s", paramValue);
          return paramValue;
        } else {
          ValidationException.check(expr instanceof Column, "Invalid boolean value %s", expr);
          ValidationException.check(
              isBooleanValue((Column) expr), "Invalid boolean value %s", expr);
          return getBooleanValue((Column) expr);
        }

      } else if (typeId == Type.TypeID.INTEGER) {
        if (isJdbcParam) {
          ValidationException.check(
              paramValue instanceof Integer, "Invalid integer value %s", paramValue);
          return paramValue;
        } else {
          return Integer.valueOf(
              getLongValueAsString(
                  expr,
                  String.format("Invalid integer value %s", expr),
                  Set.of("INT", "INTEGER", "INT32", "INT4")));
        }
      } else if (typeId == Type.TypeID.LONG) {
        if (isJdbcParam) {
          ValidationException.check(
              paramValue instanceof Integer || paramValue instanceof Long,
              "Invalid long value %s",
              paramValue);
          if (paramValue instanceof Integer) {
            paramValue = Long.valueOf((Integer) paramValue);
          }
          return paramValue;
        } else {
          return Long.valueOf(
              getLongValueAsString(
                  expr,
                  String.format("Invalid long value %s", expr),
                  Set.of("LONG", "BIGINT", "INT64", "INT8")));
        }
      } else if (typeId == Type.TypeID.FLOAT) {
        if (isJdbcParam) {
          return getFloatValue(paramValue, String.format("Invalid float value %s", paramValue));
        } else {
          if (expr instanceof StringValue stringValue) {
            if (stringValue.getValue().equalsIgnoreCase("inf")
                || stringValue.getValue().equalsIgnoreCase("infinity")
                || stringValue.getValue().equalsIgnoreCase("+inf")
                || stringValue.getValue().equalsIgnoreCase("+infinity")) {
              return Float.POSITIVE_INFINITY;
            }
            if (stringValue.getValue().equalsIgnoreCase("-inf")
                || stringValue.getValue().equalsIgnoreCase("-infinity")) {
              return Float.NEGATIVE_INFINITY;
            }
            if (stringValue.getValue().equalsIgnoreCase("nan")) {
              return Float.NaN;
            }
          }
          return Float.valueOf(
              getDoubleValueAsString(
                  expr,
                  String.format("Invalid float value %s", expr),
                  Set.of("FLOAT", "FLOAT4", "REAL")));
        }
      } else if (typeId == Type.TypeID.DOUBLE) {
        if (isJdbcParam) {
          return getDoubleValue(paramValue, String.format("Invalid double value %s", paramValue));
        } else {
          if (expr instanceof StringValue stringValue) {
            if (stringValue.getValue().equalsIgnoreCase("inf")
                || stringValue.getValue().equalsIgnoreCase("infinity")
                || stringValue.getValue().equalsIgnoreCase("+inf")
                || stringValue.getValue().equalsIgnoreCase("+infinity")) {
              return Double.POSITIVE_INFINITY;
            }
            if (stringValue.getValue().equalsIgnoreCase("-inf")
                || stringValue.getValue().equalsIgnoreCase("-infinity")) {
              return Double.NEGATIVE_INFINITY;
            }
            if (stringValue.getValue().equalsIgnoreCase("nan")) {
              return Double.NaN;
            }
          }
          return Double.valueOf(
              getDoubleValueAsString(
                  expr,
                  String.format("Invalid double value %s", expr),
                  Set.of("DOUBLE", "FLOAT8")));
        }
      } else if (typeId == Type.TypeID.DECIMAL) {
        if (isJdbcParam) {
          return getBigDecimalValue(
              paramValue, String.format("Invalid decimal value %s", paramValue));
        } else {
          return new BigDecimal(
              getDoubleValueAsString(expr, String.format("Invalid decimal value %s", expr), null));
        }
      } else if (typeId == Type.TypeID.DATE) {
        if (isJdbcParam) {
          ValidationException.check(
              paramValue instanceof LocalDate
                  || paramValue instanceof Date
                  || paramValue instanceof java.sql.Date,
              "Invalid date value %s",
              paramValue);
          LocalDate localDate = null;
          if (paramValue instanceof java.sql.Date sqlDate) {
            localDate = sqlDate.toLocalDate();
          } else if (paramValue instanceof Date date) {
            LocalDateTime localDateTime =
                LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
            ValidationException.check(
                localDateTime.getHour() == 0
                    && localDateTime.getMinute() == 0
                    && localDateTime.getSecond() == 0
                    && localDateTime.getNano() == 0,
                "Invalid date value %s",
                paramValue);
            localDate = localDateTime.toLocalDate();
          } else {
            localDate = (LocalDate) paramValue;
          }

          return DateTimeUtil.formatLocalDate(localDate);
        }

        String value = getDateTimeLiteralString(expr);
        DateTimeUtil.parseLocalDate(value);
        return value;
      } else if (typeId == Type.TypeID.TIME) {
        if (isJdbcParam) {
          ValidationException.check(
              paramValue instanceof SwiftLakeDuckDBTime || paramValue instanceof LocalTime,
              "Invalid time value %s",
              paramValue);
          LocalTime localTime = null;
          if (paramValue instanceof SwiftLakeDuckDBTime time) {
            localTime = time.toLocalDateTime().toLocalTime();
          } else {
            localTime = (LocalTime) paramValue;
          }

          return DateTimeUtil.formatLocalTimeWithMicros(localTime);
        }

        String value = getDateTimeLiteralString(expr);
        return DateTimeUtil.formatLocalTimeWithMicros(DateTimeUtil.parseLocalTimeToMicros(value));
      } else if (typeId == Type.TypeID.TIMESTAMP) {
        if (((Types.TimestampType) dataType).shouldAdjustToUTC()) {
          if (isJdbcParam) {
            ValidationException.check(
                paramValue instanceof OffsetDateTime || paramValue instanceof Date,
                "Invalid timestamptz value %s",
                paramValue);
            OffsetDateTime offsetDateTime = null;
            if (paramValue instanceof Date date) {
              offsetDateTime = OffsetDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
            } else {
              offsetDateTime = (OffsetDateTime) paramValue;
            }

            return DateTimeUtil.formatOffsetDateTimeWithMicros(offsetDateTime);
          }

          String value = getDateTimeLiteralString(expr);
          return DateTimeUtil.formatOffsetDateTimeWithMicros(
              DateTimeUtil.parseOffsetDateTimeToMicros(value));
        } else {
          if (isJdbcParam) {
            ValidationException.check(
                paramValue instanceof LocalDateTime
                    || paramValue instanceof Date
                    || paramValue instanceof java.sql.Timestamp,
                "Invalid timestamp value %s",
                paramValue);
            LocalDateTime localDateTime = null;
            if (paramValue instanceof java.sql.Timestamp timestamp) {
              localDateTime = timestamp.toLocalDateTime();
            } else if (paramValue instanceof Date date) {
              localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
            } else {
              localDateTime = (LocalDateTime) paramValue;
            }

            return DateTimeUtil.formatLocalDateTimeWithMicros(localDateTime);
          }

          String value = getDateTimeLiteralString(expr);
          return DateTimeUtil.formatLocalDateTimeWithMicros(
              DateTimeUtil.parseLocalDateTimeToMicros(value));
        }
      }
    } catch (ValidationException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new SwiftLakeException(
          e, "An error occurred while parsing value %s for data type %s", expr, dataType);
    }

    throw new ValidationException("Unsupported value %s with type %s", expr, dataType);
  }

  private String getLongValueAsString(
      Expression expr, String errorMsg, Set<String> allowedCastDataTypes) {
    // Extract sign and validate casts
    Pair<Character, Expression> result = extractSignAndValidateCast(expr, allowedCastDataTypes);
    Character sign = result.getLeft();
    expr = result.getRight();
    // Validate that we have a numeric literal, either directly or inside a SignedExpression
    // This is necessary because SignedExpression might appear after cast extraction
    ValidationException.check(
        expr instanceof LongValue
            || (expr instanceof SignedExpression signedExpression
                && signedExpression.getExpression() instanceof LongValue),
        errorMsg);
    // Construct the final string with sign if present
    return (sign == null ? "" : sign) + expr.toString();
  }

  private String getDoubleValueAsString(
      Expression expr, String errorMsg, Set<String> allowedCastDataTypes) {
    // Extract sign and validate casts
    Pair<Character, Expression> result = extractSignAndValidateCast(expr, allowedCastDataTypes);
    Character sign = result.getLeft();
    expr = result.getRight();

    // Validate that we have a numeric literal, either directly or inside a SignedExpression
    // This is necessary because SignedExpression might appear after cast extraction
    ValidationException.check(
        expr instanceof DoubleValue
            || expr instanceof LongValue
            || (expr instanceof SignedExpression signedExpression
                && (signedExpression.getExpression() instanceof DoubleValue
                    || signedExpression.getExpression() instanceof LongValue)),
        errorMsg);
    // Construct the final string with sign if present
    return (sign == null ? "" : sign) + expr.toString();
  }

  private Pair<Character, Expression> extractSignAndValidateCast(
      Expression expr, Set<String> allowedCastDataTypes) {
    // Extract top-level sign if present
    Character sign = null;
    if (expr instanceof SignedExpression signedExpression) {
      sign = signedExpression.getSign();
      expr = signedExpression.getExpression();
    }

    // Handle and validate casts if restrictions exist
    if (allowedCastDataTypes != null && !allowedCastDataTypes.isEmpty()) {
      if (expr instanceof CastExpression castExpression) {
        String dataType = castExpression.getColDataType().getDataType().toUpperCase(Locale.ROOT);
        ValidationException.check(
            allowedCastDataTypes.contains(dataType),
            "Unsupported cast type: '%s'. Only %s casting is allowed here.",
            dataType,
            String.join(", ", allowedCastDataTypes));
        expr = castExpression.getLeftExpression();
      }
    }

    return Pair.of(sign, expr);
  }

  private Object getFloatValue(Object input, String errorMsg) {
    ValidationException.check(
        input instanceof Integer || input instanceof Long || input instanceof Float, errorMsg);
    if (input instanceof Integer integer) {
      return Float.valueOf(integer);
    }

    if (input instanceof Long longValue) {
      return Float.valueOf(longValue);
    }

    return input;
  }

  private Object getDoubleValue(Object input, String errorMsg) {
    ValidationException.check(
        input instanceof Integer
            || input instanceof Long
            || input instanceof Float
            || input instanceof Double,
        errorMsg);
    if (input instanceof Integer integer) {
      return Double.valueOf(integer);
    }

    if (input instanceof Long longValue) {
      return Double.valueOf(longValue);
    }

    if (input instanceof Float floatValue) {
      return Double.valueOf(floatValue);
    }

    return input;
  }

  private Object getBigDecimalValue(Object input, String errorMsg) {
    ValidationException.check(
        input instanceof Integer
            || input instanceof Long
            || input instanceof Float
            || input instanceof Double
            || input instanceof BigDecimal,
        errorMsg);
    if (input instanceof Integer integer) {
      return BigDecimal.valueOf(integer);
    }

    if (input instanceof Long longValue) {
      return BigDecimal.valueOf(longValue);
    }

    if (input instanceof Float floatValue) {
      return BigDecimal.valueOf(floatValue);
    }

    if (input instanceof Double doubleValue) {
      return BigDecimal.valueOf(doubleValue);
    }

    return input;
  }

  private String getDateTimeLiteralString(Expression expr) {
    ValidationException.check(
        expr instanceof DateTimeLiteralExpression || expr instanceof StringValue,
        "Invalid type %s",
        expr);

    if (expr instanceof DateTimeLiteralExpression dateTimeExpr) {
      String value = dateTimeExpr.getValue();

      if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
        value = value.substring(1, value.length() - 1);
      }
      return value;
    } else {
      return ((StringValue) expr).getValue();
    }
  }

  private boolean getBooleanValue(Column column) {
    if (column.getColumnName().equalsIgnoreCase("true")) {
      return true;
    }
    if (column.getColumnName().equalsIgnoreCase("false")) {
      return false;
    }
    throw new ValidationException("Invalid boolean value %s", column.getColumnName());
  }

  private boolean isBooleanValue(Column column) {
    return column.getTable() == null
        && (column.getColumnName().equalsIgnoreCase("true")
            || column.getColumnName().equalsIgnoreCase("false"));
  }

  private boolean isColumn(Expression expr) {
    return expr instanceof Column column && !isBooleanValue(column);
  }

  private Types.NestedField validateColumn(
      Column column, Schema schema, String alias, Set<String> joinTableColumns) {
    if (column == null) {
      throw new ValidationException(
          "For comparison one side must be a column name and other side must be a literal value");
    } else if (column.getArrayConstructor() != null) {
      throw new ValidationException(
          "Conditions on List and Map types are not supported in table-level filtering. Please keep these conditions out of the table filter.");
    }

    // Two possibilities
    // 1. first part is table alias
    // 2. first part is struct column name
    Pair<String, String> nameParts = getColumnNameParts(column);
    String columnTableAlias = nameParts.getLeft();
    String fullColumnName = nameParts.getRight();
    Types.NestedField columnField = null;
    if (columnTableAlias != null && columnTableAlias.equalsIgnoreCase(alias)) {
      columnField = schema.findField(fullColumnName);
    }
    if (columnField == null) {
      if (columnTableAlias != null) {
        fullColumnName = columnTableAlias + "." + fullColumnName;
        columnTableAlias = null;
      }
      columnField = schema.findField(fullColumnName);
      ValidationException.checkNotNull(columnField, "Column does not exist %s", fullColumnName);
    }

    if (columnTableAlias == null && joinTableColumns != null) {
      // verify column conflicts
      ValidationException.check(
          !joinTableColumns.contains(fullColumnName),
          "Ambiguous reference to column name '%s'",
          fullColumnName);
    }
    column.setTable(columnTableAlias == null ? null : new Table(columnTableAlias));
    column.setColumnName(fullColumnName);
    return columnField;
  }

  private Pair<String, String> getColumnNameParts(Column column) {
    String tableAlias = null;
    StringBuilder columnName = new StringBuilder();
    if (column.getTable() != null
        && column.getTable().getNameParts() != null
        && !column.getTable().getNameParts().isEmpty()) {
      List<String> tableNameParts = column.getTable().getNameParts();
      for (int i = tableNameParts.size() - 1; i >= 0; i--) {
        String part = tableNameParts.get(i);
        if (part == null) {
          part = "";
        }
        if (i == tableNameParts.size() - 1) {
          tableAlias = part;
        } else {
          columnName.append(part);
          columnName.append(".");
        }
      }
    }
    columnName.append(column.getColumnName());
    return Pair.of(tableAlias, columnName.toString());
  }

  public void parameterizeJoinCondition(
      Expression expr,
      Schema schema,
      String alias,
      String joinTableAlias,
      Set<String> joinTableColumns,
      Map<Integer, String> outParamNames,
      int paramIndexStart,
      Consumer<Expression> expressionConsumer) {
    if (expr instanceof Parenthesis parenthesis) {
      parameterizeJoinCondition(
          parenthesis.getExpression(),
          schema,
          alias,
          joinTableAlias,
          joinTableColumns,
          outParamNames,
          paramIndexStart,
          parenthesis::setExpression);
    } else if (expr instanceof AndExpression andExpr) {
      parameterizeJoinCondition(
          andExpr.getLeftExpression(),
          schema,
          alias,
          joinTableAlias,
          joinTableColumns,
          outParamNames,
          paramIndexStart,
          andExpr::setLeftExpression);
      parameterizeJoinCondition(
          andExpr.getRightExpression(),
          schema,
          alias,
          joinTableAlias,
          joinTableColumns,
          outParamNames,
          paramIndexStart,
          andExpr::setRightExpression);
    } else if (expr instanceof OrExpression orExpr) {
      parameterizeJoinCondition(
          orExpr.getLeftExpression(),
          schema,
          alias,
          joinTableAlias,
          joinTableColumns,
          outParamNames,
          paramIndexStart,
          orExpr::setLeftExpression);
      parameterizeJoinCondition(
          orExpr.getRightExpression(),
          schema,
          alias,
          joinTableAlias,
          joinTableColumns,
          outParamNames,
          paramIndexStart,
          orExpr::setRightExpression);
    } else if (expr instanceof NotExpression notExpr) {
      parameterizeJoinCondition(
          notExpr.getExpression(),
          schema,
          alias,
          joinTableAlias,
          joinTableColumns,
          outParamNames,
          paramIndexStart,
          notExpr::setExpression);
    } else if (expr instanceof IsNullExpression isNullExpr) {
      if (isColumn(isNullExpr.getLeftExpression())) {
        processOperandsForJoinCondition(
            isNullExpr.getLeftExpression(),
            null,
            schema,
            alias,
            joinTableAlias,
            joinTableColumns,
            outParamNames,
            paramIndexStart,
            isNullExpr::setLeftExpression,
            null);
      } else {
        throw new ValidationException("Unsupported IsNullExpression %s", isNullExpr);
      }
    } else if (expr instanceof ComparisonOperator || expr instanceof IsDistinctExpression) {
      BinaryExpression binaryExpr = (BinaryExpression) expr;
      processOperandsForJoinCondition(
          binaryExpr.getLeftExpression(),
          binaryExpr.getRightExpression(),
          schema,
          alias,
          joinTableAlias,
          joinTableColumns,
          outParamNames,
          paramIndexStart,
          binaryExpr::setLeftExpression,
          binaryExpr::setRightExpression);
    } else if (expr instanceof Between betweenExpr) {
      Pair<Column, Column> startPartColumns =
          processOperandsForJoinCondition(
              betweenExpr.getLeftExpression(),
              betweenExpr.getBetweenExpressionStart(),
              schema,
              alias,
              joinTableAlias,
              joinTableColumns,
              outParamNames,
              paramIndexStart,
              betweenExpr::setLeftExpression,
              betweenExpr::setBetweenExpressionStart);
      // Comparing refs
      ValidationException.check(
          startPartColumns.getLeft() != null
              && startPartColumns.getLeft() == betweenExpr.getLeftExpression(),
          "Left side expression in the between operator must be a column of the table mentioned in from clause. %s",
          betweenExpr.getLeftExpression());

      processOperandsForJoinCondition(
          betweenExpr.getLeftExpression(),
          betweenExpr.getBetweenExpressionEnd(),
          schema,
          alias,
          joinTableAlias,
          joinTableColumns,
          outParamNames,
          paramIndexStart,
          betweenExpr::setLeftExpression,
          betweenExpr::setBetweenExpressionEnd);

    } else if (expr instanceof InExpression inExpr) {
      processOperandsForJoinCondition(
          inExpr.getLeftExpression(),
          inExpr.getRightExpression(),
          schema,
          alias,
          joinTableAlias,
          joinTableColumns,
          outParamNames,
          paramIndexStart,
          inExpr::setLeftExpression,
          inExpr::setRightExpression);
    } else if (expr instanceof Column column) {
      if (!isBooleanValue(column)) {
        processOperandsForJoinCondition(
            expr,
            null,
            schema,
            alias,
            joinTableAlias,
            joinTableColumns,
            outParamNames,
            paramIndexStart,
            expressionConsumer,
            null);
      }
    }
  }

  private Pair<Column, Column> processOperandsForJoinCondition(
      Expression left,
      Expression right,
      Schema schema,
      String alias,
      String joinTableAlias,
      Set<String> joinTableColumns,
      Map<Integer, String> outParamNames,
      int paramIndexStart,
      Consumer<Expression> leftExpressionConsumer,
      Consumer<Expression> rightExpressionConsumer) {
    Pair<Column, Column> resolvedForLeft = null;
    Pair<Column, Column> resolvedForRight = null;
    Consumer<Expression> joinColumnExpressionConsumer = null;
    left = unwrapParentheses(left);
    right = unwrapParentheses(right);

    boolean isLeftJoinColumnJdbcParameter =
        left instanceof JdbcParameter jdbcParameter
            && outParamNames.containsKey(jdbcParameter.getIndex());
    if (isColumn(left) || isLeftJoinColumnJdbcParameter) {
      if (isLeftJoinColumnJdbcParameter) {
        resolvedForLeft =
            Pair.of(
                null,
                new Column(outParamNames.get(((JdbcParameter) left).getIndex()))
                    .withTable(joinTableAlias != null ? new Table(joinTableAlias) : null));
      } else {
        resolvedForLeft =
            resolveColumn((Column) left, schema, alias, joinTableAlias, joinTableColumns);
      }
    }

    boolean isRightJoinColumnJdbcParameter =
        right instanceof JdbcParameter jdbcParameter
            && outParamNames.containsKey(jdbcParameter.getIndex());
    if (right != null) {
      if (isColumn(right) || isRightJoinColumnJdbcParameter) {
        if (isRightJoinColumnJdbcParameter) {
          resolvedForRight =
              Pair.of(
                  null,
                  new Column(outParamNames.get(((JdbcParameter) right).getIndex()))
                      .withTable(joinTableAlias != null ? new Table(joinTableAlias) : null));
        } else {
          resolvedForRight =
              resolveColumn((Column) right, schema, alias, joinTableAlias, joinTableColumns);
        }
      }
    }

    if (resolvedForLeft != null
        && resolvedForLeft.getLeft() != null
        && resolvedForRight != null
        && resolvedForRight.getLeft() != null) {
      throw new ValidationException(
          "Both the columns %s and %s belong to same table.",
          resolvedForLeft.getLeft(), resolvedForRight.getLeft());
    }

    if (resolvedForLeft != null
        && resolvedForLeft.getRight() != null
        && resolvedForRight != null
        && resolvedForRight.getRight() != null) {
      throw new ValidationException(
          "Both the columns %s and %s belong to same table.",
          resolvedForLeft.getRight(), resolvedForRight.getRight());
    }

    Column column = null;
    Column joinTableColumn = null;
    boolean isJoinColumnJdbcParameter = false;

    if (resolvedForLeft != null && resolvedForLeft.getLeft() != null) {
      column = resolvedForLeft.getLeft();
    } else if (resolvedForRight != null) {
      column = resolvedForRight.getLeft();
    }

    if (resolvedForLeft != null && resolvedForLeft.getRight() != null) {
      joinTableColumn = resolvedForLeft.getRight();
      joinColumnExpressionConsumer = leftExpressionConsumer;
      isJoinColumnJdbcParameter = isLeftJoinColumnJdbcParameter;
    } else if (resolvedForRight != null) {
      joinTableColumn = resolvedForRight.getRight();
      if (joinTableColumn != null) {
        joinColumnExpressionConsumer = rightExpressionConsumer;
        isJoinColumnJdbcParameter = isRightJoinColumnJdbcParameter;
      }
    }

    if (column == null) {
      throw new ValidationException("Column from the table is mandatory.");
    }

    if (!isJoinColumnJdbcParameter && joinTableColumn != null) {
      int paramIndex = paramIndexStart + outParamNames.size();
      outParamNames.put(paramIndex, joinTableColumn.getColumnName());
      var jdbcParameter = new JdbcParameter();
      jdbcParameter.withUseFixedIndex(true).withIndex(paramIndex);
      joinColumnExpressionConsumer.accept(jdbcParameter);
    }

    return Pair.of(column, joinTableColumn);
  }
}
