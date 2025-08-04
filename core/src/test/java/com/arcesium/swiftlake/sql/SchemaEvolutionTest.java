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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.TestUtil;
import com.arcesium.swiftlake.common.ParquetUtil;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.expressions.Expressions;
import com.arcesium.swiftlake.io.SwiftLakeFileIO;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchemaEvolutionTest {
  @Mock private Table mockTable;
  @Mock private SwiftLakeFileIO mockFileIO;
  @Mock private InputFile mockInputFile;
  @Mock private ParquetFileReader mockParquetFileReader;
  @Mock private FileMetaData mockFileMetaData;
  @Mock private MessageType mockMessageType;
  private SchemaEvolution schemaEvolution;
  private Schema schema;

  @BeforeEach
  void setUp() {
    schemaEvolution = new SchemaEvolution();
    schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(
                4,
                "nested",
                Types.StructType.of(
                    Types.NestedField.required(5, "value", Types.IntegerType.get()))),
            Types.NestedField.optional(
                6, "list", Types.ListType.ofOptional(7, Types.StringType.get())),
            Types.NestedField.optional(
                8,
                "map",
                Types.MapType.ofOptional(9, 10, Types.StringType.get(), Types.IntegerType.get())));
  }

  @Test
  void testGetProjectionExprWithTypeCasting() {
    List<Types.NestedField> fields = schema.columns();
    List<String> columns = Arrays.asList("id", "name");

    String result = schemaEvolution.getProjectionExprWithTypeCasting(fields, columns);

    assertThat(result)
        .isEqualTo(
            "id::LONG AS id, name::STRING AS name, NULL::INTEGER AS age, NULL::STRUCT(value INTEGER) AS nested, NULL::STRING[] AS list, "
                + "NULL::MAP(STRING, INTEGER) AS map");
  }

  @ParameterizedTest
  @MethodSource("provideTypesForTypeExpr")
  void testGetTypeExpr(Type type, String expected) {
    assertThat(schemaEvolution.getTypeExpr(type)).isEqualTo(expected);
  }

  private static Stream<Arguments> provideTypesForTypeExpr() {
    return Stream.of(
        Arguments.of(Types.LongType.get(), "LONG"),
        Arguments.of(Types.StringType.get(), "STRING"),
        Arguments.of(Types.IntegerType.get(), "INTEGER"),
        Arguments.of(Types.BooleanType.get(), "BOOLEAN"),
        Arguments.of(Types.FloatType.get(), "FLOAT"),
        Arguments.of(Types.DoubleType.get(), "DOUBLE"),
        Arguments.of(Types.DateType.get(), "DATE"),
        Arguments.of(Types.TimeType.get(), "TIME"),
        Arguments.of(Types.TimestampType.withoutZone(), "TIMESTAMP"),
        Arguments.of(Types.TimestampType.withZone(), "TIMESTAMPTZ"),
        Arguments.of(Types.UUIDType.get(), "UUID"),
        Arguments.of(Types.BinaryType.get(), "BINARY"),
        Arguments.of(Types.DecimalType.of(10, 2), "DECIMAL(10,2)"),
        Arguments.of(
            Types.StructType.of(Types.NestedField.required(1, "field", Types.StringType.get())),
            "STRUCT(field STRING)"),
        Arguments.of(Types.ListType.ofRequired(1, Types.IntegerType.get()), "INTEGER[]"),
        Arguments.of(
            Types.MapType.ofRequired(1, 2, Types.StringType.get(), Types.IntegerType.get()),
            "MAP(STRING, INTEGER)"));
  }

  @ParameterizedTest
  @MethodSource("providePrimitiveTypeValuesForSql")
  void testGetPrimitiveTypeValueForSql(Type.PrimitiveType type, Object value, String expected) {
    assertThat(schemaEvolution.getPrimitiveTypeValueForSql(type, value)).isEqualTo(expected);
  }

  private static Stream<Arguments> providePrimitiveTypeValuesForSql() {
    return Stream.of(
        Arguments.of(Types.LongType.get(), 42L, "(42)::LONG"),
        Arguments.of(Types.StringType.get(), "test", "('test')::STRING"),
        Arguments.of(Types.BooleanType.get(), true, "(true)::BOOLEAN"),
        Arguments.of(Types.IntegerType.get(), null, "(NULL)::INTEGER"),
        Arguments.of(Types.DateType.get(), LocalDate.of(2023, 5, 1), "('2023-05-01')::DATE"),
        Arguments.of(Types.TimeType.get(), LocalTime.of(12, 30), "('12:30:00')::TIME"),
        Arguments.of(
            Types.TimestampType.withoutZone(),
            LocalDateTime.of(2023, 5, 1, 12, 30),
            "('2023-05-01T12:30:00')::TIMESTAMP"),
        Arguments.of(
            Types.TimestampType.withZone(),
            OffsetDateTime.parse("2023-05-01T12:30:00+01:00"),
            "('2023-05-01T12:30:00+01:00')::TIMESTAMPTZ"),
        Arguments.of(
            Types.DecimalType.of(10, 2), new BigDecimal("123.45"), "(123.45)::DECIMAL(10,2)"));
  }

  @Test
  void testCreateTypeForComparison() {
    Types.StructType originalType =
        Types.StructType.of(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(
                3,
                "nested",
                Types.StructType.of(
                    Types.NestedField.required(4, "value", Types.IntegerType.get()))),
            Types.NestedField.required(
                5, "list", Types.ListType.ofRequired(6, Types.StringType.get())),
            Types.NestedField.required(
                7,
                "map",
                Types.MapType.ofRequired(8, 9, Types.StringType.get(), Types.IntegerType.get())));

    Type result = schemaEvolution.createTypeForComparison(originalType);

    assertThat(result).isInstanceOf(Types.StructType.class);
    Types.StructType resultStruct = (Types.StructType) result;
    assertThat(resultStruct.fields()).hasSize(5);
    for (Types.NestedField field : resultStruct.fields()) {
      assertThat(field.isOptional()).isTrue();
    }
    assertThat(resultStruct.field("nested").type()).isInstanceOf(Types.StructType.class);
    assertThat(resultStruct.field("nested").type().asStructType().field("value").isOptional())
        .isTrue();
    assertThat(resultStruct.field("list").type()).isInstanceOf(Types.ListType.class);
    assertThat(resultStruct.field("list").type().asListType().isElementOptional()).isTrue();
    assertThat(resultStruct.field("map").type()).isInstanceOf(Types.MapType.class);
    assertThat(resultStruct.field("map").type().asMapType().isValueOptional()).isTrue();
  }

  @Test
  void testGetProjectionListWithTypeCasting() {
    List<Types.NestedField> fields = schema.columns();
    List<String> columns = Arrays.asList("id", "name");

    List<String> result = schemaEvolution.getProjectionListWithTypeCasting(fields, columns);

    assertThat(result)
        .containsExactly(
            "id::LONG AS id",
            "name::STRING AS name",
            "NULL::INTEGER AS age",
            "NULL::STRUCT(value INTEGER) AS nested",
            "NULL::STRING[] AS list",
            "NULL::MAP(STRING, INTEGER) AS map");
  }

  @Test
  void testGetProjectionExprForFieldWithTypeChange() {
    Types.NestedField firstField = Types.NestedField.required(1, "field", Types.LongType.get());
    Types.NestedField secondField = Types.NestedField.required(1, "field", Types.IntegerType.get());
    StringBuilder result = new StringBuilder();
    invokeGetProjectionExprForField(result, firstField, secondField, "", 1);

    assertThat(result.toString()).isEqualTo("field::LONG");
  }

  @Test
  void testGetProjectionExprForFieldWithMissingField() {
    Types.NestedField firstField = Types.NestedField.required(1, "field", Types.LongType.get());
    StringBuilder result = new StringBuilder();
    invokeGetProjectionExprForField(result, firstField, null, "", 1);

    assertThat(result.toString()).isEqualTo("NULL::LONG");
  }

  @Test
  void testGetProjectionExprForNestedTypeStruct() {
    Types.StructType firstType =
        Types.StructType.of(
            Types.NestedField.required(3, "date", Types.DateType.get()),
            Types.NestedField.required(1, "long", Types.LongType.get()),
            Types.NestedField.required(4, "string", Types.StringType.get()));
    Types.StructType secondType =
        Types.StructType.of(
            Types.NestedField.required(1, "integer", Types.IntegerType.get()),
            Types.NestedField.required(2, "string", Types.StringType.get()),
            Types.NestedField.required(3, "date", Types.DateType.get()));
    StringBuilder result = new StringBuilder();
    invokeGetProjectionExprForNestedType(result, firstType, secondType, "parent", 1);

    assertThat(result.toString())
        .isEqualTo("{date:parent.date,long:parent.integer::LONG,string:NULL::STRING}");
  }

  @Test
  void testGetProjectionExprForNestedTypeList() {
    Types.ListType firstType = Types.ListType.ofRequired(1, Types.LongType.get());
    Types.ListType secondType = Types.ListType.ofRequired(1, Types.IntegerType.get());
    StringBuilder result = new StringBuilder();
    invokeGetProjectionExprForNestedType(result, firstType, secondType, "parent", 1);

    assertThat(result.toString()).isEqualTo("[__v1__::LONG for __v1__ in parent]");
  }

  @Test
  void testGetProjectionExprForNestedTypeMap() {
    Types.MapType firstType =
        Types.MapType.ofRequired(1, 2, Types.StringType.get(), Types.LongType.get());
    Types.MapType secondType =
        Types.MapType.ofRequired(1, 2, Types.StringType.get(), Types.IntegerType.get());
    StringBuilder result = new StringBuilder();
    invokeGetProjectionExprForNestedType(result, firstType, secondType, "parent", 1);

    assertThat(result.toString())
        .isEqualTo(
            "map_from_entries([{key: __v1__.key, value: __v1__.value::LONG} for __v1__ in map_entries(parent)])");
  }

  // Helper method to invoke private method getProjectionExprForField
  private Boolean invokeGetProjectionExprForField(
      StringBuilder projection,
      Types.NestedField firstField,
      Types.NestedField secondField,
      String secondFieldParent,
      int depth) {
    try {
      java.lang.reflect.Method method =
          SchemaEvolution.class.getDeclaredMethod(
              "getProjectionExprForField",
              StringBuilder.class,
              Types.NestedField.class,
              Types.NestedField.class,
              String.class,
              int.class);
      method.setAccessible(true);
      return (Boolean)
          method.invoke(
              schemaEvolution, projection, firstField, secondField, secondFieldParent, depth);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Helper method to invoke private method getProjectionExprForNestedType
  private void invokeGetProjectionExprForNestedType(
      StringBuilder projection,
      Type.NestedType firstNestedType,
      Type.NestedType secondNestedType,
      String secondTypeParent,
      int depth) {
    try {
      java.lang.reflect.Method method =
          SchemaEvolution.class.getDeclaredMethod(
              "getProjectionExprForNestedType",
              StringBuilder.class,
              Type.NestedType.class,
              Type.NestedType.class,
              String.class,
              int.class);
      method.setAccessible(true);
      method.invoke(
          schemaEvolution, projection, firstNestedType, secondNestedType, secondTypeParent, depth);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testGetTypeExpr() {
    assertThat(schemaEvolution.getTypeExpr(Types.LongType.get())).isEqualTo("LONG");
    assertThat(schemaEvolution.getTypeExpr(Types.StringType.get())).isEqualTo("STRING");
    assertThat(schemaEvolution.getTypeExpr(Types.DateType.get())).isEqualTo("DATE");
    assertThat(schemaEvolution.getTypeExpr(Types.TimestampType.withZone()))
        .isEqualTo("TIMESTAMPTZ");
    assertThat(schemaEvolution.getTypeExpr(Types.DecimalType.of(10, 2))).isEqualTo("DECIMAL(10,2)");
  }

  @Test
  void testGetPrimitiveTypeValueForSql() {
    assertThat(schemaEvolution.getPrimitiveTypeValueForSql(Types.LongType.get(), 123L))
        .isEqualTo("(123)::LONG");
    assertThat(schemaEvolution.getPrimitiveTypeValueForSql(Types.StringType.get(), "test"))
        .isEqualTo("('test')::STRING");
    assertThat(
            schemaEvolution.getPrimitiveTypeValueForSql(
                Types.DateType.get(), LocalDate.of(2023, 1, 1)))
        .isEqualTo("('2023-01-01')::DATE");
  }

  @Test
  void testGetPrimitiveTypeStringValueForSql() {
    assertThat(schemaEvolution.getPrimitiveTypeStringValueForSql(Types.LongType.get(), 123L, false))
        .isEqualTo("123");
    assertThat(schemaEvolution.getPrimitiveTypeStringValueForSql(Types.LongType.get(), 123L, true))
        .isEqualTo("123");
    assertThat(
            schemaEvolution.getPrimitiveTypeStringValueForSql(
                Types.StringType.get(), "test", false))
        .isEqualTo("test");
    assertThat(
            schemaEvolution.getPrimitiveTypeStringValueForSql(Types.StringType.get(), "test", true))
        .isEqualTo("'test'");
    assertThat(
            schemaEvolution.getPrimitiveTypeStringValueForSql(
                Types.DateType.get(), LocalDate.of(2023, 1, 1), false))
        .isEqualTo("2023-01-01");
    assertThat(
            schemaEvolution.getPrimitiveTypeStringValueForSql(
                Types.DateType.get(), LocalDate.of(2023, 1, 1), true))
        .isEqualTo("'2023-01-01'");
  }

  @Test
  void testHandleComplexTypes() {
    Types.StructType complexType =
        Types.StructType.of(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(
                2,
                "nested",
                Types.StructType.of(Types.NestedField.required(3, "name", Types.StringType.get()))),
            Types.NestedField.required(
                4, "list", Types.ListType.ofRequired(5, Types.StringType.get())),
            Types.NestedField.required(
                6,
                "map",
                Types.MapType.ofRequired(7, 8, Types.StringType.get(), Types.IntegerType.get())));

    Type comparisonType = schemaEvolution.createTypeForComparison(complexType);

    assertThat(comparisonType).isInstanceOf(Types.StructType.class);
    Types.StructType structType = (Types.StructType) comparisonType;
    assertThat(structType.fields()).hasSize(4);
    assertThat(structType.field("nested").type()).isInstanceOf(Types.StructType.class);
    assertThat(structType.field("list").type()).isInstanceOf(Types.ListType.class);
    assertThat(structType.field("map").type()).isInstanceOf(Types.MapType.class);
  }

  @Test
  void testGetSelectSQLForDataFiles() {
    when(mockTable.schema()).thenReturn(schema);
    when(mockTable.io()).thenReturn(mockFileIO);

    executeWithMockedStaticParquetUtil(
        schema,
        () -> {
          List<String> dataFiles = Arrays.asList("file1.parquet", "file2.parquet");
          String sql = schemaEvolution.getSelectSQLForDataFiles(mockTable, dataFiles, true, true);
          assertThat(sql)
              .isEqualTo(
                  "(SELECT * FROM read_parquet(['file1.parquet','file2.parquet'], hive_partitioning=0, union_by_name=True, "
                      + "filename=true, file_row_number=true))");
        });
  }

  @Test
  void testGetSelectSQLForDataFilesWithEmptyListAndFileMeta() {
    when(mockTable.schema()).thenReturn(schema);
    String sql = schemaEvolution.getSelectSQLForDataFiles(mockTable, List.of(), true, true);
    assertThat(sql)
        .isEqualTo(
            "(SELECT NULL::LONG AS id, NULL::STRING AS name, NULL::INTEGER AS age, NULL::STRUCT(value INTEGER) AS nested, "
                + "NULL::STRING[] AS list, NULL::MAP(STRING, INTEGER) AS map, NULL::STRING AS filename, NULL::LONG AS file_row_number WHERE FALSE)");
  }

  @Test
  void testGetSelectSQLForDataFilesWithEmptyList() {
    when(mockTable.schema()).thenReturn(schema);
    String sql = schemaEvolution.getSelectSQLForDataFiles(mockTable, List.of(), false, false);
    assertThat(sql)
        .isEqualTo(
            "(SELECT NULL::LONG AS id, NULL::STRING AS name, NULL::INTEGER AS age, NULL::STRUCT(value INTEGER) AS nested, "
                + "NULL::STRING[] AS list, NULL::MAP(STRING, INTEGER) AS map WHERE FALSE)");
  }

  @Test
  void testGetSelectSQLForDataFilesWithAddColumn() throws Exception {
    Schema oldSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    Schema newSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()));

    when(mockTable.io()).thenReturn(mockFileIO);
    when(mockTable.schema()).thenReturn(newSchema);

    executeWithMockedStaticParquetUtil(
        oldSchema,
        () -> {
          List<String> dataFiles = Arrays.asList("file1.parquet");
          String sql = schemaEvolution.getSelectSQLForDataFiles(mockTable, dataFiles, false, false);

          assertThat(sql)
              .isEqualTo(
                  "(SELECT id, name, NULL::INTEGER AS age FROM read_parquet(['file1.parquet'], hive_partitioning=0, union_by_name=True))");
        });
  }

  @Test
  void testGetSelectSQLForDataFilesWithDropColumn() {
    Schema oldSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()));
    Schema newSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    when(mockTable.io()).thenReturn(mockFileIO);
    when(mockTable.schema()).thenReturn(newSchema);

    executeWithMockedStaticParquetUtil(
        oldSchema,
        () -> {
          List<String> dataFiles = Arrays.asList("old_file.parquet");
          String sql = schemaEvolution.getSelectSQLForDataFiles(mockTable, dataFiles, false, false);

          assertThat(sql)
              .isEqualTo(
                  "(SELECT id, name FROM read_parquet(['old_file.parquet'], hive_partitioning=0, union_by_name=True))");
        });
  }

  @Test
  void testGetSelectSQLForDataFilesWithRenameColumn() {
    Schema oldSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "full_name", Types.StringType.get()));
    Schema newSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    when(mockTable.schema()).thenReturn(newSchema);

    executeWithMockedStaticParquetUtil(
        oldSchema,
        () -> {
          List<String> dataFiles = Arrays.asList("old_file.parquet");
          String sql = schemaEvolution.getSelectSQLForDataFiles(mockTable, dataFiles, false, false);

          assertThat(sql)
              .isEqualTo(
                  "(SELECT id, full_name AS name FROM read_parquet(['old_file.parquet'], hive_partitioning=0, union_by_name=True))");
        });
  }

  @Test
  void testGetSelectSQLForDataFilesWithUpdateColumnType() {
    Schema oldSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    Schema newSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    when(mockTable.schema()).thenReturn(newSchema);

    executeWithMockedStaticParquetUtil(
        oldSchema,
        () -> {
          List<String> dataFiles = Arrays.asList("old_file.parquet");
          String sql = schemaEvolution.getSelectSQLForDataFiles(mockTable, dataFiles, false, false);

          assertThat(sql)
              .isEqualTo(
                  "(SELECT id::LONG AS id, name FROM read_parquet(['old_file.parquet'], hive_partitioning=0, union_by_name=True))");
        });
  }

  @Test
  void testGetSelectSQLForDataFilesWithReorderColumns() {
    Schema oldSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    Schema newSchema =
        new Schema(
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()));

    when(mockTable.schema()).thenReturn(newSchema);
    executeWithMockedStaticParquetUtil(
        oldSchema,
        () -> {
          List<String> dataFiles = Arrays.asList("old_file.parquet");
          String sql = schemaEvolution.getSelectSQLForDataFiles(mockTable, dataFiles, false, false);

          assertThat(sql)
              .isEqualTo(
                  "(SELECT name, id, NULL::INTEGER AS age FROM read_parquet(['old_file.parquet'], hive_partitioning=0, union_by_name=True))");
        });
  }

  @Test
  void testGetSelectSQLForDataFilesWithNestedStructAddField() {
    Schema oldSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(
                2,
                "info",
                Types.StructType.of(
                    Types.NestedField.required(3, "name", Types.StringType.get()))));
    Schema newSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(
                2,
                "info",
                Types.StructType.of(
                    Types.NestedField.required(3, "name", Types.StringType.get()),
                    Types.NestedField.optional(4, "age", Types.IntegerType.get()))));

    when(mockTable.schema()).thenReturn(newSchema);

    executeWithMockedStaticParquetUtil(
        oldSchema,
        () -> {
          List<String> dataFiles = Arrays.asList("old_file.parquet");
          String sql = schemaEvolution.getSelectSQLForDataFiles(mockTable, dataFiles, false, false);

          assertThat(sql)
              .isEqualTo(
                  "(SELECT id, {name:info.name,age:NULL::INTEGER} AS info FROM read_parquet(['old_file.parquet'], "
                      + "hive_partitioning=0, union_by_name=True))");
        });
  }

  @Test
  void testGetSelectSQLForDataFilesWithListElementTypeUpdate() {
    Schema oldSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(
                2, "tags", Types.ListType.ofRequired(3, Types.IntegerType.get())));
    Schema newSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(
                2, "tags", Types.ListType.ofRequired(3, Types.LongType.get())));

    when(mockTable.schema()).thenReturn(newSchema);

    executeWithMockedStaticParquetUtil(
        oldSchema,
        () -> {
          List<String> dataFiles = Arrays.asList("old_file.parquet");
          String sql = schemaEvolution.getSelectSQLForDataFiles(mockTable, dataFiles, false, false);

          assertThat(sql)
              .isEqualTo(
                  "(SELECT id, [__v1__::LONG for __v1__ in tags] AS tags FROM read_parquet(['old_file.parquet'], "
                      + "hive_partitioning=0, union_by_name=True))");
        });
  }

  @Test
  void testGetSelectSQLForDataFilesWithMapValueTypeUpdate() {
    Schema oldSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(
                2,
                "properties",
                Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.IntegerType.get())));
    Schema newSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(
                2,
                "properties",
                Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.LongType.get())));

    when(mockTable.schema()).thenReturn(newSchema);

    executeWithMockedStaticParquetUtil(
        oldSchema,
        () -> {
          List<String> dataFiles = Arrays.asList("old_file.parquet");
          String sql = schemaEvolution.getSelectSQLForDataFiles(mockTable, dataFiles, false, false);

          assertThat(sql)
              .isEqualTo(
                  "(SELECT id, map_from_entries([{key: __v1__.key, value: __v1__.value::LONG} for __v1__ in map_entries(properties)]) "
                      + "AS properties FROM read_parquet(['old_file.parquet'], hive_partitioning=0, union_by_name=True))");
        });
  }

  @Test
  void testGetSelectSQLForDataFilesWithComplexNestedChanges() {
    Schema oldSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(
                2,
                "info",
                Types.StructType.of(
                    Types.NestedField.required(3, "name", Types.StringType.get()),
                    Types.NestedField.required(
                        4, "tags", Types.ListType.ofRequired(5, Types.DecimalType.of(10, 2))),
                    Types.NestedField.required(6, "other_name", Types.StringType.get()))),
            Types.NestedField.required(
                7,
                "metadata",
                Types.MapType.ofRequired(8, 9, Types.StringType.get(), Types.IntegerType.get())));
    Schema newSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(
                2,
                "info",
                Types.StructType.of(
                    Types.NestedField.required(
                        4, "tags", Types.ListType.ofRequired(5, Types.DecimalType.of(20, 2))),
                    Types.NestedField.required(3, "full_name", Types.StringType.get()),
                    Types.NestedField.optional(10, "age", Types.IntegerType.get()))),
            Types.NestedField.required(
                7,
                "metadata",
                Types.MapType.ofRequired(8, 9, Types.StringType.get(), Types.LongType.get())));

    when(mockTable.schema()).thenReturn(newSchema);

    executeWithMockedStaticParquetUtil(
        oldSchema,
        () -> {
          List<String> dataFiles = Arrays.asList("old_file.parquet");
          String sql = schemaEvolution.getSelectSQLForDataFiles(mockTable, dataFiles, false, false);

          assertThat(sql)
              .isEqualTo(
                  "(SELECT id, {tags:[__v2__::DECIMAL(20,2) for __v2__ in info.tags],full_name:info.name,age:NULL::INTEGER} AS info, "
                      + "map_from_entries([{key: __v1__.key, value: __v1__.value::LONG} for __v1__ in map_entries(metadata)]) AS metadata "
                      + "FROM read_parquet(['old_file.parquet'], hive_partitioning=0, union_by_name=True))");
        });
  }

  @Test
  void testGetSelectSQLForDataFilesWithMultipleSchemas() {
    Schema oldSchema1 =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(
                3,
                "properties",
                Types.MapType.ofRequired(4, 5, Types.StringType.get(), Types.IntegerType.get())));
    Schema oldSchema2 =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(
                3,
                "properties",
                Types.MapType.ofRequired(4, 5, Types.StringType.get(), Types.IntegerType.get())));
    Schema newSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(
                3,
                "properties",
                Types.MapType.ofRequired(4, 5, Types.StringType.get(), Types.LongType.get())));

    when(mockTable.schema()).thenReturn(newSchema);
    when(mockTable.io()).thenReturn(mockFileIO);

    List<Pair<String, Schema>> files =
        List.of(
            Pair.of("file1.parquet", oldSchema1),
            Pair.of("file2.parquet", oldSchema1),
            Pair.of("file3.parquet", oldSchema2),
            Pair.of("file4.parquet", oldSchema2),
            Pair.of("file5.parquet", oldSchema2),
            Pair.of("file6.parquet", newSchema),
            Pair.of("file7.parquet", newSchema));
    Set<String> localFiles = Set.of("file1.parquet", "file3.parquet", "file6.parquet");
    List<String> remoteFiles =
        List.of("file2.parquet", "file4.parquet", "file5.parquet", "file7.parquet");
    try (MockedStatic<Files> mockedFiles = mockStatic(Files.class);
        MockedStatic<ParquetUtil> mockedParquetUtil = mockStatic(ParquetUtil.class);
        MockedStatic<ParquetSchemaUtil> mockedParquetSchemaUtil =
            mockStatic(org.apache.iceberg.parquet.ParquetSchemaUtil.class)) {
      for (Pair<String, Schema> file : files) {
        InputFile mockLocalInputFile = mock(InputFile.class);
        ParquetFileReader mockLocalParquetFileReader = mock(ParquetFileReader.class);
        FileMetaData mockLocalFileMetaData = mock(FileMetaData.class);
        MessageType mockLocalMessageType = mock(MessageType.class);

        if (localFiles.contains(file.getLeft())) {
          mockedFiles.when(() -> Files.localInput(file.getLeft())).thenReturn(mockLocalInputFile);
        } else {
          when(mockFileIO.newInputFile(eq(file.getLeft()), eq(false)))
              .thenReturn(mockLocalInputFile);
        }
        mockedParquetUtil
            .when(() -> ParquetUtil.getParquetFileReader(mockLocalInputFile))
            .thenReturn(mockLocalParquetFileReader);
        when(mockLocalParquetFileReader.getFileMetaData()).thenReturn(mockLocalFileMetaData);
        when(mockLocalFileMetaData.getSchema()).thenReturn(mockLocalMessageType);
        mockedParquetSchemaUtil
            .when(
                () ->
                    org.apache.iceberg.parquet.ParquetSchemaUtil.convertAndPrune(
                        mockLocalMessageType))
            .thenReturn(file.getRight());
      }

      String sql =
          schemaEvolution.getSelectSQLForDataFiles(
              mockTable, localFiles.stream().toList(), remoteFiles, false, false);

      assertThat(sql)
          .contains(
              "SELECT * FROM read_parquet(['file6.parquet','file7.parquet'], hive_partitioning=0, union_by_name=True)");
      assertThat(sql)
          .contains(
              "SELECT id, map_from_entries([{key: __v1__.key, value: __v1__.value::LONG} for __v1__ in map_entries(properties)]) AS properties "
                  + "FROM read_parquet(['file1.parquet','file2.parquet'], hive_partitioning=0, union_by_name=True)");
      assertThat(sql)
          .contains(
              "SELECT id, map_from_entries([{key: __v1__.key, value: __v1__.value::LONG} for __v1__ in map_entries(properties)]) AS properties "
                  + "FROM read_parquet(['file3.parquet','file4.parquet','file5.parquet'], hive_partitioning=0, union_by_name=True)");
    }
  }

  @Test
  void testGetSelectSQLForDataFilesWithLocalAndRemote() {
    List<String> localFiles = Arrays.asList("local1.parquet", "local2.parquet");
    List<String> remoteFiles = Arrays.asList("remote1.parquet", "remote2.parquet");
    when(mockTable.schema()).thenReturn(schema);
    when(mockTable.io()).thenReturn(mockFileIO);
    when(mockFileIO.newInputFile(anyString(), eq(false))).thenReturn(mockInputFile);

    executeWithMockedStaticParquetUtil(
        schema,
        () -> {
          String result =
              schemaEvolution.getSelectSQLForDataFiles(
                  mockTable, localFiles, remoteFiles, true, true);
          assertThat(result)
              .isEqualTo(
                  "(SELECT * FROM read_parquet(['local1.parquet','local2.parquet','remote1.parquet','remote2.parquet'], "
                      + "hive_partitioning=0, union_by_name=True, filename=true, file_row_number=true))");
        });
  }

  @Test
  void testGetSelectSQLForDataFilesWithRemoteFiles() {
    List<String> remoteFiles = Arrays.asList("remote1.parquet", "remote2.parquet");
    when(mockTable.schema()).thenReturn(schema);
    when(mockTable.io()).thenReturn(mockFileIO);
    when(mockFileIO.newInputFile(anyString(), eq(false))).thenReturn(mockInputFile);

    executeWithMockedStaticParquetUtil(
        schema,
        () -> {
          String result =
              schemaEvolution.getSelectSQLForDataFiles(mockTable, null, remoteFiles, true, true);
          assertThat(result)
              .isEqualTo(
                  "(SELECT * FROM read_parquet(['remote1.parquet','remote2.parquet'], hive_partitioning=0, "
                      + "union_by_name=True, filename=true, file_row_number=true))");
        });
  }

  @Test
  void testGetDuckDBFilterSql() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    when(mockTable.schema()).thenReturn(schema);

    com.arcesium.swiftlake.expressions.Expression filter =
        Expressions.and(Expressions.equal("id", 1), Expressions.startsWith("name", "A"));

    String sql = schemaEvolution.getDuckDBFilterSql(mockTable, filter);

    assertThat(sql).isEqualTo("(id = (1)::LONG AND name LIKE 'A%')");
  }

  @Test
  void testGetDuckDBFilterSqlWithTableAlias() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    when(mockTable.schema()).thenReturn(schema);

    com.arcesium.swiftlake.expressions.Expression filter =
        Expressions.and(Expressions.equal("id", 1), Expressions.startsWith("name", "A"));

    String sql = schemaEvolution.getDuckDBFilterSql(mockTable, filter, "a");

    assertThat(sql).isEqualTo("(a.id = (1)::LONG AND a.name LIKE 'A%')");
  }

  @Test
  void testGetDuckDBFilterSqlWithComplexFilter() {
    Schema schema = TestUtil.createComplexSchema();
    when(mockTable.schema()).thenReturn(schema);

    Expression filter =
        Expressions.or(
            Expressions.and(
                Expressions.notNull("int_1"),
                Expressions.and(
                    Expressions.lessThanOrEqual("struct_1.struct_1_decimal_1", 22.455),
                    Expressions.greaterThan("float_1", 22.455)),
                Expressions.or(
                    Expressions.and(
                        Expressions.isNull("list_1"), Expressions.startsWith("string_1", "abc")),
                    Expressions.or(
                        Expressions.notStartsWith("string_1", "xyz"),
                        Expressions.notIn("string_1", "a", "b")))),
            Expressions.and(
                Expressions.lessThan("float_1", 451.14),
                Expressions.greaterThanOrEqual("float_1", 99.22)));
    String sql = schemaEvolution.getDuckDBFilterSql(mockTable, filter);
    String expected =
        "(((int_1 IS NOT NULL AND (struct_1.struct_1_decimal_1 <= (22.455)::DECIMAL(20,14) AND (isnan(float_1) OR float_1 > (22.455)::FLOAT))) AND ((list_1 IS NULL "
            + "AND string_1 LIKE 'abc%') OR (string_1 NOT LIKE 'xyz%' OR string_1 NOT IN (('a')::STRING, ('b')::STRING)))) OR "
            + "(float_1 < (451.14)::FLOAT AND (isnan(float_1) OR float_1 >= (99.22)::FLOAT)))";

    assertThat(sql).isEqualTo(expected);

    filter =
        Expressions.or(
            Expressions.not(
                Expressions.and(
                    Expressions.notNull("int_1"),
                    Expressions.and(
                        Expressions.lessThanOrEqual("struct_1.struct_1_decimal_1", 22.455),
                        Expressions.greaterThan("float_1", 22.455)),
                    Expressions.or(
                        Expressions.and(
                            Expressions.isNull("list_1"),
                            Expressions.not(Expressions.startsWith("string_1", "abc"))),
                        Expressions.or(
                            Expressions.notStartsWith("string_1", "xyz"),
                            Expressions.notIn("string_1", "a", "b"))))),
            Expressions.and(
                Expressions.lessThan("float_1", 451.14),
                Expressions.greaterThanOrEqual("float_1", 99.22)));

    sql = schemaEvolution.getDuckDBFilterSql(mockTable, filter);
    expected =
        "(((int_1 IS NULL OR (struct_1.struct_1_decimal_1 > (22.455)::DECIMAL(20,14) OR ((NOT isnan(float_1)) AND float_1 <= (22.455)::FLOAT))) "
            + "OR ((list_1 IS NOT NULL OR string_1 LIKE 'abc%') AND (string_1 LIKE 'xyz%' AND string_1 IN (('a')::STRING, ('b')::STRING)))) "
            + "OR (float_1 < (451.14)::FLOAT AND (isnan(float_1) OR float_1 >= (99.22)::FLOAT)))";
    assertThat(sql).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("provideColumnNamesAndExpectedResults")
  void testEscapeColumnName(String input, String expected) {
    String result = schemaEvolution.escapeColumnName(input);
    assertThat(result).isEqualTo(expected);
  }

  static Stream<Arguments> provideColumnNamesAndExpectedResults() {
    return Stream.of(
        // Basic cases
        Arguments.of("column", "\"column\""),
        Arguments.of("", "\"\""),

        // Special cases that need proper handling
        Arguments.of("column name", "\"column name\""),
        Arguments.of("'column\tname'", "\"'column\tname'\""),
        Arguments.of("SELECT", "\"SELECT\""),
        Arguments.of("123numericStart", "\"123numericStart\""),

        // The most important test cases - handling of quotes
        Arguments.of("column\"name", "\"column\"\"name\""),
        Arguments.of("\"column\"", "\"\"\"column\"\"\""),
        Arguments.of("col\"um\"n", "\"col\"\"um\"\"n\""),
        Arguments.of("\"\"column\"\"", "\"\"\"\"\"column\"\"\"\"\""));
  }

  @Test
  void testEscapeColumnName_nullInput() {
    assertThatThrownBy(() -> schemaEvolution.escapeColumnName(null))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Column name cannot be null");
  }

  private void executeWithMockedStaticParquetUtil(Schema schema, Runnable runnable) {
    // Mock the ParquetUtil.getParquetFileReader method
    try (MockedStatic<ParquetUtil> mockedParquetUtil = mockStatic(ParquetUtil.class)) {
      mockedParquetUtil
          .when(() -> ParquetUtil.getParquetFileReader(any(InputFile.class)))
          .thenReturn(mockParquetFileReader);

      when(mockParquetFileReader.getFileMetaData()).thenReturn(mockFileMetaData);
      when(mockFileMetaData.getSchema()).thenReturn(mockMessageType);

      // Mock the ParquetSchemaUtil.convertAndPrune method
      try (MockedStatic<ParquetSchemaUtil> mockedParquetSchemaUtil =
          mockStatic(org.apache.iceberg.parquet.ParquetSchemaUtil.class)) {
        mockedParquetSchemaUtil
            .when(
                () ->
                    org.apache.iceberg.parquet.ParquetSchemaUtil.convertAndPrune(
                        any(MessageType.class)))
            .thenReturn(schema);
        runnable.run();
      }
    }
  }
}
