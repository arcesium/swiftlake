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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.ArrayStructLike;
import com.arcesium.swiftlake.common.DataFile;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.dao.PartitionedDataDao;
import com.arcesium.swiftlake.io.SwiftLakeFileIO;
import com.arcesium.swiftlake.sql.SchemaEvolution;
import com.arcesium.swiftlake.sql.SwiftLakeConnection;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PartitionedDataFileWriterTest {

  @Mock private SwiftLakeEngine mockEngine;
  @Mock private Table mockTable;
  @Mock private PartitionedDataDao mockPartitionedDataDao;
  @Mock private SchemaEvolution mockSchemaEvolution;
  @Mock private SwiftLakeFileIO mockFileIO;
  @Mock private SwiftLakeConnection mockConnection;
  @Mock private Statement mockStatement;

  private PartitionedDataFileWriter writer;
  private Method getPartitionValuesStructMethod;
  private Method createPartitionsDataFileMethod;
  private Method uploadDataFilesForPartitionKeyMethod;

  @BeforeEach
  void setUp() {
    when(mockEngine.getLocalDir()).thenReturn("/tmp");
    when(mockTable.io()).thenReturn(mockFileIO);
    when(mockEngine.getSchemaEvolution()).thenReturn(mockSchemaEvolution);

    writer =
        PartitionedDataFileWriter.builderFor(
                mockEngine, mockTable, "SELECT * FROM partition_data", "SELECT * FROM main_data")
            .currentFlagColumn("is_current")
            .effectiveStartColumn("effective_start")
            .effectiveEndColumn("effective_end")
            .build();
    try {
      // Use reflection to set the mockPartitionedDataDao
      Field partitionedDataDaoField =
          PartitionedDataFileWriter.class.getDeclaredField("partitionedDataDao");
      partitionedDataDaoField.setAccessible(true);
      partitionedDataDaoField.set(writer, mockPartitionedDataDao);
      // Get the private method for testing
      getPartitionValuesStructMethod =
          PartitionedDataFileWriter.class.getDeclaredMethod(
              "getPartitionValuesStruct", Table.class, Map.class, Set.class);
      getPartitionValuesStructMethod.setAccessible(true);

      createPartitionsDataFileMethod =
          PartitionedDataFileWriter.class.getDeclaredMethod(
              "createPartitionsDataFile", Table.class, List.class, Set.class);
      createPartitionsDataFileMethod.setAccessible(true);

      uploadDataFilesForPartitionKeyMethod =
          PartitionedDataFileWriter.class.getDeclaredMethod(
              "uploadDataFilesForPartitionKey",
              Table.class,
              String.class,
              Statement.class,
              StructLike.class);
      uploadDataFilesForPartitionKeyMethod.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testWrite_SinglePartition() throws Exception {
    when(mockTable.spec()).thenReturn(PartitionSpec.unpartitioned());

    assertThatThrownBy(
            () ->
                getPartitionValuesStructMethod.invoke(
                    writer, mockTable, new HashMap<>(), new HashSet<>()))
        .isInstanceOf(java.lang.reflect.InvocationTargetException.class)
        .getCause()
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot add partition data for an un-partitioned table");
  }

  @Test
  void testGetPartitionValuesStruct_MissingPartitionData() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();

    when(mockTable.spec()).thenReturn(spec);

    assertThatThrownBy(
            () -> getPartitionValuesStructMethod.invoke(writer, mockTable, null, new HashSet<>()))
        .isInstanceOf(java.lang.reflect.InvocationTargetException.class)
        .getCause()
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Provide partition data for partitioned table");
  }

  @ParameterizedTest
  @MethodSource("providePartitionDataForStruct")
  void testGetPartitionValuesStruct_ValidPartitionData(
      Schema schema,
      PartitionSpec spec,
      Map<String, Object> partitionData,
      Set<String> singleBucketColumns,
      Object expectedValue)
      throws Exception {
    when(mockTable.spec()).thenReturn(spec);
    when(mockTable.schema()).thenReturn(schema);

    StructLike result =
        (StructLike)
            getPartitionValuesStructMethod.invoke(
                writer, mockTable, partitionData, singleBucketColumns);

    assertThat(result).isNotNull();
    assertThat(result.get(0, Object.class)).isEqualTo(expectedValue);
  }

  private static Stream<Arguments> providePartitionDataForStruct() {
    return Stream.of(
        Arguments.of(
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
            PartitionSpec.builderFor(
                    new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
                .identity("id")
                .build(),
            Collections.singletonMap("id", 1L),
            new HashSet<>(),
            1L),
        Arguments.of(
            new Schema(Types.NestedField.required(1, "date", Types.DateType.get())),
            PartitionSpec.builderFor(
                    new Schema(Types.NestedField.required(1, "date", Types.DateType.get())))
                .identity("date")
                .build(),
            Collections.singletonMap("date", LocalDate.of(2025, 1, 1)),
            new HashSet<>(),
            DateTimeUtil.daysFromDate(LocalDate.of(2025, 1, 1))),
        Arguments.of(
            new Schema(
                Types.NestedField.required(1, "timestamp", Types.TimestampType.withoutZone())),
            PartitionSpec.builderFor(
                    new Schema(
                        Types.NestedField.required(
                            1, "timestamp", Types.TimestampType.withoutZone())))
                .identity("timestamp")
                .build(),
            Collections.singletonMap("timestamp", LocalDateTime.of(2025, 1, 1, 12, 0)),
            new HashSet<>(),
            DateTimeUtil.microsFromTimestamp(LocalDateTime.of(2025, 1, 1, 12, 0))),
        Arguments.of(
            new Schema(
                Types.NestedField.required(1, "timestamp", Types.TimestampType.withoutZone())),
            PartitionSpec.builderFor(
                    new Schema(
                        Types.NestedField.required(
                            1, "timestamp", Types.TimestampType.withoutZone())))
                .identity("timestamp")
                .build(),
            Collections.singletonMap("timestamp", null),
            new HashSet<>(),
            null));
  }

  @Test
  void testCreatePartitionsDataFile() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "date", Types.DateType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").day("date").build();
    when(mockTable.schema()).thenReturn(schema);
    when(mockTable.spec()).thenReturn(spec);

    List<Pair<Map<String, Object>, StructLike>> partitionDataList =
        Arrays.asList(
            Pair.of(Collections.singletonMap("id", 1L), new ArrayStructLike(new Object[] {1L, 1})),
            Pair.of(Collections.singletonMap("id", 2L), new ArrayStructLike(new Object[] {2L, 2})));

    Set<String> singleBucketColumns = new HashSet<>();

    @SuppressWarnings("unchecked")
    Pair<String, Map<String, String>> result =
        (Pair<String, Map<String, String>>)
            createPartitionsDataFileMethod.invoke(
                writer, mockTable, partitionDataList, singleBucketColumns);

    assertThat(result).isNotNull();
    assertThat(result.getLeft()).startsWith("(SELECT * FROM read_parquet('");
    assertThat(result.getRight()).isEmpty();
  }

  @Test
  void testUploadDataFilesForPartitionKey() throws Exception {
    // Setup common mocks
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();
    when(mockTable.spec()).thenReturn(spec);

    String sql = "SELECT * FROM test_table";
    StructLike partitionKey = new ArrayStructLike(new Object[] {1L});

    // Stub the executeSqlAndCreateLocalDataFiles method
    PartitionedDataFileWriter writerSpy = spy(writer);
    doReturn(Collections.singletonList("/tmp/test.parquet"))
        .when(writerSpy)
        .executeSqlAndCreateLocalDataFiles(
            any(),
            anyString(),
            anyString(),
            any(),
            any(),
            any(),
            any(),
            anyBoolean(),
            any(),
            any());

    when(mockTable.locationProvider()).thenReturn(mock(LocationProvider.class));
    when(mockTable.locationProvider().newDataLocation(any(), any(), any()))
        .thenReturn("/path/test.parquet");

    // Create a mock DataFile to be returned by prepareNewDataFiles
    List<DataFile> mockPreparedFiles = new ArrayList<>();
    DataFile mockDataFile = mock(DataFile.class);
    mockPreparedFiles.add(mockDataFile);

    // Stub prepareNewDataFiles to return our mock DataFile
    doReturn(mockPreparedFiles).when(writerSpy).prepareNewDataFiles(any(), any(), any());

    // Invoke the private method using reflection, but on the spy
    @SuppressWarnings("unchecked")
    List<DataFile> result =
        (List<DataFile>)
            uploadDataFilesForPartitionKeyMethod.invoke(
                writerSpy, mockTable, sql, mockStatement, partitionKey);

    // Assertions
    assertThat(result).hasSize(1);
    assertThat(result).isEqualTo(mockPreparedFiles);
    verify(mockFileIO).uploadFiles(anyList());
  }

  @Test
  void testBuilder() {
    PartitionedDataFileWriter customWriter =
        PartitionedDataFileWriter.builderFor(
                mockEngine, mockTable, "SELECT * FROM partition_data", "SELECT * FROM main_data")
            .currentFlagColumn("is_current")
            .effectiveStartColumn("start_date")
            .effectiveEndColumn("end_date")
            .sortOrder(SortOrder.unsorted())
            .skipDataSorting(true)
            .targetFileSizeBytes(1024L)
            .rowGroupSize(128L)
            .build();

    assertThat(customWriter).isNotNull();
    assertThat(customWriter).extracting("swiftLakeEngine").isEqualTo(mockEngine);
    assertThat(customWriter).extracting("table").isEqualTo(mockTable);
    assertThat(customWriter)
        .extracting("partitionDataSql")
        .isEqualTo("SELECT * FROM partition_data");
    assertThat(customWriter).extracting("sql").isEqualTo("SELECT * FROM main_data");
    assertThat(customWriter).extracting("sortOrder").isEqualTo(null);
    assertThat(customWriter).extracting("currentFlagColumn").isEqualTo("is_current");
    assertThat(customWriter).extracting("effectiveStartColumn").isEqualTo("start_date");
    assertThat(customWriter).extracting("effectiveEndColumn").isEqualTo("end_date");
    assertThat(customWriter).extracting("targetFileSizeBytes").isEqualTo(1024L);
    assertThat(customWriter).extracting("rowGroupSize").isEqualTo(128L);
  }

  @Test
  void testEncodeAndDecodePartitionValue() throws Exception {
    Method encodeMethod =
        PartitionedDataFileWriter.class.getDeclaredMethod("encodePartitionValue", String.class);
    Method decodeMethod =
        PartitionedDataFileWriter.class.getDeclaredMethod("decodePartitionValue", String.class);
    encodeMethod.setAccessible(true);
    decodeMethod.setAccessible(true);

    // Test basic functionality
    String originalValue = "test value";
    String encodedValue = (String) encodeMethod.invoke(writer, originalValue);
    String decodedValue = (String) decodeMethod.invoke(writer, encodedValue);
    assertThat(decodedValue).isEqualTo(originalValue);

    // Test null values
    assertThat(encodeMethod.invoke(writer, (Object) null)).isEqualTo("#n");
    assertThat(decodeMethod.invoke(writer, "#n")).isNull();

    // Test empty strings
    assertThat(encodeMethod.invoke(writer, "")).isEqualTo("");
    assertThat(decodeMethod.invoke(writer, "")).isEqualTo("");

    // Test the string "null"
    assertThat(encodeMethod.invoke(writer, "null")).isEqualTo("#null");
    assertThat(decodeMethod.invoke(writer, "#null")).isEqualTo("null");

    // Test values that start with the escape character
    assertThat(encodeMethod.invoke(writer, "#")).isEqualTo("##");
    assertThat(decodeMethod.invoke(writer, "##")).isEqualTo("#");
    assertThat(encodeMethod.invoke(writer, "#null")).isEqualTo("##null");
    assertThat(decodeMethod.invoke(writer, "##null")).isEqualTo("#null");
    assertThat(encodeMethod.invoke(writer, "#value")).isEqualTo("##value");
    assertThat(decodeMethod.invoke(writer, "##value")).isEqualTo("#value");

    // Test with special characters
    String specialChars = "value with spaces & special/chars?=+%";
    String encodedSpecialChars = (String) encodeMethod.invoke(writer, specialChars);
    assertThat(decodeMethod.invoke(writer, encodedSpecialChars)).isEqualTo(specialChars);

    // Test with Unicode characters
    String unicodeChars = "Unicode文字🔥";
    String encodedUnicode = (String) encodeMethod.invoke(writer, unicodeChars);
    assertThat(decodeMethod.invoke(writer, encodedUnicode)).isEqualTo(unicodeChars);

    // Test with already URL-encoded values
    String alreadyEncoded = "already%20encoded";
    String doubleEncoded = (String) encodeMethod.invoke(writer, alreadyEncoded);
    // After double-decoding, we should get the original
    String decoded = (String) decodeMethod.invoke(writer, doubleEncoded);
    assertThat(decoded).isEqualTo(alreadyEncoded);

    // Test round-trip for all combinations
    List<String> testValues =
        Arrays.asList(
            null,
            "",
            "#value",
            "null",
            "#null",
            "value with spaces & special/chars?=+%",
            "Unicode文字🔥",
            "already%20encoded",
            "#");

    for (String value : testValues) {
      String encoded = (String) encodeMethod.invoke(writer, value);
      String roundTrip = (String) decodeMethod.invoke(writer, encoded);
      assertThat(roundTrip).as("Round-trip for value: " + value).isEqualTo(value);
    }
  }

  @Test
  void testGetDuckDBFilterForPartitionTransforms() throws Exception {
    Method method =
        PartitionedDataFileWriter.class.getDeclaredMethod(
            "getDuckDBFilterForPartitionTransforms",
            Table.class,
            Map.class,
            String.class,
            String.class);
    method.setAccessible(true);

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "date", Types.DateType.get()));
    PartitionSpec spec =
        PartitionSpec.builderFor(schema).identity("id").truncate("name", 5).day("date").build();
    when(mockTable.schema()).thenReturn(schema);
    when(mockTable.spec()).thenReturn(spec);

    Map<String, Object> values = new HashMap<>();
    values.put("__id__", 1L);
    values.put("__date__", 18262);
    values.put("__name__", "John");

    when(mockSchemaEvolution.getPrimitiveTypeValueForSql(eq(Types.LongType.get()), eq(1L)))
        .thenReturn("1::LONG");
    when(mockSchemaEvolution.getPrimitiveTypeValueForSql(eq(Types.IntegerType.get()), eq(18262)))
        .thenReturn("18262::INTEGER");
    when(mockSchemaEvolution.getPrimitiveTypeValueForSql(eq(Types.StringType.get()), eq("John")))
        .thenReturn("'John'::STRING");

    @SuppressWarnings("unchecked")
    List<String> filters = (List<String>) method.invoke(writer, mockTable, values, "__", "__");

    assertThat(filters).hasSize(3);
    assertThat(filters).contains("__id__ = 1::LONG");
    assertThat(filters).contains("__date__ = 18262::INTEGER");
    assertThat(filters).contains("__name__ = 'John'::STRING");

    values = new HashMap<>();
    values.put("__id__", null);
    values.put("__date__", null);
    values.put("__name__", "");

    when(mockSchemaEvolution.getPrimitiveTypeValueForSql(eq(Types.StringType.get()), eq("")))
        .thenReturn("''::STRING");

    filters = (List<String>) method.invoke(writer, mockTable, values, "__", "__");

    assertThat(filters).hasSize(3);
    assertThat(filters).contains("__id__ IS NULL");
    assertThat(filters).contains("__date__ IS NULL");
    assertThat(filters).contains("__name__ = ''::STRING");
  }

  @Test
  void testWrite_EmptyPartitions() {
    when(mockPartitionedDataDao.getDistinctPartitions(
            anyString(), anyList(), any(), any(), anySet()))
        .thenReturn(Collections.emptyList());
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();

    when(mockTable.schema()).thenReturn(schema);
    when(mockTable.spec()).thenReturn(spec);

    List<DataFile> result = writer.write();

    assertThat(result).isEmpty();
  }

  @Test
  void testWrite_NullValuesInPartitionColumns() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    when(mockTable.schema()).thenReturn(schema);
    when(mockTable.spec())
        .thenReturn(PartitionSpec.builderFor(schema).identity("id").identity("name").build());

    Map<String, Object> partitionData = new HashMap<>();
    partitionData.put("id", null);
    partitionData.put("name", null);

    when(mockPartitionedDataDao.getDistinctPartitions(
            anyString(), anyList(), any(), any(), anySet()))
        .thenReturn(Collections.singletonList(partitionData));

    when(mockEngine.createConnection(false)).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);

    PartitionedDataFileWriter writerSpy = spy(writer);
    doReturn(Collections.singletonList("/tmp/test.parquet"))
        .when(writerSpy)
        .executeSqlAndCreateLocalDataFiles(
            any(),
            anyString(),
            anyString(),
            any(),
            any(),
            any(),
            any(),
            anyBoolean(),
            any(),
            any());
    when(mockTable.locationProvider()).thenReturn(mock(LocationProvider.class));
    when(mockTable.locationProvider().newDataLocation(any(), any(), any()))
        .thenReturn("/path/test.parquet");

    // Create a mock DataFile to be returned by prepareNewDataFiles
    List<DataFile> mockPreparedFiles = new ArrayList<>();
    DataFile mockDataFile = mock(DataFile.class);
    mockPreparedFiles.add(mockDataFile);

    // Stub prepareNewDataFiles to return our mock DataFile
    doReturn(mockPreparedFiles).when(writerSpy).prepareNewDataFiles(any(), any(), any());

    List<DataFile> result = writerSpy.write();

    assertThat(result).hasSize(1);
    assertThat(result).isEqualTo(mockPreparedFiles);
    verify(mockFileIO).uploadFiles(anyList());
  }

  @Test
  void testWrite_MalformedSqlQuery() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    when(mockTable.schema()).thenReturn(schema);
    when(mockTable.spec()).thenReturn(PartitionSpec.builderFor(schema).identity("id").build());
    PartitionedDataFileWriter malformedWriter =
        PartitionedDataFileWriter.builderFor(
                mockEngine, mockTable, "SELECT * FRM invalid_table", "SELECT * FRM main_data")
            .build();
    // Use reflection to set the mockPartitionedDataDao
    Field partitionedDataDaoField =
        PartitionedDataFileWriter.class.getDeclaredField("partitionedDataDao");
    partitionedDataDaoField.setAccessible(true);
    partitionedDataDaoField.set(malformedWriter, mockPartitionedDataDao);
    when(mockPartitionedDataDao.getDistinctPartitions(
            anyString(), anyList(), any(), any(), anySet()))
        .thenThrow(new RuntimeException("SQL syntax error"));

    assertThatThrownBy(() -> malformedWriter.write())
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("SQL syntax error");
  }
}
