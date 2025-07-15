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
package com.arcesium.swiftlake.mybatis.type;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;
import org.apache.ibatis.type.JdbcType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class OffsetDateTimeTypeHandlerTest {

  private OffsetDateTimeTypeHandler typeHandler;

  @Mock private PreparedStatement ps;

  @Mock private ResultSet rs;

  @Mock private CallableStatement cs;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    typeHandler = new OffsetDateTimeTypeHandler();
  }

  @ParameterizedTest
  @MethodSource("provideOffsetDateTimes")
  void testSetNonNullParameter(OffsetDateTime offsetDateTime) throws SQLException {
    typeHandler.setNonNullParameter(ps, 1, offsetDateTime, null);
    verify(ps).setObject(1, offsetDateTime);
  }

  @Test
  void testSetNonNullParameterWithJdbcType() throws SQLException {
    OffsetDateTime offsetDateTime = OffsetDateTime.now();
    typeHandler.setNonNullParameter(ps, 1, offsetDateTime, JdbcType.TIMESTAMP_WITH_TIMEZONE);
    verify(ps).setObject(1, offsetDateTime);
  }

  @Test
  void testGetNullableResultByColumnName() throws SQLException {
    OffsetDateTime expected = OffsetDateTime.now();
    when(rs.findColumn("column")).thenReturn(1);
    when(rs.getObject(1, OffsetDateTime.class)).thenReturn(expected);

    OffsetDateTime result = typeHandler.getNullableResult(rs, "column");
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetNullableResultByColumnIndex() throws SQLException {
    OffsetDateTime expected = OffsetDateTime.now();
    when(rs.getObject(1, OffsetDateTime.class)).thenReturn(expected);

    OffsetDateTime result = typeHandler.getNullableResult(rs, 1);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetNullableResultFromCallableStatement() throws SQLException {
    OffsetDateTime expected = OffsetDateTime.now();
    when(cs.getObject(1, OffsetDateTime.class)).thenReturn(expected);

    OffsetDateTime result = typeHandler.getNullableResult(cs, 1);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetNullableResultWithNull() throws SQLException {
    when(rs.getObject(1, OffsetDateTime.class)).thenReturn(null);

    OffsetDateTime result = typeHandler.getNullableResult(rs, 1);
    assertThat(result).isNull();
  }

  @Test
  void testGetNullableResultByColumnNameWithNull() throws SQLException {
    when(rs.findColumn("column")).thenReturn(1);
    when(rs.getObject(1, OffsetDateTime.class)).thenReturn(null);

    OffsetDateTime result = typeHandler.getNullableResult(rs, "column");
    assertThat(result).isNull();
  }

  @Test
  void testGetNullableResultFromCallableStatementWithNull() throws SQLException {
    when(cs.getObject(1, OffsetDateTime.class)).thenReturn(null);

    OffsetDateTime result = typeHandler.getNullableResult(cs, 1);
    assertThat(result).isNull();
  }

  @Test
  void testGetNullableResultByInvalidColumnName() throws SQLException {
    when(rs.findColumn("invalidColumn")).thenThrow(new SQLException("Column not found"));

    assertThatThrownBy(() -> typeHandler.getNullableResult(rs, "invalidColumn"))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Column not found");
  }

  @Test
  void testGetNullableResultByInvalidColumnIndex() throws SQLException {
    when(rs.getObject(999, OffsetDateTime.class))
        .thenThrow(new SQLException("Invalid column index"));

    assertThatThrownBy(() -> typeHandler.getNullableResult(rs, 999))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Invalid column index");
  }

  @Test
  void testGetNullableResultFromCallableStatementWithInvalidIndex() throws SQLException {
    when(cs.getObject(999, OffsetDateTime.class))
        .thenThrow(new SQLException("Invalid parameter index"));

    assertThatThrownBy(() -> typeHandler.getNullableResult(cs, 999))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Invalid parameter index");
  }

  private static Stream<OffsetDateTime> provideOffsetDateTimes() {
    return Stream.of(
        OffsetDateTime.now(),
        OffsetDateTime.of(2023, 5, 15, 10, 30, 0, 0, ZoneOffset.UTC),
        OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.MIN),
        OffsetDateTime.of(2999, 12, 31, 23, 59, 59, 999999999, ZoneOffset.MAX),
        OffsetDateTime.now(ZoneOffset.ofHours(-5)),
        OffsetDateTime.now(ZoneOffset.ofHours(5)));
  }
}
