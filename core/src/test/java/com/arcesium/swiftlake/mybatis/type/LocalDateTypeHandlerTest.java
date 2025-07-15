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

import java.sql.*;
import java.time.LocalDate;
import java.util.stream.Stream;
import org.apache.ibatis.type.JdbcType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class LocalDateTypeHandlerTest {

  private LocalDateTypeHandler typeHandler;

  @Mock private PreparedStatement ps;

  @Mock private ResultSet rs;

  @Mock private CallableStatement cs;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    typeHandler = new LocalDateTypeHandler();
  }

  @ParameterizedTest
  @MethodSource("provideDates")
  void testSetNonNullParameter(LocalDate date) throws SQLException {
    typeHandler.setNonNullParameter(ps, 1, date, null);
    verify(ps).setObject(1, Date.valueOf(date));
  }

  @Test
  void testSetNonNullParameterWithJdbcType() throws SQLException {
    LocalDate date = LocalDate.of(2023, 5, 15);
    typeHandler.setNonNullParameter(ps, 1, date, JdbcType.DATE);
    verify(ps).setObject(1, Date.valueOf(date));
  }

  @Test
  void testGetNullableResultByColumnName() throws SQLException {
    LocalDate expected = LocalDate.of(2023, 5, 15);
    when(rs.getObject("column")).thenReturn(expected);

    LocalDate result = typeHandler.getNullableResult(rs, "column");
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetNullableResultByColumnIndex() throws SQLException {
    LocalDate expected = LocalDate.of(2023, 5, 15);
    when(rs.getObject(1)).thenReturn(expected);

    LocalDate result = typeHandler.getNullableResult(rs, 1);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetNullableResultFromCallableStatement() throws SQLException {
    LocalDate expected = LocalDate.of(2023, 5, 15);
    when(cs.getObject(1, LocalDate.class)).thenReturn(expected);

    LocalDate result = typeHandler.getNullableResult(cs, 1);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetNullableResultWithNull() throws SQLException {
    when(rs.getObject(1)).thenReturn(null);

    LocalDate result = typeHandler.getNullableResult(rs, 1);
    assertThat(result).isNull();
  }

  @Test
  void testGetNullableResultByColumnNameWithNull() throws SQLException {
    when(rs.getObject("column")).thenReturn(null);

    LocalDate result = typeHandler.getNullableResult(rs, "column");
    assertThat(result).isNull();
  }

  @Test
  void testGetNullableResultFromCallableStatementWithNull() throws SQLException {
    when(cs.getObject(1, LocalDate.class)).thenReturn(null);

    LocalDate result = typeHandler.getNullableResult(cs, 1);
    assertThat(result).isNull();
  }

  @Test
  void testGetNullableResultByInvalidColumnName() throws SQLException {
    when(rs.getObject("invalidColumn")).thenThrow(new SQLException("Column not found"));

    assertThatThrownBy(() -> typeHandler.getNullableResult(rs, "invalidColumn"))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Column not found");
  }

  @Test
  void testGetNullableResultByInvalidColumnIndex() throws SQLException {
    when(rs.getObject(999)).thenThrow(new SQLException("Invalid column index"));

    assertThatThrownBy(() -> typeHandler.getNullableResult(rs, 999))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Invalid column index");
  }

  @Test
  void testGetNullableResultFromCallableStatementWithInvalidIndex() throws SQLException {
    when(cs.getObject(999, LocalDate.class)).thenThrow(new SQLException("Invalid parameter index"));

    assertThatThrownBy(() -> typeHandler.getNullableResult(cs, 999))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Invalid parameter index");
  }

  @Test
  void testHandlingOfNonLocalDateObject() throws SQLException {
    when(rs.getObject(1)).thenReturn("Not a LocalDate");

    assertThatThrownBy(() -> typeHandler.getNullableResult(rs, 1))
        .isInstanceOf(ClassCastException.class);
  }

  private static Stream<LocalDate> provideDates() {
    return Stream.of(
        LocalDate.of(2023, 5, 15),
        LocalDate.of(1970, 1, 1),
        LocalDate.of(2999, 12, 31),
        LocalDate.now(),
        LocalDate.MIN,
        LocalDate.MAX);
  }
}
