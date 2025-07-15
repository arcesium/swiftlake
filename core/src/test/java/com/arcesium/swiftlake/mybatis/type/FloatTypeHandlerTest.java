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
import org.apache.ibatis.type.JdbcType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class FloatTypeHandlerTest {

  private FloatTypeHandler typeHandler;

  @Mock private PreparedStatement ps;

  @Mock private ResultSet rs;

  @Mock private CallableStatement cs;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    typeHandler = new FloatTypeHandler();
  }

  @ParameterizedTest
  @ValueSource(floats = {0.0f, 1.0f, -1.0f, Float.MAX_VALUE, Float.MIN_VALUE, (float) Math.PI})
  void testSetNonNullParameter(Float value) throws SQLException {
    typeHandler.setNonNullParameter(ps, 1, value, null);
    verify(ps).setObject(1, value);
  }

  @Test
  void testSetNonNullParameterWithJdbcType() throws SQLException {
    Float value = 3.14f;
    typeHandler.setNonNullParameter(ps, 1, value, JdbcType.FLOAT);
    verify(ps).setObject(1, value);
  }

  @Test
  void testGetNullableResultByColumnName() throws SQLException {
    when(rs.findColumn("column")).thenReturn(1);
    when(rs.getObject(1)).thenReturn(3.14f);

    Float result = typeHandler.getNullableResult(rs, "column");
    assertThat(result).isEqualTo(3.14f);
  }

  @Test
  void testGetNullableResultByColumnIndex() throws SQLException {
    when(rs.getObject(1)).thenReturn(3.14f);

    Float result = typeHandler.getNullableResult(rs, 1);
    assertThat(result).isEqualTo(3.14f);
  }

  @Test
  void testGetNullableResultFromCallableStatement() throws SQLException {
    when(cs.getObject(1)).thenReturn(3.14f);

    Float result = typeHandler.getNullableResult(cs, 1);
    assertThat(result).isEqualTo(3.14f);
  }

  @Test
  void testGetNullableResultWithNull() throws SQLException {
    when(rs.getObject(1)).thenReturn(null);

    Float result = typeHandler.getNullableResult(rs, 1);
    assertThat(result).isNull();
  }

  @Test
  void testGetNullableResultByColumnNameWithNull() throws SQLException {
    when(rs.findColumn("column")).thenReturn(1);
    when(rs.getObject(1)).thenReturn(null);

    Float result = typeHandler.getNullableResult(rs, "column");
    assertThat(result).isNull();
  }

  @Test
  void testGetNullableResultFromCallableStatementWithNull() throws SQLException {
    when(cs.getObject(1)).thenReturn(null);

    Float result = typeHandler.getNullableResult(cs, 1);
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
    when(rs.getObject(999)).thenThrow(new SQLException("Invalid column index"));

    assertThatThrownBy(() -> typeHandler.getNullableResult(rs, 999))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Invalid column index");
  }

  @Test
  void testGetNullableResultFromCallableStatementWithInvalidIndex() throws SQLException {
    when(cs.getObject(999)).thenThrow(new SQLException("Invalid parameter index"));

    assertThatThrownBy(() -> typeHandler.getNullableResult(cs, 999))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Invalid parameter index");
  }

  @Test
  void testHandlingOfNonFloatObject() throws SQLException {
    when(rs.getObject(1)).thenReturn("Not a Float");

    assertThatThrownBy(() -> typeHandler.getNullableResult(rs, 1))
        .isInstanceOf(ClassCastException.class);
  }

  @Test
  void testHandlingOfSpecialFloatValues() throws SQLException {
    when(rs.getObject(1)).thenReturn(Float.NaN);
    assertThat(typeHandler.getNullableResult(rs, 1)).isNaN();

    when(rs.getObject(2)).thenReturn(Float.POSITIVE_INFINITY);
    assertThat(typeHandler.getNullableResult(rs, 2)).isEqualTo(Float.POSITIVE_INFINITY);

    when(rs.getObject(3)).thenReturn(Float.NEGATIVE_INFINITY);
    assertThat(typeHandler.getNullableResult(rs, 3)).isEqualTo(Float.NEGATIVE_INFINITY);
  }
}
