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

class DoubleTypeHandlerTest {

  private DoubleTypeHandler typeHandler;

  @Mock private PreparedStatement ps;

  @Mock private ResultSet rs;

  @Mock private CallableStatement cs;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    typeHandler = new DoubleTypeHandler();
  }

  @ParameterizedTest
  @ValueSource(doubles = {0.0, 1.0, -1.0, Double.MAX_VALUE, Double.MIN_VALUE, Math.PI})
  void testSetNonNullParameter(Double value) throws SQLException {
    typeHandler.setNonNullParameter(ps, 1, value, null);
    verify(ps).setObject(1, value);
  }

  @Test
  void testSetNonNullParameterWithJdbcType() throws SQLException {
    Double value = 3.14;
    typeHandler.setNonNullParameter(ps, 1, value, JdbcType.DOUBLE);
    verify(ps).setObject(1, value);
  }

  @Test
  void testGetNullableResultByColumnName() throws SQLException {
    when(rs.findColumn("column")).thenReturn(1);
    when(rs.getObject(1)).thenReturn(3.14);

    Double result = typeHandler.getNullableResult(rs, "column");
    assertThat(result).isEqualTo(3.14);
  }

  @Test
  void testGetNullableResultByColumnIndex() throws SQLException {
    when(rs.getObject(1)).thenReturn(3.14);

    Double result = typeHandler.getNullableResult(rs, 1);
    assertThat(result).isEqualTo(3.14);
  }

  @Test
  void testGetNullableResultFromCallableStatement() throws SQLException {
    when(cs.getObject(1)).thenReturn(3.14);

    Double result = typeHandler.getNullableResult(cs, 1);
    assertThat(result).isEqualTo(3.14);
  }

  @Test
  void testGetNullableResultWithNull() throws SQLException {
    when(rs.getObject(1)).thenReturn(null);

    Double result = typeHandler.getNullableResult(rs, 1);
    assertThat(result).isNull();
  }

  @Test
  void testGetNullableResultByColumnNameWithNull() throws SQLException {
    when(rs.findColumn("column")).thenReturn(1);
    when(rs.getObject(1)).thenReturn(null);

    Double result = typeHandler.getNullableResult(rs, "column");
    assertThat(result).isNull();
  }

  @Test
  void testGetNullableResultFromCallableStatementWithNull() throws SQLException {
    when(cs.getObject(1)).thenReturn(null);

    Double result = typeHandler.getNullableResult(cs, 1);
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
}
