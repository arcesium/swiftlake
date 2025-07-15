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
package com.arcesium.swiftlake.mybatis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SwiftLakeBoundSqlTest {

  @Mock private Configuration mockConfiguration;

  @Mock private BoundSql mockDelegate;

  private List<ParameterMapping> parameterMappings;
  private Object parameterObject;
  private String sql;

  @BeforeEach
  void setUp() {
    parameterMappings = new ArrayList<>();
    parameterObject = new Object();
    sql = "SELECT * FROM table WHERE id = ?";
  }

  @Test
  void testConstructorWithDelegate() {
    SwiftLakeBoundSql boundSql = new SwiftLakeBoundSql(mockConfiguration, mockDelegate, sql);

    assertThat(boundSql.getSql()).isEqualTo(sql);
    assertThat(boundSql.getParameterMappings()).isEqualTo(mockDelegate.getParameterMappings());
    assertThat(boundSql.getParameterObject()).isEqualTo(mockDelegate.getParameterObject());
  }

  @Test
  void testConstructorWithoutDelegate() {
    SwiftLakeBoundSql boundSql =
        new SwiftLakeBoundSql(mockConfiguration, sql, parameterMappings, parameterObject);

    assertThat(boundSql.getSql()).isEqualTo(sql);
    assertThatThrownBy(() -> boundSql.getParameterMappings())
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> boundSql.getParameterObject())
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void testHasAdditionalParameter() {
    String paramName = "testParam";
    when(mockDelegate.hasAdditionalParameter(paramName)).thenReturn(true);
    SwiftLakeBoundSql boundSql = new SwiftLakeBoundSql(mockConfiguration, mockDelegate, sql);
    assertThat(boundSql.hasAdditionalParameter(paramName)).isTrue();
  }

  @Test
  void testSetAdditionalParameter() {
    String paramName = "testParam";
    Object paramValue = "testValue";
    SwiftLakeBoundSql boundSql = new SwiftLakeBoundSql(mockConfiguration, mockDelegate, sql);
    boundSql.setAdditionalParameter(paramName, paramValue);
    verify(mockDelegate).setAdditionalParameter(paramName, paramValue);
  }

  @Test
  void testGetAdditionalParameter() {
    String paramName = "testParam";
    Object paramValue = "testValue";
    when(mockDelegate.getAdditionalParameter(paramName)).thenReturn(paramValue);
    SwiftLakeBoundSql boundSql = new SwiftLakeBoundSql(mockConfiguration, mockDelegate, sql);
    assertThat(boundSql.getAdditionalParameter(paramName)).isEqualTo(paramValue);
  }

  @Test
  void testNullConfiguration() {
    assertThatThrownBy(() -> new SwiftLakeBoundSql(null, mockDelegate, sql))
        .isInstanceOf(NullPointerException.class);
  }
}
