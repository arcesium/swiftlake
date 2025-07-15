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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.sql.SwiftLakeConnection;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.TransactionIsolationLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SwiftLakeSqlSessionFactoryTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private SwiftLakeMybatisConfiguration mockConfiguration;

  @Mock private SqlSessionFactory mockSqlSessionFactory;

  @Mock private SwiftLakeConnection mockConnection;

  @Mock private SqlSession mockSqlSession;

  @Mock private MappedStatement mockMappedStatement;

  @Mock private BoundSql mockBoundSql;

  private SwiftLakeSqlSessionFactory factory;

  @BeforeEach
  void setUp() {
    when(mockConfiguration.getSwiftLakeEngine()).thenReturn(mockSwiftLakeEngine);
    factory = new SwiftLakeSqlSessionFactory(mockConfiguration, true);
    try {
      var field = SwiftLakeSqlSessionFactory.class.getDeclaredField("sqlSessionFactory");
      field.setAccessible(true);
      field.set(factory, mockSqlSessionFactory);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testConstructorWithConfigPath() {
    try (MockedStatic<Resources> mockedResources = mockStatic(Resources.class)) {
      // Mock the static method
      InputStream mockInputStream =
          new ByteArrayInputStream(
              "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<!DOCTYPE configuration PUBLIC \"-//mybatis.org//DTD Config 3.0//EN\" \"https://mybatis.org/dtd/mybatis-3-config.dtd\">\n<configuration>\n</configuration>"
                  .getBytes());
      mockedResources
          .when(() -> Resources.getResourceAsStream("mybatis-config.xml"))
          .thenReturn(mockInputStream);

      assertThatCode(
              () -> new SwiftLakeSqlSessionFactory(mockSwiftLakeEngine, "mybatis-config.xml", true))
          .doesNotThrowAnyException();
    }
  }

  @Test
  void testGetSql() {
    String statement = "testStatement";
    Object parameter = new Object();
    String expectedSql = "SELECT * FROM test";

    when(mockSqlSessionFactory.getConfiguration()).thenReturn(mockConfiguration);
    when(mockConfiguration.getMappedStatement(statement)).thenReturn(mockMappedStatement);
    when(mockMappedStatement.getBoundSql(parameter)).thenReturn(mockBoundSql);
    when(mockBoundSql.getSql()).thenReturn(expectedSql);

    String result = factory.getSql(statement, parameter);

    assertThat(result).isEqualTo(expectedSql);
  }

  @Test
  void testGetSqlWithNonExistentStatement() {
    when(mockSqlSessionFactory.getConfiguration()).thenReturn(mockConfiguration);
    when(mockConfiguration.getMappedStatement("nonExistentStatement")).thenReturn(null);

    assertThatThrownBy(() -> factory.getSql("nonExistentStatement", null))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Mybatis statement does not exist");
  }

  @Test
  void testOpenSession() {
    when(mockSwiftLakeEngine.createConnection(true)).thenReturn(mockConnection);
    when(mockSqlSessionFactory.openSession(mockConnection)).thenReturn(mockSqlSession);

    SqlSession result = factory.openSession();

    assertThat(result).isEqualTo(mockSqlSession);
  }

  @Test
  void testOpenSessionWithTableProcessing() {
    when(mockSwiftLakeEngine.createConnection(true)).thenReturn(mockConnection);
    when(mockSqlSessionFactory.openSession(mockConnection)).thenReturn(mockSqlSession);

    SqlSession result = factory.openSessionWithTableProcessing();

    assertThat(result).isEqualTo(mockSqlSession);
  }

  @Test
  void testOpenSessionWithoutTableProcessing() {
    when(mockSwiftLakeEngine.createConnection(false)).thenReturn(mockConnection);
    when(mockSqlSessionFactory.openSession(mockConnection)).thenReturn(mockSqlSession);

    SqlSession result = factory.openSessionWithoutTableProcessing();

    assertThat(result).isEqualTo(mockSqlSession);
  }

  @Test
  void testOpenSessionWithAutoCommit() {
    when(mockSwiftLakeEngine.createConnection(true)).thenReturn(mockConnection);
    when(mockSqlSessionFactory.openSession(mockConnection)).thenReturn(mockSqlSession);

    SqlSession result = factory.openSession(true);

    assertThat(result).isEqualTo(mockSqlSession);
  }

  @Test
  void testOpenSessionWithConnection() {
    when(mockSqlSessionFactory.openSession(mockConnection)).thenReturn(mockSqlSession);

    SqlSession result = factory.openSession(mockConnection);

    assertThat(result).isEqualTo(mockSqlSession);
  }

  @Test
  void testOpenSessionWithTransactionIsolationLevel() {
    when(mockSwiftLakeEngine.createConnection(true)).thenReturn(mockConnection);
    when(mockSqlSessionFactory.openSession(mockConnection)).thenReturn(mockSqlSession);

    SqlSession result = factory.openSession(TransactionIsolationLevel.READ_COMMITTED);

    assertThat(result).isEqualTo(mockSqlSession);
  }

  @Test
  void testOpenSessionWithExecutorType() {
    when(mockSwiftLakeEngine.createConnection(true)).thenReturn(mockConnection);
    when(mockSqlSessionFactory.openSession(mockConnection)).thenReturn(mockSqlSession);

    SqlSession result = factory.openSession(ExecutorType.SIMPLE);

    assertThat(result).isEqualTo(mockSqlSession);
  }

  @Test
  void testOpenSessionWithExecutorTypeAndAutoCommit() {
    when(mockSwiftLakeEngine.createConnection(true)).thenReturn(mockConnection);
    when(mockSqlSessionFactory.openSession(mockConnection)).thenReturn(mockSqlSession);

    SqlSession result = factory.openSession(ExecutorType.SIMPLE, true);

    assertThat(result).isEqualTo(mockSqlSession);
  }

  @Test
  void testOpenSessionWithExecutorTypeAndTransactionIsolationLevel() {
    when(mockSwiftLakeEngine.createConnection(true)).thenReturn(mockConnection);
    when(mockSqlSessionFactory.openSession(mockConnection)).thenReturn(mockSqlSession);

    SqlSession result =
        factory.openSession(ExecutorType.SIMPLE, TransactionIsolationLevel.READ_COMMITTED);

    assertThat(result).isEqualTo(mockSqlSession);
  }

  @Test
  void testOpenSessionWithExecutorTypeAndConnection() {
    when(mockSqlSessionFactory.openSession(mockConnection)).thenReturn(mockSqlSession);

    SqlSession result = factory.openSession(ExecutorType.SIMPLE, mockConnection);

    assertThat(result).isEqualTo(mockSqlSession);
  }

  @Test
  void testGetConfiguration() {
    Configuration mockConfig = mock(Configuration.class);
    when(mockSqlSessionFactory.getConfiguration()).thenReturn(mockConfig);

    Configuration result = factory.getConfiguration();

    assertThat(result).isEqualTo(mockConfig);
  }
}
