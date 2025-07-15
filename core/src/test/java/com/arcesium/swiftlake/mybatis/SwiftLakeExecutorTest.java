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
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.sql.IcebergScanExecutor;
import com.arcesium.swiftlake.sql.TableFilter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SwiftLakeExecutorTest {

  @Mock private Configuration mockConfiguration;
  @Mock private Transaction mockTransaction;
  @Mock private SwiftLakeEngine mockSwiftLakeEngine;
  @Mock private MappedStatement mockMappedStatement;
  @Mock private ResultHandler<?> mockResultHandler;
  @Mock private BoundSql mockBoundSql;
  @Mock private IcebergScanExecutor mockIcebergScanExecutor;
  @Mock private StatementHandler mockStatementHandler;
  @Mock private Connection mockConnection;
  @Mock private Statement mockStatement;
  @Mock private Log statementLog;

  private SwiftLakeExecutor executor;

  @BeforeEach
  void setUp() throws Exception {
    executor = new SwiftLakeExecutor(mockConfiguration, mockTransaction, mockSwiftLakeEngine);
  }

  @Test
  void testQueryWithoutTableFiltersOrTransformer() throws SQLException {
    Object parameter = new Object();
    List<Object> expectedResult = Collections.singletonList(new Object());
    configureStatementExecution(parameter, true, expectedResult);

    List<Object> result =
        executor.query(mockMappedStatement, parameter, RowBounds.DEFAULT, mockResultHandler);

    assertThat(result).isEqualTo(expectedResult);

    verify(mockMappedStatement).getBoundSql(parameter);
    verify(mockStatementHandler).query(mockStatement, mockResultHandler);
  }

  @Test
  void testQueryWithTableFiltersAndTransformer() throws SQLException {
    QueryProperties<Object> queryProperties =
        new QueryProperties<>(
            Collections.singletonList(new TableFilter("testTable", (String) null)),
            new Object(),
            sql -> sql + " TRANSFORMED");
    when(mockBoundSql.getSql()).thenReturn("SELECT * FROM testTable");
    when(mockSwiftLakeEngine.getIcebergScanExecutor()).thenReturn(mockIcebergScanExecutor);
    when(mockIcebergScanExecutor.executeTableScansAndUpdateSql(
            anyString(), anyList(), any(), anyBoolean()))
        .thenReturn(Pair.of("SCANNED SQL", new ArrayList<>()));

    List<Object> expectedResult = Collections.singletonList(new Object());
    configureStatementExecution(queryProperties.getParameter(), true, expectedResult);

    List<Object> result =
        executor.query(mockMappedStatement, queryProperties, RowBounds.DEFAULT, mockResultHandler);

    assertThat(result).isEqualTo(expectedResult);
    verify(mockIcebergScanExecutor)
        .executeTableScansAndUpdateSql(anyString(), anyList(), any(), anyBoolean());
    verify(mockMappedStatement).getBoundSql(queryProperties.getParameter());
    verify(mockStatementHandler).query(mockStatement, mockResultHandler);
  }

  @Test
  void testDoUpdateWithoutTableFiltersOrTransformer() throws SQLException {
    Object parameter = new Object();
    configureStatementExecution(parameter, false, null);

    int result = executor.doUpdate(mockMappedStatement, parameter);

    assertThat(result).isEqualTo(1);
    verify(mockMappedStatement).getBoundSql(parameter);
    verify(mockStatementHandler).update(mockStatement);
  }

  @Test
  void testDoUpdateWithTableFiltersAndTransformer() throws SQLException {
    QueryProperties<Object> queryProperties =
        new QueryProperties<>(
            Collections.singletonList(new TableFilter("testTable", (String) null)),
            new Object(),
            sql -> sql + " TRANSFORMED");

    configureStatementExecution(queryProperties.getParameter(), false, null);
    when(mockBoundSql.getSql()).thenReturn("UPDATE testTable SET column = value");
    when(mockSwiftLakeEngine.getIcebergScanExecutor()).thenReturn(mockIcebergScanExecutor);
    when(mockIcebergScanExecutor.executeTableScansAndUpdateSql(
            anyString(), anyList(), any(), anyBoolean()))
        .thenReturn(Pair.of("SCANNED SQL", new ArrayList<>()));

    int result = executor.doUpdate(mockMappedStatement, queryProperties);

    assertThat(result).isEqualTo(1);
    verify(mockIcebergScanExecutor)
        .executeTableScansAndUpdateSql(anyString(), anyList(), any(), anyBoolean());
    verify(mockMappedStatement).getBoundSql(queryProperties.getParameter());
    verify(mockStatementHandler).update(mockStatement);
  }

  private void configureStatementExecution(
      Object parameter, boolean isQuery, List<Object> expectedResult) throws SQLException {
    when(mockMappedStatement.getConfiguration()).thenReturn(mockConfiguration);
    when(mockMappedStatement.getBoundSql(parameter)).thenReturn(mockBoundSql);
    when(mockBoundSql.getParameterObject()).thenReturn(parameter);
    when(mockMappedStatement.getStatementLog()).thenReturn(statementLog);
    when(mockConfiguration.newStatementHandler(
            eq(executor),
            eq(mockMappedStatement),
            eq(parameter),
            any(),
            isQuery ? eq(mockResultHandler) : isNull(),
            any(BoundSql.class)))
        .thenReturn(mockStatementHandler);
    when(mockTransaction.getConnection()).thenReturn(mockConnection);
    when(mockStatementHandler.prepare(any(), anyInt())).thenReturn(mockStatement);
    if (isQuery) {
      when(mockStatementHandler.query(mockStatement, mockResultHandler)).thenReturn(expectedResult);
    } else {
      when(mockStatementHandler.update(mockStatement)).thenReturn(1);
    }
  }
}
