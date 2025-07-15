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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.mybatis.type.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.transaction.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SwiftLakeMybatisConfigurationTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private Transaction mockTransaction;

  @Test
  void testDefaultConstructor() {
    SwiftLakeMybatisConfiguration configuration = new SwiftLakeMybatisConfiguration();

    assertThat(configuration.getTypeHandlerRegistry().getTypeHandler(LocalDate.class))
        .isInstanceOf(LocalDateTypeHandler.class);
    assertThat(configuration.getTypeHandlerRegistry().getTypeHandler(LocalDateTime.class))
        .isInstanceOf(LocalDateTimeTypeHandler.class);
    assertThat(configuration.getTypeHandlerRegistry().getTypeHandler(LocalTime.class))
        .isInstanceOf(LocalTimeTypeHandler.class);
    assertThat(configuration.getTypeHandlerRegistry().getTypeHandler(OffsetDateTime.class))
        .isInstanceOf(OffsetDateTimeTypeHandler.class);
    assertThat(configuration.getTypeHandlerRegistry().getTypeHandler(Float.class))
        .isInstanceOf(FloatTypeHandler.class);
    assertThat(configuration.getTypeHandlerRegistry().getTypeHandler(Double.class))
        .isInstanceOf(DoubleTypeHandler.class);

    assertThat(configuration.getSwiftLakeEngine()).isNull();
  }

  @Test
  void testConstructorWithSwiftLakeEngine() {
    SwiftLakeMybatisConfiguration configuration =
        new SwiftLakeMybatisConfiguration(mockSwiftLakeEngine);

    assertThat(configuration.getTypeHandlerRegistry().getTypeHandler(LocalDate.class))
        .isInstanceOf(LocalDateTypeHandler.class);
    assertThat(configuration.getTypeHandlerRegistry().getTypeHandler(LocalDateTime.class))
        .isInstanceOf(LocalDateTimeTypeHandler.class);
    assertThat(configuration.getTypeHandlerRegistry().getTypeHandler(LocalTime.class))
        .isInstanceOf(LocalTimeTypeHandler.class);
    assertThat(configuration.getTypeHandlerRegistry().getTypeHandler(OffsetDateTime.class))
        .isInstanceOf(OffsetDateTimeTypeHandler.class);
    assertThat(configuration.getTypeHandlerRegistry().getTypeHandler(Float.class))
        .isInstanceOf(FloatTypeHandler.class);
    assertThat(configuration.getTypeHandlerRegistry().getTypeHandler(Double.class))
        .isInstanceOf(DoubleTypeHandler.class);

    assertThat(configuration.getSwiftLakeEngine()).isEqualTo(mockSwiftLakeEngine);
  }

  @Test
  void testNewExecutor() {
    SwiftLakeMybatisConfiguration configuration =
        new SwiftLakeMybatisConfiguration(mockSwiftLakeEngine);
    Executor executor = configuration.newExecutor(mockTransaction, ExecutorType.SIMPLE);

    assertThat(executor).isInstanceOf(SwiftLakeExecutor.class);
    assertThat(((SwiftLakeExecutor) executor))
        .extracting("swiftLakeEngine")
        .isEqualTo(mockSwiftLakeEngine);
  }

  @Test
  void testSetAndGetSwiftLakeEngine() {
    SwiftLakeMybatisConfiguration configuration = new SwiftLakeMybatisConfiguration();

    configuration.setSwiftLakeEngine(mockSwiftLakeEngine);
    assertThat(configuration.getSwiftLakeEngine()).isEqualTo(mockSwiftLakeEngine);

    SwiftLakeEngine anotherEngine = mock(SwiftLakeEngine.class);
    configuration.setSwiftLakeEngine(anotherEngine);
    assertThat(configuration.getSwiftLakeEngine()).isEqualTo(anotherEngine);
  }

  @Test
  void testNewExecutorWithNullSwiftLakeEngine() {
    SwiftLakeMybatisConfiguration configuration = new SwiftLakeMybatisConfiguration();
    Executor executor = configuration.newExecutor(mockTransaction, ExecutorType.SIMPLE);

    assertThat(executor).isInstanceOf(SwiftLakeExecutor.class);
    assertThat(((SwiftLakeExecutor) executor)).extracting("swiftLakeEngine").isNull();
  }

  @Test
  void testNewExecutorIgnoresExecutorType() {
    SwiftLakeMybatisConfiguration configuration =
        new SwiftLakeMybatisConfiguration(mockSwiftLakeEngine);

    Executor simpleExecutor = configuration.newExecutor(mockTransaction, ExecutorType.SIMPLE);
    Executor reuseExecutor = configuration.newExecutor(mockTransaction, ExecutorType.REUSE);
    Executor batchExecutor = configuration.newExecutor(mockTransaction, ExecutorType.BATCH);

    assertThat(simpleExecutor).isInstanceOf(SwiftLakeExecutor.class);
    assertThat(reuseExecutor).isInstanceOf(SwiftLakeExecutor.class);
    assertThat(batchExecutor).isInstanceOf(SwiftLakeExecutor.class);
  }
}
