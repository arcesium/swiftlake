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

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.mybatis.type.DoubleTypeHandler;
import com.arcesium.swiftlake.mybatis.type.FloatTypeHandler;
import com.arcesium.swiftlake.mybatis.type.LocalDateTimeTypeHandler;
import com.arcesium.swiftlake.mybatis.type.LocalDateTypeHandler;
import com.arcesium.swiftlake.mybatis.type.LocalTimeTypeHandler;
import com.arcesium.swiftlake.mybatis.type.OffsetDateTimeTypeHandler;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.transaction.Transaction;

/**
 * SwiftLakeMybatisConfiguration extends MyBatis Configuration to provide custom functionality for
 * SwiftLake, including custom type handlers and executors.
 */
public class SwiftLakeMybatisConfiguration extends Configuration {
  private SwiftLakeEngine swiftLakeEngine;

  /**
   * Default constructor. Initializes the SwiftLakeMybatisConfiguration with custom type handlers.
   */
  public SwiftLakeMybatisConfiguration() {
    super();
    init();
  }

  /**
   * Constructor that takes a SwiftLakeEngine. Initializes the SwiftLakeMybatisConfiguration with
   * custom type handlers and sets the SwiftLakeEngine.
   *
   * @param swiftLakeEngine The SwiftLakeEngine to be used.
   */
  public SwiftLakeMybatisConfiguration(SwiftLakeEngine swiftLakeEngine) {
    super();
    init();
    this.swiftLakeEngine = swiftLakeEngine;
  }

  /** Initializes the SwiftLakeMybatisConfiguration by registering custom type handlers. */
  private void init() {
    this.getTypeHandlerRegistry().register(LocalDateTypeHandler.class);
    this.getTypeHandlerRegistry().register(LocalDateTimeTypeHandler.class);
    this.getTypeHandlerRegistry().register(LocalTimeTypeHandler.class);
    this.getTypeHandlerRegistry().register(OffsetDateTimeTypeHandler.class);
    this.getTypeHandlerRegistry().register(FloatTypeHandler.class);
    this.getTypeHandlerRegistry().register(DoubleTypeHandler.class);
  }

  /**
   * Creates a new SwiftLakeExecutor.
   *
   * @param transaction The Transaction to be used by the executor.
   * @param executorType The ExecutorType (ignored in this implementation).
   * @return A new SwiftLakeExecutor instance.
   */
  @Override
  public Executor newExecutor(Transaction transaction, ExecutorType executorType) {
    return new SwiftLakeExecutor(this, transaction, swiftLakeEngine);
  }

  /**
   * Sets the SwiftLakeEngine.
   *
   * @param swiftLakeEngine The SwiftLakeEngine to be set.
   */
  public void setSwiftLakeEngine(SwiftLakeEngine swiftLakeEngine) {
    this.swiftLakeEngine = swiftLakeEngine;
  }

  /**
   * Gets the current SwiftLakeEngine.
   *
   * @return The current SwiftLakeEngine.
   */
  public SwiftLakeEngine getSwiftLakeEngine() {
    return swiftLakeEngine;
  }
}
