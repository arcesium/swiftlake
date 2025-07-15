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
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import org.apache.ibatis.builder.xml.XMLConfigBuilder;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.TransactionIsolationLevel;
import org.apache.ibatis.session.defaults.DefaultSqlSessionFactory;

/**
 * A custom implementation of SqlSessionFactory for SwiftLake. This class wraps a standard MyBatis
 * SqlSessionFactory and adds SwiftLake-specific functionality.
 */
public class SwiftLakeSqlSessionFactory implements SqlSessionFactory {
  private SqlSessionFactory sqlSessionFactory;
  private SwiftLakeEngine swiftLakeEngine;
  private boolean processTables;

  /**
   * Constructs a SwiftLakeSqlSessionFactory with the given SwiftLakeEngine, configuration path, and
   * table processing flag.
   *
   * @param swiftLakeEngine The SwiftLakeEngine to use
   * @param configPath The path to the MyBatis configuration file
   * @param processTables Flag indicating whether to process tables
   */
  public SwiftLakeSqlSessionFactory(
      SwiftLakeEngine swiftLakeEngine, String configPath, boolean processTables) {
    this.swiftLakeEngine = swiftLakeEngine;
    this.processTables = processTables;
    init(configPath);
  }

  /**
   * Constructs a SwiftLakeSqlSessionFactory with the given SwiftLakeMybatisConfiguration and table
   * processing flag.
   *
   * @param configuration The SwiftLakeMybatisConfiguration to use
   * @param processTables Flag indicating whether to process tables
   */
  public SwiftLakeSqlSessionFactory(
      SwiftLakeMybatisConfiguration configuration, boolean processTables) {
    this.swiftLakeEngine = configuration.getSwiftLakeEngine();
    this.sqlSessionFactory = new DefaultSqlSessionFactory(configuration);
    this.processTables = processTables;
  }

  /**
   * Initializes the SqlSessionFactory using the provided configuration path.
   *
   * @param path The path to the MyBatis configuration file
   */
  private void init(String path) {
    String resource = path;
    InputStream inputStream = null;
    try {
      inputStream = Resources.getResourceAsStream(resource);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
      XMLConfigBuilder parser =
          new XMLConfigBuilder(SwiftLakeMybatisConfiguration.class, inputStream, null, null);
      SwiftLakeMybatisConfiguration config = (SwiftLakeMybatisConfiguration) parser.parse();
      config.setSwiftLakeEngine(swiftLakeEngine);
      this.sqlSessionFactory = new DefaultSqlSessionFactory(config);
    } catch (Exception e) {
      throw new SwiftLakeException(e, "An error occurred while building SqlSessionFactory object.");
    } finally {
      ErrorContext.instance().reset();
      try {
        if (inputStream != null) {
          inputStream.close();
        }
      } catch (IOException e) {
        // Intentionally ignore. Prefer previous error.
      }
    }
  }

  /**
   * Retrieves the SQL for a given statement and parameter.
   *
   * @param statement The statement identifier
   * @param parameter The parameter object
   * @return The SQL string
   * @throws ValidationException if the statement does not exist
   */
  public String getSql(String statement, Object parameter) {
    MappedStatement ms = sqlSessionFactory.getConfiguration().getMappedStatement(statement);
    if (ms == null)
      throw new ValidationException("Mybatis statement does not exist - %s", statement);
    BoundSql boundSql = ms.getBoundSql(parameter);
    return boundSql.getSql();
  }

  /**
   * Opens a new SqlSession with default settings.
   *
   * @return A new SqlSession
   */
  @Override
  public SqlSession openSession() {
    return sqlSessionFactory.openSession(swiftLakeEngine.createConnection(processTables));
  }

  /**
   * Opens a new SqlSession with table processing enabled.
   *
   * @return A new SqlSession with table processing
   */
  public SqlSession openSessionWithTableProcessing() {
    return sqlSessionFactory.openSession(swiftLakeEngine.createConnection(true));
  }

  /**
   * Opens a new SqlSession without table processing.
   *
   * @return A new SqlSession without table processing
   */
  public SqlSession openSessionWithoutTableProcessing() {
    return sqlSessionFactory.openSession(swiftLakeEngine.createConnection(false));
  }

  @Override
  public SqlSession openSession(boolean autoCommit) {
    return openSession();
  }

  @Override
  public SqlSession openSession(Connection connection) {
    return sqlSessionFactory.openSession(connection);
  }

  @Override
  public SqlSession openSession(TransactionIsolationLevel level) {
    return openSession();
  }

  @Override
  public SqlSession openSession(ExecutorType execType) {
    return openSession();
  }

  @Override
  public SqlSession openSession(ExecutorType execType, boolean autoCommit) {
    return openSession();
  }

  @Override
  public SqlSession openSession(ExecutorType execType, TransactionIsolationLevel level) {
    return openSession();
  }

  @Override
  public SqlSession openSession(ExecutorType execType, Connection connection) {
    return sqlSessionFactory.openSession(connection);
  }

  @Override
  public Configuration getConfiguration() {
    return sqlSessionFactory.getConfiguration();
  }
}
