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
package com.arcesium.swiftlake.sql;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.arcesium.swiftlake.SwiftLakeEngine;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SwiftLakeDataSourceTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  private SwiftLakeDataSource dataSource;

  @BeforeEach
  void setUp() {
    dataSource = new SwiftLakeDataSource(mockSwiftLakeEngine, true);
  }

  @Test
  void testGetConnection() throws SQLException {
    SwiftLakeConnection mockConnection = mock(SwiftLakeConnection.class);
    when(mockSwiftLakeEngine.createConnection(true)).thenReturn(mockConnection);

    Connection result = dataSource.getConnection();

    assertThat(result).isEqualTo(mockConnection);
    verify(mockSwiftLakeEngine).createConnection(true);
  }

  @Test
  void testGetConnectionWithUsernameAndPassword() {
    assertThatThrownBy(() -> dataSource.getConnection("username", "password"))
        .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  void testGetLogWriter() {
    assertThatThrownBy(() -> dataSource.getLogWriter())
        .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  void testSetLogWriter() {
    assertThatThrownBy(() -> dataSource.setLogWriter(null))
        .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  void testSetLoginTimeout() throws SQLException {
    dataSource.setLoginTimeout(30);

    assertThat(DriverManager.getLoginTimeout()).isEqualTo(30);
  }

  @Test
  void testGetLoginTimeout() throws SQLException {
    DriverManager.setLoginTimeout(60);

    int timeout = dataSource.getLoginTimeout();

    assertThat(timeout).isEqualTo(60);
  }

  @Test
  void testGetParentLogger() {
    assertThatThrownBy(() -> dataSource.getParentLogger())
        .isInstanceOf(SQLFeatureNotSupportedException.class)
        .hasMessage("no logger");
  }

  @Test
  void testUnwrap() {
    assertThatThrownBy(() -> dataSource.unwrap(Connection.class))
        .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  void testIsWrapperFor() throws SQLException {
    boolean result = dataSource.isWrapperFor(DataSource.class);
    assertThat(result).isFalse();
  }
}
