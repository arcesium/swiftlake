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
package com.arcesium.swiftlake.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.SwiftLakeEngine;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

@ExtendWith(MockitoExtension.class)
class AwsUtilTest {

  @Mock private SwiftLakeEngine mockSwiftLakeEngine;

  @Mock private DuckDBConnection mockConnection;

  @Mock private Statement mockStatement;

  @Mock private AwsCredentialsProvider mockCredentialsProvider;

  @Mock private AwsCredentials mockAwsCredentials;

  @Mock private AwsSessionCredentials mockAwsSessionCredentials;

  private Map<String, String> properties;
  private String prevDuckDBExtPath;

  @BeforeEach
  void setUp() throws SQLException {
    properties = new HashMap<>();
    properties.put(AwsClientProperties.CLIENT_REGION, "us-west-2");
    properties.put(S3FileIOProperties.ENDPOINT, "https://s3.us-west-2.amazonaws.com");
    properties.put(SwiftLakeS3FileIOProperties.DUCKDB_S3_EXTENSION_ENABLED, "true");
    prevDuckDBExtPath = System.getProperty("duckdb.extensions.path");
    System.clearProperty("duckdb.extensions.path");
    CredentialProvider.credentialsProvider = null;
  }

  @AfterEach
  void teardown() {
    if (prevDuckDBExtPath != null) {
      System.setProperty("duckdb.extensions.path", prevDuckDBExtPath);
    } else {
      System.clearProperty("duckdb.extensions.path");
    }
  }

  @Test
  void testCreateS3CrtAsyncClient() {
    // Add S3 client properties
    properties.put(SwiftLakeS3FileIOProperties.CRT_TARGET_THROUGHPUT_GBPS, "20.0");
    properties.put(SwiftLakeS3FileIOProperties.CRT_MAX_CONCURRENCY, "100");
    properties.put(S3FileIOProperties.MULTIPART_SIZE, "10485760"); // 10MB
    properties.put(SwiftLakeS3FileIOProperties.CRT_MULTIPART_THRESHOLD_BYTES, "20971520"); // 20MB
    properties.put(
        SwiftLakeS3FileIOProperties.CRT_MAX_NATIVE_MEMORY_LIMIT_BYTES, "104857600"); // 100MB
    properties.put(S3FileIOProperties.CROSS_REGION_ACCESS_ENABLED, "true");
    properties.put(
        S3FileIOProperties.ACCESS_POINTS_PREFIX + "my-bucket1",
        "arn:aws:s3::<ACCOUNT_ID>:accesspoint/<MRAP_ALIAS>");
    properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
    properties.put(S3FileIOProperties.S3_RETRY_NUM_RETRIES, "5");

    try (MockedStatic<S3AsyncClient> mockS3AsyncClient = mockStatic(S3AsyncClient.class)) {
      S3CrtAsyncClientBuilder mockBuilder = mock(S3CrtAsyncClientBuilder.class);
      S3AsyncClient mockClient = mock(S3AsyncClient.class);

      mockS3AsyncClient.when(S3AsyncClient::crtBuilder).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockClient);

      when(mockBuilder.region(any())).thenReturn(mockBuilder);
      when(mockBuilder.targetThroughputInGbps(anyDouble())).thenReturn(mockBuilder);
      when(mockBuilder.maxConcurrency(anyInt())).thenReturn(mockBuilder);
      when(mockBuilder.minimumPartSizeInBytes(anyLong())).thenReturn(mockBuilder);
      when(mockBuilder.thresholdInBytes(anyLong())).thenReturn(mockBuilder);
      when(mockBuilder.maxNativeMemoryLimitInBytes(anyLong())).thenReturn(mockBuilder);
      when(mockBuilder.endpointOverride(any())).thenReturn(mockBuilder);
      when(mockBuilder.crossRegionAccessEnabled(anyBoolean())).thenReturn(mockBuilder);
      when(mockBuilder.accelerate(anyBoolean())).thenReturn(mockBuilder);
      when(mockBuilder.forcePathStyle(anyBoolean())).thenReturn(mockBuilder);
      when(mockBuilder.retryConfiguration(any(Consumer.class))).thenReturn(mockBuilder);
      when(mockBuilder.applyMutation(any())).thenReturn(mockBuilder);

      S3AsyncClient result = AwsUtil.createS3CrtAsyncClient(properties);

      assertThat(result).isEqualTo(mockClient);
      verify(mockBuilder).region(Region.of("us-west-2"));
      verify(mockBuilder).targetThroughputInGbps(20.0);
      verify(mockBuilder).maxConcurrency(100);
      verify(mockBuilder).minimumPartSizeInBytes(10485760L);
      verify(mockBuilder).thresholdInBytes(20971520L);
      verify(mockBuilder).maxNativeMemoryLimitInBytes(104857600L);
      verify(mockBuilder).endpointOverride(URI.create("https://s3.us-west-2.amazonaws.com"));
      verify(mockBuilder).crossRegionAccessEnabled(true);
      verify(mockBuilder).forcePathStyle(true);
      verify(mockBuilder).retryConfiguration(any(Consumer.class));
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        S3FileIOProperties.SSE_TYPE_NONE,
        S3FileIOProperties.SSE_TYPE_KMS,
        S3FileIOProperties.DSSE_TYPE_KMS,
        S3FileIOProperties.SSE_TYPE_S3,
        S3FileIOProperties.SSE_TYPE_CUSTOM
      })
  void testConfigureEncryptionForPutRequest(String sseType) {
    properties.put(S3FileIOProperties.SSE_TYPE, sseType);

    if (sseType.equals(S3FileIOProperties.SSE_TYPE_KMS)
        || sseType.equals(S3FileIOProperties.DSSE_TYPE_KMS)
        || sseType.equals(S3FileIOProperties.SSE_TYPE_CUSTOM)) {
      properties.put(S3FileIOProperties.SSE_KEY, "test-key");
    }

    if (sseType.equals(S3FileIOProperties.SSE_TYPE_CUSTOM)) {
      properties.put(S3FileIOProperties.SSE_MD5, "test-md5");
    }

    S3FileIOProperties s3FileIOProperties = new SwiftLakeS3FileIOProperties(properties);
    PutObjectRequest.Builder requestBuilder = mock(PutObjectRequest.Builder.class);

    switch (sseType) {
      case S3FileIOProperties.SSE_TYPE_NONE:
        break;
      case S3FileIOProperties.SSE_TYPE_KMS, S3FileIOProperties.DSSE_TYPE_KMS:
        when(requestBuilder.serverSideEncryption(any(ServerSideEncryption.class)))
            .thenReturn(requestBuilder);
        when(requestBuilder.ssekmsKeyId(any())).thenReturn(requestBuilder);
        break;
      case S3FileIOProperties.SSE_TYPE_S3:
        when(requestBuilder.serverSideEncryption(any(ServerSideEncryption.class)))
            .thenReturn(requestBuilder);
        break;
      case S3FileIOProperties.SSE_TYPE_CUSTOM:
        when(requestBuilder.sseCustomerAlgorithm(any())).thenReturn(requestBuilder);
        when(requestBuilder.sseCustomerKey(any())).thenReturn(requestBuilder);
        when(requestBuilder.sseCustomerKeyMD5(any())).thenReturn(requestBuilder);
        break;
    }

    AwsUtil.configureEncryption(s3FileIOProperties, requestBuilder);

    switch (sseType) {
      case S3FileIOProperties.SSE_TYPE_NONE:
        verify(requestBuilder, never()).serverSideEncryption(any(ServerSideEncryption.class));
        break;
      case S3FileIOProperties.SSE_TYPE_KMS:
        verify(requestBuilder).serverSideEncryption(ServerSideEncryption.AWS_KMS);
        verify(requestBuilder).ssekmsKeyId("test-key");
        break;
      case S3FileIOProperties.DSSE_TYPE_KMS:
        verify(requestBuilder).serverSideEncryption(ServerSideEncryption.AWS_KMS_DSSE);
        verify(requestBuilder).ssekmsKeyId("test-key");
        break;
      case S3FileIOProperties.SSE_TYPE_S3:
        verify(requestBuilder).serverSideEncryption(ServerSideEncryption.AES256);
        break;
      case S3FileIOProperties.SSE_TYPE_CUSTOM:
        verify(requestBuilder).sseCustomerAlgorithm(ServerSideEncryption.AES256.name());
        verify(requestBuilder).sseCustomerKey("test-key");
        verify(requestBuilder).sseCustomerKeyMD5("test-md5");
        break;
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {S3FileIOProperties.SSE_TYPE_NONE, S3FileIOProperties.SSE_TYPE_CUSTOM})
  void testConfigureEncryptionForGetRequest(String sseType) {
    properties.put(S3FileIOProperties.SSE_TYPE, sseType);

    if (sseType.equals(S3FileIOProperties.SSE_TYPE_CUSTOM)) {
      properties.put(S3FileIOProperties.SSE_KEY, "test-key");
      properties.put(S3FileIOProperties.SSE_MD5, "test-md5");
    }

    S3FileIOProperties s3FileIOProperties = new SwiftLakeS3FileIOProperties(properties);
    GetObjectRequest.Builder requestBuilder = mock(GetObjectRequest.Builder.class);

    if (sseType.equals(S3FileIOProperties.SSE_TYPE_CUSTOM)) {
      when(requestBuilder.sseCustomerAlgorithm(any())).thenReturn(requestBuilder);
      when(requestBuilder.sseCustomerKey(any())).thenReturn(requestBuilder);
      when(requestBuilder.sseCustomerKeyMD5(any())).thenReturn(requestBuilder);
    }

    AwsUtil.configureEncryption(s3FileIOProperties, requestBuilder);

    if (sseType.equals(S3FileIOProperties.SSE_TYPE_CUSTOM)) {
      verify(requestBuilder).sseCustomerAlgorithm(ServerSideEncryption.AES256.name());
      verify(requestBuilder).sseCustomerKey("test-key");
      verify(requestBuilder).sseCustomerKeyMD5("test-md5");
    } else {
      verify(requestBuilder, never()).sseCustomerAlgorithm(any());
    }
  }

  @Test
  void testConfigureEncryptionWithInvalidType() {
    properties.put(S3FileIOProperties.SSE_TYPE, "INVALID");
    S3FileIOProperties s3FileIOProperties = new SwiftLakeS3FileIOProperties(properties);
    PutObjectRequest.Builder requestBuilder = mock(PutObjectRequest.Builder.class);

    assertThatThrownBy(() -> AwsUtil.configureEncryption(s3FileIOProperties, requestBuilder))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot support given S3 encryption type: INVALID");
  }

  @Test
  void testConfigurePermission() {
    properties.put(S3FileIOProperties.ACL, "bucket-owner-full-control");
    S3FileIOProperties s3FileIOProperties = new SwiftLakeS3FileIOProperties(properties);
    PutObjectRequest.Builder requestBuilder = mock(PutObjectRequest.Builder.class);

    when(requestBuilder.acl(any(ObjectCannedACL.class))).thenReturn(requestBuilder);

    AwsUtil.configurePermission(s3FileIOProperties, requestBuilder);

    verify(requestBuilder).acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL);
  }

  @Test
  void testInitializeDuckDBS3IntegrationWithRegularCredentials() throws SQLException {
    properties.put(SwiftLakeS3FileIOProperties.DUCKDB_S3_EXTENSION_ENABLED, "true");
    properties.put(
        AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER, CredentialProvider.class.getName());
    CredentialProvider.credentialsProvider = mockCredentialsProvider;
    when(mockCredentialsProvider.resolveCredentials()).thenReturn(mockAwsCredentials);
    when(mockAwsCredentials.accessKeyId()).thenReturn("access-key");
    when(mockAwsCredentials.secretAccessKey()).thenReturn("secret-key");
    when(mockSwiftLakeEngine.createDuckDBConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    doNothing()
        .when(mockSwiftLakeEngine)
        .loadDuckDBExtensionsFromClassPath(any(), anyString(), anyList());

    List<AutoCloseable> result =
        AwsUtil.initializeDuckDBS3Integration(mockSwiftLakeEngine, properties, 60);

    assertThat(result).isNotNull();
    assertThat(result).hasSize(1); // Should have one closeable for the scheduler

    // Verify loading of extensions
    verify(mockSwiftLakeEngine)
        .loadDuckDBExtensionsFromClassPath(
            eq(mockStatement), eq("/duckdb_extensions/"), eq(List.of("httpfs", "aws")));

    // Verify SQL statement execution
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockStatement).execute(sqlCaptor.capture());

    String capturedSql = sqlCaptor.getValue();
    assertThat(capturedSql).startsWith("CREATE OR REPLACE SECRET (TYPE S3,");
    assertThat(capturedSql).contains("KEY_ID 'access-key'");
    assertThat(capturedSql).contains("SECRET 'secret-key'");
  }

  @Test
  void testInitializeDuckDBS3IntegrationWithSessionCredentials() throws SQLException {
    properties.put(SwiftLakeS3FileIOProperties.DUCKDB_S3_EXTENSION_ENABLED, "true");
    properties.put(
        AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER, CredentialProvider.class.getName());
    properties.put(S3FileIOProperties.ENDPOINT, "https://s3.us-west-2.amazonaws.com");
    CredentialProvider.credentialsProvider = mockCredentialsProvider;
    when(mockCredentialsProvider.resolveCredentials()).thenReturn(mockAwsSessionCredentials);
    when(mockAwsSessionCredentials.accessKeyId()).thenReturn("session-access-key");
    when(mockAwsSessionCredentials.secretAccessKey()).thenReturn("session-secret-key");
    when(mockAwsSessionCredentials.sessionToken()).thenReturn("session-token");
    when(mockSwiftLakeEngine.createDuckDBConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    doNothing().when(mockSwiftLakeEngine).loadDuckDBExtensions(any(), anyString(), anyList());
    System.setProperty("duckdb.extensions.path", "/tmp"); // So it uses loadDuckDBExtensions

    List<AutoCloseable> result =
        AwsUtil.initializeDuckDBS3Integration(mockSwiftLakeEngine, properties, null);

    assertThat(result).isNotNull();
    assertThat(result).isEmpty(); // No scheduler created since refresh interval is null

    // Verify loading of extensions
    verify(mockSwiftLakeEngine)
        .loadDuckDBExtensions(eq(mockStatement), eq("/tmp"), eq(Arrays.asList("httpfs", "aws")));

    // Verify SQL statement execution
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockStatement).execute(sqlCaptor.capture());

    String capturedSql = sqlCaptor.getValue();
    assertThat(capturedSql).startsWith("CREATE OR REPLACE SECRET (TYPE S3,");
    assertThat(capturedSql).contains("KEY_ID 'session-access-key'");
    assertThat(capturedSql).contains("SECRET 'session-secret-key'");
    assertThat(capturedSql).contains("SESSION_TOKEN 'session-token'");
    assertThat(capturedSql).contains("ENDPOINT 's3.us-west-2.amazonaws.com'");
  }

  @Test
  void testInitializeDuckDBS3IntegrationWithExtensionsDisabled() throws SQLException {
    properties.put(SwiftLakeS3FileIOProperties.DUCKDB_S3_EXTENSION_ENABLED, "false");

    List<AutoCloseable> result =
        AwsUtil.initializeDuckDBS3Integration(mockSwiftLakeEngine, properties, 60);

    assertThat(result).isNull();
    verifyNoInteractions(mockConnection, mockStatement);
  }

  @Test
  void testInitializeDuckDBS3IntegrationWithSQLException() throws SQLException {
    when(mockSwiftLakeEngine.createDuckDBConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenThrow(new RuntimeException("Test exception"));

    assertThatThrownBy(
            () -> AwsUtil.initializeDuckDBS3Integration(mockSwiftLakeEngine, properties, 60))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Test exception");
  }

  @Test
  void testRefreshDuckDBS3CredentialsWithSwiftLakeEngine() throws Exception {
    when(mockAwsCredentials.accessKeyId()).thenReturn("access-key");
    when(mockAwsCredentials.secretAccessKey()).thenReturn("secret-key");
    when(mockCredentialsProvider.resolveCredentials()).thenReturn(mockAwsCredentials);
    when(mockSwiftLakeEngine.createDuckDBConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);

    SwiftLakeS3FileIOProperties s3Properties = new SwiftLakeS3FileIOProperties(properties);
    AwsClientProperties awsProperties = new AwsClientProperties(properties);

    // Access the private method using reflection
    Method refreshMethod =
        AwsUtil.class.getDeclaredMethod(
            "refreshDuckDBS3Credentials",
            SwiftLakeEngine.class,
            SwiftLakeS3FileIOProperties.class,
            AwsClientProperties.class,
            AwsCredentialsProvider.class);
    refreshMethod.setAccessible(true);

    // Invoke the private method
    refreshMethod.invoke(
        null, mockSwiftLakeEngine, s3Properties, awsProperties, mockCredentialsProvider);

    // Verify expected interactions
    verify(mockSwiftLakeEngine).createDuckDBConnection();
    verify(mockConnection).createStatement();

    // Verify SQL execution
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockStatement).execute(sqlCaptor.capture());

    String capturedSql = sqlCaptor.getValue();
    assertThat(capturedSql).startsWith("CREATE OR REPLACE SECRET (TYPE S3,");
    assertThat(capturedSql).contains("KEY_ID 'access-key'");
    assertThat(capturedSql).contains("SECRET 'secret-key'");
  }

  @Test
  void testRefreshDuckDBS3CredentialsWithSQLException() throws Exception {
    when(mockSwiftLakeEngine.createDuckDBConnection())
        .thenThrow(new RuntimeException("Test exception"));

    SwiftLakeS3FileIOProperties s3Properties = new SwiftLakeS3FileIOProperties(properties);
    AwsClientProperties awsProperties = new AwsClientProperties(properties);

    // Access the private method using reflection
    Method refreshMethod =
        AwsUtil.class.getDeclaredMethod(
            "refreshDuckDBS3Credentials",
            SwiftLakeEngine.class,
            SwiftLakeS3FileIOProperties.class,
            AwsClientProperties.class,
            AwsCredentialsProvider.class);
    refreshMethod.setAccessible(true);

    // Execute and verify exception
    assertThatThrownBy(
            () ->
                refreshMethod.invoke(
                    null,
                    mockSwiftLakeEngine,
                    s3Properties,
                    awsProperties,
                    mockCredentialsProvider))
        .isInstanceOf(InvocationTargetException.class)
        .getCause()
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Test exception");
  }

  @Test
  void testRefreshDuckDBS3CredentialsWithStatement() throws Exception {
    when(mockAwsSessionCredentials.accessKeyId()).thenReturn("session-access-key");
    when(mockAwsSessionCredentials.secretAccessKey()).thenReturn("session-secret-key");
    when(mockAwsSessionCredentials.sessionToken()).thenReturn("session-token");
    when(mockCredentialsProvider.resolveCredentials()).thenReturn(mockAwsSessionCredentials);
    SwiftLakeS3FileIOProperties s3Properties = mock(SwiftLakeS3FileIOProperties.class);
    AwsClientProperties awsProperties = mock(AwsClientProperties.class);

    // Configure mocks
    when(awsProperties.clientRegion()).thenReturn("us-west-2");
    when(s3Properties.endpoint()).thenReturn("https://s3.us-west-2.amazonaws.com");

    // Access the private method using reflection
    Method refreshMethod =
        AwsUtil.class.getDeclaredMethod(
            "refreshDuckDBS3Credentials",
            Statement.class,
            SwiftLakeS3FileIOProperties.class,
            AwsClientProperties.class,
            AwsCredentialsProvider.class);
    refreshMethod.setAccessible(true);

    // Invoke the private method
    refreshMethod.invoke(null, mockStatement, s3Properties, awsProperties, mockCredentialsProvider);

    // Verify SQL execution
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockStatement).execute(sqlCaptor.capture());

    String capturedSql = sqlCaptor.getValue();
    assertThat(capturedSql).startsWith("CREATE OR REPLACE SECRET (TYPE S3,");
    assertThat(capturedSql).contains("KEY_ID 'session-access-key'");
    assertThat(capturedSql).contains("SECRET 'session-secret-key'");
    assertThat(capturedSql).contains("SESSION_TOKEN 'session-token'");
    assertThat(capturedSql).contains("REGION 'us-west-2'");
    assertThat(capturedSql).contains("ENDPOINT 's3.us-west-2.amazonaws.com'");
  }

  class CredentialProvider implements AwsCredentialsProvider {
    static AwsCredentialsProvider credentialsProvider;

    static AwsCredentialsProvider create(Map<String, String> properties) {
      return credentialsProvider;
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return credentialsProvider.resolveCredentials();
    }
  }
}
