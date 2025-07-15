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
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.common.DynConstructors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

@ExtendWith(MockitoExtension.class)
class SwiftLakeS3FileIOPropertiesTest {

  @Mock private AwsClientProperties mockAwsClientProperties;

  @Mock private AwsCredentialsProvider mockCredentialsProvider;

  @Mock private S3CrtAsyncClientBuilder mockBuilder;

  @Test
  void testDefaultConstructor() {
    SwiftLakeS3FileIOProperties properties = new SwiftLakeS3FileIOProperties();

    assertThat(properties.getCrtTargetThroughputInGbps())
        .isEqualTo(SwiftLakeS3FileIOProperties.CRT_TARGET_THROUGHPUT_GBPS_DEFAULT);
    assertThat(properties.isDuckDBS3ExtensionEnabled())
        .isEqualTo(SwiftLakeS3FileIOProperties.DUCKDB_S3_EXTENSION_ENABLED_DEFAULT);
  }

  @Test
  void testConstructorWithProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(SwiftLakeS3FileIOProperties.TRANSFER_MANAGER_PROVIDER, "custom.provider.Class");
    props.put(SwiftLakeS3FileIOProperties.CRT_TARGET_THROUGHPUT_GBPS, "15.5");
    props.put(SwiftLakeS3FileIOProperties.CRT_MAX_CONCURRENCY, "50");
    props.put(SwiftLakeS3FileIOProperties.CRT_MAX_NATIVE_MEMORY_LIMIT_BYTES, "1073741824"); // 1GB
    props.put(SwiftLakeS3FileIOProperties.CRT_MULTIPART_THRESHOLD_BYTES, "10485760"); // 10MB
    props.put(SwiftLakeS3FileIOProperties.DUCKDB_S3_EXTENSION_ENABLED, "true");
    props.put(SwiftLakeS3FileIOProperties.DUCKDB_S3_EXTENSION_THRESHOLD_BYTES, "5242880"); // 5MB
    props.put(
        SwiftLakeS3FileIOProperties.TRANSFER_MANAGER_PROVIDER_PREFIX + "custom.key",
        "custom.value");

    SwiftLakeS3FileIOProperties properties = new SwiftLakeS3FileIOProperties(props);

    assertThat(properties.getS3TransferManagerProviderClass()).isEqualTo("custom.provider.Class");
    assertThat(properties.getCrtTargetThroughputInGbps()).isEqualTo(15.5);
    assertThat(properties.getCrtMaxConcurrency()).isEqualTo(50);
    assertThat(properties.getCrtMaxNativeMemoryLimitInBytes()).isEqualTo(1073741824L);
    assertThat(properties.getCrtMultipartThresholdInBytes()).isEqualTo(10485760L);
    assertThat(properties.isDuckDBS3ExtensionEnabled()).isTrue();
    assertThat(properties.getDuckDBS3ExtensionThresholdInBytes()).isEqualTo(5242880L);
    assertThat(properties.getS3TransferManagerProviderProperties())
        .containsEntry("custom.key", "custom.value");
  }

  @Test
  void testGetAwsCredentialsProvider_WithRemoteSigningEnabled() {
    SwiftLakeS3FileIOProperties properties = spy(new SwiftLakeS3FileIOProperties());
    when(properties.isRemoteSigningEnabled()).thenReturn(true);

    AwsCredentialsProvider provider = properties.getAwsCredentialsProvider(mockAwsClientProperties);

    assertThat(provider).isInstanceOf(AnonymousCredentialsProvider.class);
  }

  @Test
  void testGetAwsCredentialsProvider_WithRemoteSigningDisabled() {
    SwiftLakeS3FileIOProperties properties = spy(new SwiftLakeS3FileIOProperties());
    when(properties.isRemoteSigningEnabled()).thenReturn(false);
    when(properties.accessKeyId()).thenReturn("testKey");
    when(properties.secretAccessKey()).thenReturn("testSecret");
    when(properties.sessionToken()).thenReturn("testToken");
    when(mockAwsClientProperties.credentialsProvider(anyString(), anyString(), anyString()))
        .thenReturn(mockCredentialsProvider);

    AwsCredentialsProvider provider = properties.getAwsCredentialsProvider(mockAwsClientProperties);

    assertThat(provider).isEqualTo(mockCredentialsProvider);
    verify(mockAwsClientProperties).credentialsProvider("testKey", "testSecret", "testToken");
  }

  @Test
  void testApplyCredentialConfigurations() {
    SwiftLakeS3FileIOProperties properties = new SwiftLakeS3FileIOProperties();
    when(mockAwsClientProperties.credentialsProvider(any(), any(), any()))
        .thenReturn(mockCredentialsProvider);
    properties.applyCredentialConfigurations(mockAwsClientProperties, mockBuilder);

    verify(mockBuilder).credentialsProvider(mockCredentialsProvider);
  }

  @Test
  void testGetS3TransferManager() {
    Map<String, String> props = new HashMap<>();
    props.put(
        SwiftLakeS3FileIOProperties.TRANSFER_MANAGER_PROVIDER,
        SwiftLakeS3FileIOProperties.TRANSFER_MANAGER_PROVIDER_DEFAULT);

    SwiftLakeS3FileIOProperties properties = new SwiftLakeS3FileIOProperties(props);

    S3TransferManagerProvider mockProvider = mock(S3TransferManagerProvider.class);
    S3TransferManager mockTransferManager = mock(S3TransferManager.class);
    when(mockProvider.getS3TransferManager(props)).thenReturn(mockTransferManager);

    try (MockedStatic<DynConstructors> mockedDynConstructors = mockStatic(DynConstructors.class)) {
      DynConstructors.Builder mockBuilder = mock(DynConstructors.Builder.class);
      DynConstructors.Ctor<S3TransferManagerProvider> mockCtor = mock(DynConstructors.Ctor.class);

      mockedDynConstructors
          .when(() -> DynConstructors.builder(S3TransferManagerProvider.class))
          .thenReturn(mockBuilder);
      when(mockBuilder.loader(any())).thenReturn(mockBuilder);
      when(mockBuilder.impl(anyString())).thenReturn(mockBuilder);
      when(mockBuilder.buildChecked()).thenReturn((DynConstructors.Ctor) mockCtor);
      when(mockCtor.newInstance()).thenReturn(mockProvider);

      // Test the method
      S3TransferManager result = properties.getS3TransferManager(props);

      // Verify the result
      assertThat(result).isEqualTo(mockTransferManager);
      verify(mockProvider).getS3TransferManager(props);
    } catch (NoSuchMethodException e) {
      fail("Test setup failed", e);
    }
  }

  @Test
  void testCreateS3TransferManagerProviderInstance_ClassNotFound() {
    SwiftLakeS3FileIOProperties properties = new SwiftLakeS3FileIOProperties();

    // Use reflection to access private method
    try {
      java.lang.reflect.Method method =
          SwiftLakeS3FileIOProperties.class.getDeclaredMethod(
              "createS3TransferManagerProviderInstance", String.class);
      method.setAccessible(true);

      // Test with non-existent class
      assertThatThrownBy(() -> method.invoke(properties, "NonExistentClass"))
          .hasCauseInstanceOf(IllegalArgumentException.class);

    } catch (Exception e) {
      fail("Test setup failed", e);
    }
  }
}
