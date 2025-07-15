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
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

@ExtendWith(MockitoExtension.class)
class SingletonS3TransferManagerProviderTest {

  private SingletonS3TransferManagerProvider provider;
  private Map<String, String> properties;

  @BeforeEach
  void setUp() {
    provider = new SingletonS3TransferManagerProvider();
    properties = new HashMap<>();
    properties.put("test-key", "test-value");

    resetS3TransferManager();
  }

  @AfterEach
  void tearDown() {
    resetS3TransferManager();
  }

  /** Reset the static s3TransferManager field using reflection */
  private void resetS3TransferManager() {
    try {
      Field field = SingletonS3TransferManagerProvider.class.getDeclaredField("s3TransferManager");
      field.setAccessible(true);
      field.set(null, null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to reset s3TransferManager", e);
    }
  }

  @Test
  void testGetS3TransferManager_CreatesNewInstanceWhenNull() {
    try (MockedStatic<AwsUtil> mockedAwsUtil = mockStatic(AwsUtil.class);
        MockedStatic<S3TransferManager> mockedS3TransferManager =
            mockStatic(S3TransferManager.class)) {

      S3AsyncClient mockS3Client = mock(S3AsyncClient.class);
      S3TransferManager mockTransferManager = mock(S3TransferManager.class);
      S3TransferManager.Builder mockBuilder = mock(S3TransferManager.Builder.class);

      mockedAwsUtil.when(() -> AwsUtil.createS3CrtAsyncClient(properties)).thenReturn(mockS3Client);
      mockedS3TransferManager.when(S3TransferManager::builder).thenReturn(mockBuilder);
      when(mockBuilder.s3Client(mockS3Client)).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockTransferManager);

      S3TransferManager result = provider.getS3TransferManager(properties);

      // Verify
      assertThat(result).isEqualTo(mockTransferManager);
      mockedAwsUtil.verify(() -> AwsUtil.createS3CrtAsyncClient(properties), times(1));
      verify(mockBuilder).s3Client(mockS3Client);
      verify(mockBuilder).build();
    }
  }

  @Test
  void testGetS3TransferManager_ReturnsSameInstanceOnSubsequentCalls() {
    try (MockedStatic<AwsUtil> mockedAwsUtil = mockStatic(AwsUtil.class);
        MockedStatic<S3TransferManager> mockedS3TransferManager =
            mockStatic(S3TransferManager.class)) {

      S3AsyncClient mockS3Client = mock(S3AsyncClient.class);
      S3TransferManager mockTransferManager = mock(S3TransferManager.class);
      S3TransferManager.Builder mockBuilder = mock(S3TransferManager.Builder.class);

      mockedAwsUtil.when(() -> AwsUtil.createS3CrtAsyncClient(properties)).thenReturn(mockS3Client);
      mockedS3TransferManager.when(S3TransferManager::builder).thenReturn(mockBuilder);
      when(mockBuilder.s3Client(mockS3Client)).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockTransferManager);

      // Call method twice
      S3TransferManager result1 = provider.getS3TransferManager(properties);
      S3TransferManager result2 = provider.getS3TransferManager(properties);

      // Verify
      assertThat(result1).isEqualTo(mockTransferManager);
      assertThat(result2).isEqualTo(mockTransferManager);
      assertThat(result1).isSameAs(result2);

      // Verify createS3CrtAsyncClient was called only once
      mockedAwsUtil.verify(() -> AwsUtil.createS3CrtAsyncClient(properties), times(1));
      verify(mockBuilder, times(1)).s3Client(mockS3Client);
      verify(mockBuilder, times(1)).build();
    }
  }

  @Test
  void testGetS3TransferManager_DifferentProviderInstancesShareSameTransferManager() {
    try (MockedStatic<AwsUtil> mockedAwsUtil = mockStatic(AwsUtil.class);
        MockedStatic<S3TransferManager> mockedS3TransferManager =
            mockStatic(S3TransferManager.class)) {

      S3AsyncClient mockS3Client = mock(S3AsyncClient.class);
      S3TransferManager mockTransferManager = mock(S3TransferManager.class);
      S3TransferManager.Builder mockBuilder = mock(S3TransferManager.Builder.class);

      mockedAwsUtil.when(() -> AwsUtil.createS3CrtAsyncClient(properties)).thenReturn(mockS3Client);
      mockedS3TransferManager.when(S3TransferManager::builder).thenReturn(mockBuilder);
      when(mockBuilder.s3Client(mockS3Client)).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockTransferManager);

      // Create two different provider instances
      SingletonS3TransferManagerProvider provider1 = new SingletonS3TransferManagerProvider();
      SingletonS3TransferManagerProvider provider2 = new SingletonS3TransferManagerProvider();

      // Get S3TransferManager from both providers
      S3TransferManager result1 = provider1.getS3TransferManager(properties);
      S3TransferManager result2 = provider2.getS3TransferManager(properties);

      // Verify
      assertThat(result1).isSameAs(result2);

      // Verify createS3CrtAsyncClient was called only once
      mockedAwsUtil.verify(() -> AwsUtil.createS3CrtAsyncClient(properties), times(1));
    }
  }
}
