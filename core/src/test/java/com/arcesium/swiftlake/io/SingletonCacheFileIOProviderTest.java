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
package com.arcesium.swiftlake.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SingletonCacheFileIOProviderTest {

  private SingletonCacheFileIOProvider provider;
  private Map<String, String> properties;

  @BeforeEach
  void setUp() {
    provider = new SingletonCacheFileIOProvider();
    properties = new HashMap<>();
    properties.put("key1", "value1");
    properties.put("key2", "value2");
  }

  @Test
  void testGetCacheFileIO_ReturnsSameInstance() {
    CacheFileIO instance1 = provider.getCacheFileIO(properties);
    CacheFileIO instance2 = provider.getCacheFileIO(properties);

    assertThat(instance1).isNotNull();
    assertThat(instance2).isNotNull();
    assertThat(instance1).isSameAs(instance2);
  }

  @Test
  void testGetCacheFileIO_ThreadSafety() throws InterruptedException {
    int threadCount = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    CacheFileIO[] instances = new CacheFileIO[threadCount];

    for (int i = 0; i < threadCount; i++) {
      final int index = i;
      executorService.submit(
          () -> {
            try {
              instances[index] = provider.getCacheFileIO(properties);
            } finally {
              latch.countDown();
            }
          });
    }

    latch.await();
    executorService.shutdown();

    CacheFileIO firstInstance = instances[0];
    assertThat(firstInstance).isNotNull();

    for (int i = 1; i < threadCount; i++) {
      assertThat(instances[i]).isSameAs(firstInstance);
    }
  }

  @Test
  void testGetCacheFileIO_WithDifferentProperties() {
    CacheFileIO instance1 = provider.getCacheFileIO(properties);

    Map<String, String> differentProperties = new HashMap<>();
    differentProperties.put("key3", "value3");
    CacheFileIO instance2 = provider.getCacheFileIO(differentProperties);

    assertThat(instance1).isSameAs(instance2);
  }

  @Test
  void testGetCacheFileIO_WithNullProperties() {
    assertThatCode(() -> provider.getCacheFileIO(null)).doesNotThrowAnyException();

    CacheFileIO instance = provider.getCacheFileIO(null);
    assertThat(instance).isNotNull();
  }
}
