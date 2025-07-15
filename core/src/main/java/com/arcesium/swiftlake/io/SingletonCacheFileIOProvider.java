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

import java.util.Map;

/**
 * A singleton implementation of the CacheFileIOProvider interface. This class ensures that only one
 * instance of CacheFileIO is created and used.
 */
public class SingletonCacheFileIOProvider implements CacheFileIOProvider {
  /** The single instance of CacheFileIO, using volatile for thread-safety. */
  private static volatile CacheFileIO cacheFileIO;

  /**
   * Retrieves the singleton instance of CacheFileIO.
   *
   * @param properties A map of properties used to configure the CacheFileIO instance.
   * @return The singleton instance of CacheFileIO.
   */
  @Override
  public CacheFileIO getCacheFileIO(Map<String, String> properties) {
    if (cacheFileIO == null) {
      synchronized (SingletonCacheFileIOProvider.class) {
        if (cacheFileIO == null) {
          cacheFileIO = new DefaultCacheFileIO();
          cacheFileIO.initialize(properties);
        }
      }
    }
    return cacheFileIO;
  }
}
