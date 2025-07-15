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

/** Provides a way to obtain a {@link CacheFileIO} instance based on specified properties. */
public interface CacheFileIOProvider {

  /**
   * Returns a {@link CacheFileIO} instance configured with the given properties.
   *
   * @param properties A map of configuration properties for the CacheFileIO
   * @return A {@link CacheFileIO} instance
   */
  CacheFileIO getCacheFileIO(Map<String, String> properties);
}
