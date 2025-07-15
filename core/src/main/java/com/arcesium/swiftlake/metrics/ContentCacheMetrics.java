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
package com.arcesium.swiftlake.metrics;

import com.github.benmanes.caffeine.cache.RemovalCause;

/** Represents metrics for a content cache. */
public class ContentCacheMetrics implements Metrics {
  private final long hitCount;
  private final long missCount;
  private final long evictionCount;
  private final long evictionWeight;
  private final RemovalCause evictionCause;

  /**
   * Constructs a ContentCacheMetrics object with the specified metrics.
   *
   * @param hitCount The number of cache hits.
   * @param missCount The number of cache misses.
   * @param evictionCount The number of cache evictions.
   * @param evictionWeight The weight of evicted entries.
   * @param evictionCause The cause of evictions.
   */
  public ContentCacheMetrics(
      long hitCount,
      long missCount,
      long evictionCount,
      long evictionWeight,
      RemovalCause evictionCause) {
    this.hitCount = hitCount;
    this.missCount = missCount;
    this.evictionCount = evictionCount;
    this.evictionWeight = evictionWeight;
    this.evictionCause = evictionCause;
  }

  /**
   * Gets the number of cache hits.
   *
   * @return The number of cache hits.
   */
  public long getHitCount() {
    return hitCount;
  }

  /**
   * Gets the number of cache misses.
   *
   * @return The number of cache misses.
   */
  public long getMissCount() {
    return missCount;
  }

  /**
   * Gets the number of cache evictions.
   *
   * @return The number of cache evictions.
   */
  public long getEvictionCount() {
    return evictionCount;
  }

  /**
   * Gets the weight of evicted entries.
   *
   * @return The weight of evicted entries.
   */
  public long getEvictionWeight() {
    return evictionWeight;
  }

  /**
   * Gets the cause of evictions.
   *
   * @return The cause of evictions.
   */
  public RemovalCause getEvictionCause() {
    return evictionCause;
  }

  /**
   * Returns a string representation of the ContentCacheMetrics object.
   *
   * @return A string representation of the object.
   */
  @Override
  public String toString() {
    return "ContentCacheMetrics{"
        + "hitCount="
        + hitCount
        + ", missCount="
        + missCount
        + ", evictionCount="
        + evictionCount
        + ", evictionWeight="
        + evictionWeight
        + ", evictionCause="
        + evictionCause
        + '}';
  }
}
