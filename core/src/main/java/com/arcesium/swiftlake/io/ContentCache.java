/*
 * Copyright (c) 2025, Arcesium LLC. All rights reserved.
 * Copyright 2017-2025 The Apache Software Foundation.
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

import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.metrics.ContentCacheMetrics;
import com.arcesium.swiftlake.metrics.ContentCacheRuntimeMetrics;
import com.arcesium.swiftlake.metrics.MetricCollector;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import com.google.common.collect.Lists;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.io.ByteBufferInputStream;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.checkerframework.checker.index.qual.NonNegative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ContentCache is a caching mechanism for storing and retrieving file content. It uses Caffeine as
 * the underlying caching library and provides various configuration options. This class is
 * substantially derived from the Apache Iceberg project's implementation.
 *
 * @see <a
 *     href="https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/io/ContentCache.java">
 *     ContentCache</a>
 */
public class ContentCache {
  private static final Logger LOG = LoggerFactory.getLogger(ContentCache.class);
  private static final int BUFFER_CHUNK_SIZE = 4 * 1024 * 1024; // 4MB
  private final long expireAfterAccessInSeconds;
  private final long maxTotalBytes;
  private final long maxContentLength;
  private final Cache<String, CacheEntry> cache;

  /**
   * Constructs a new ContentCache with specified parameters.
   *
   * @param expireAfterAccessInSeconds The duration in seconds after which a cache entry should
   *     expire if not accessed. Must be non-negative.
   * @param maxTotalBytes The maximum total size of the cache in bytes. Must be positive.
   * @param maxContentLength The maximum length of a single content entry. Must be positive.
   * @param metricCollector The MetricCollector for recording cache statistics. Can be null.
   * @throws ValidationException If any of the input parameters are invalid.
   */
  public ContentCache(
      long expireAfterAccessInSeconds,
      long maxTotalBytes,
      long maxContentLength,
      MetricCollector metricCollector) {
    ValidationException.check(
        expireAfterAccessInSeconds >= 0, "expireAfterAccessInSeconds is less than 0");
    ValidationException.check(maxTotalBytes > 0, "maxTotalBytes is equal or less than 0");
    ValidationException.check(maxContentLength > 0, "maxContentLength is equal or less than 0");
    this.expireAfterAccessInSeconds = expireAfterAccessInSeconds;
    this.maxTotalBytes = maxTotalBytes;
    this.maxContentLength = maxContentLength;

    Caffeine<Object, Object> builder = Caffeine.newBuilder();
    if (expireAfterAccessInSeconds > 0) {
      builder = builder.expireAfterAccess(Duration.ofSeconds(expireAfterAccessInSeconds));
    }

    Caffeine<String, CacheEntry> cacheBuilder =
        builder
            .maximumWeight(maxTotalBytes)
            .weigher(
                (Weigher<String, CacheEntry>)
                    (key, value) -> (int) Math.min(value.length, Integer.MAX_VALUE))
            .softValues()
            .removalListener(
                (location, cacheEntry, cause) ->
                    LOG.debug("Evicted {} from ContentCache ({})", location, cause));

    if (metricCollector != null) {
      cacheBuilder = cacheBuilder.recordStats(() -> new CacheStatsCounter(metricCollector));
    }

    this.cache = cacheBuilder.build();
  }

  /**
   * Returns the duration in seconds after which a cache entry expires if not accessed.
   *
   * @return The expiration time in seconds.
   */
  public long expireAfterAccessInSeconds() {
    return expireAfterAccessInSeconds;
  }

  /**
   * Returns the maximum content length that can be cached.
   *
   * @return The maximum content length in bytes
   */
  public long maxContentLength() {
    return maxContentLength;
  }

  /**
   * Returns the maximum total bytes the cache can hold.
   *
   * @return The maximum total bytes capacity of the cache
   */
  public long maxTotalBytes() {
    return maxTotalBytes;
  }

  /**
   * Returns the current cache statistics.
   *
   * @return The {@link CacheStats} object containing current cache statistics
   */
  public CacheStats stats() {
    return cache.stats();
  }

  /**
   * Gets a cache entry, computing it if absent.
   *
   * @param key The cache key
   * @param mappingFunction The function to compute the value if absent
   * @return The cache entry
   */
  public CacheEntry get(String key, Function<String, CacheEntry> mappingFunction) {
    return cache.get(key, mappingFunction);
  }

  /**
   * Gets a cache entry if present.
   *
   * @param location The cache key
   * @return The cache entry if present, null otherwise
   */
  public CacheEntry getIfPresent(String location) {
    return cache.getIfPresent(location);
  }

  /**
   * Gets an InputFile, potentially using the cache.
   *
   * @param fileIO The FileIO to use
   * @param location The file location
   * @param length The file length
   * @return An InputFile, either cached or not based on the length
   */
  public InputFile get(FileIO fileIO, String location, long length) {
    if (length <= maxContentLength) {
      return new CachingInputFile(this, fileIO, location, length);
    }
    return fileIO.newInputFile(location, length);
  }

  /**
   * Invalidates a cache entry.
   *
   * @param key The key to invalidate
   */
  public void invalidate(String key) {
    cache.invalidate(key);
  }

  /** Invalidates all cache entries. */
  public void invalidateAll() {
    cache.invalidateAll();
  }

  /** Performs any pending maintenance operations needed by the cache. */
  public void cleanUp() {
    cache.cleanUp();
  }

  /**
   * Estimates the number of entries in the cache.
   *
   * @return The estimated number of entries in the cache
   */
  public long estimatedCacheSize() {
    return cache.estimatedSize();
  }

  /**
   * Retrieves the current runtime metrics of the cache.
   *
   * @return The current runtime metrics of the cache as a {@link ContentCacheRuntimeMetrics} object
   */
  public ContentCacheRuntimeMetrics getRuntimeMetrics() {
    long currentUsageInBytes = cache.policy().eviction().orElseThrow().weightedSize().orElseThrow();
    return new ContentCacheRuntimeMetrics(
        cache.estimatedSize(), maxTotalBytes, currentUsageInBytes);
  }

  /** Represents an entry in the content cache. */
  private static class CacheEntry {
    private final long length;
    private final List<ByteBuffer> buffers;

    /**
     * Constructs a new CacheEntry.
     *
     * @param length The length of the content
     * @param buffers The content to be cached *
     */
    private CacheEntry(long length, List<ByteBuffer> buffers) {
      this.length = length;
      this.buffers = buffers;
    }
  }

  /** An InputFile implementation that uses the content cache. */
  private static class CachingInputFile implements InputFile {
    private final ContentCache contentCache;
    private final FileIO io;
    private final String location;
    private final long length;
    private InputFile fallbackInputFile = null;

    /**
     * Constructs a new CachingInputFile.
     *
     * @param cache The ContentCache to use
     * @param io The FileIO to use for reading if not cached
     * @param location The file location
     * @param length The file length
     */
    private CachingInputFile(ContentCache cache, FileIO io, String location, long length) {
      this.contentCache = cache;
      this.io = io;
      this.location = location;
      this.length = length;
    }

    private InputFile wrappedInputFile() {
      if (fallbackInputFile == null) {
        fallbackInputFile = ((SwiftLakeFileIO) io).newInputFile(location, length, false);
      }
      return fallbackInputFile;
    }

    /**
     * Returns the length of the file.
     *
     * @return The length of the file in bytes.
     */
    @Override
    public long getLength() {
      CacheEntry buf = contentCache.getIfPresent(location);
      if (buf != null) {
        return buf.length;
      } else if (fallbackInputFile != null) {
        return fallbackInputFile.getLength();
      } else {
        return length;
      }
    }

    /**
     * Creates a new InputStream for reading the file content.
     *
     * @return An InputStream for the file content
     */
    @Override
    public SeekableInputStream newStream() {
      try {
        // read from cache if file length is less than or equal to maximum length allowed to
        // cache.
        if (getLength() <= contentCache.maxContentLength()) {
          return cachedStream();
        }

        // fallback to non-caching input stream.
        return wrappedInputFile().newStream();
      } catch (FileNotFoundException e) {
        throw new SwiftLakeException(
            e, "Failed to open input stream for file %s: %s", location, e.toString());
      } catch (IOException e) {
        throw new UncheckedIOException(
            String.format("Failed to open input stream for file %s: %s", location, e), e);
      }
    }

    /**
     * Returns the location of this cache entry.
     *
     * @return The location string.
     */
    @Override
    public String location() {
      return location;
    }

    /**
     * Checks if the content exists in the cache or in the wrapped input file.
     *
     * @return true if the content exists either in the cache or in the wrapped input file, false
     *     otherwise.
     */
    @Override
    public boolean exists() {
      CacheEntry buf = contentCache.getIfPresent(location);
      return buf != null || wrappedInputFile().exists();
    }

    private CacheEntry cacheEntry() {
      long start = System.currentTimeMillis();
      try (SeekableInputStream stream = wrappedInputFile().newStream()) {
        long fileLength = getLength();
        long totalBytesToRead = fileLength;
        List<ByteBuffer> buffers = Lists.newArrayList();

        while (totalBytesToRead > 0) {
          // read the stream in 4MB chunk
          int bytesToRead = (int) Math.min(BUFFER_CHUNK_SIZE, totalBytesToRead);
          byte[] buf = new byte[bytesToRead];
          int bytesRead = IOUtil.readRemaining(stream, buf, 0, bytesToRead);
          totalBytesToRead -= bytesRead;

          if (bytesRead < bytesToRead) {
            // Read less than it should be, possibly hitting EOF. Abandon caching by throwing
            // IOException and let the caller fallback to non-caching input file.
            throw new IOException(
                String.format(
                    "Expected to read %d bytes, but only %d bytes read.",
                    fileLength, fileLength - totalBytesToRead));
          } else {
            buffers.add(ByteBuffer.wrap(buf));
          }
        }

        CacheEntry newEntry = new CacheEntry(fileLength, buffers);
        LOG.debug("cacheEntry took {} ms for {}", (System.currentTimeMillis() - start), location);
        return newEntry;
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    }

    private SeekableInputStream cachedStream() throws IOException {
      try {
        CacheEntry entry = contentCache.get(location, k -> cacheEntry());
        ValidationException.checkNotNull(
            entry, "CacheEntry should not be null when there is no RuntimeException occurs");
        LOG.debug("Cache stats: {}", contentCache.stats());
        return ByteBufferInputStream.wrap(entry.buffers);
      } catch (UncheckedIOException ex) {
        throw ex.getCause();
      } catch (RuntimeException ex) {
        throw new IOException("Caught an error while reading from cache", ex);
      }
    }
  }

  /** A custom CacheStats implementation that reports metrics to a MetricCollector. */
  private static class CacheStatsCounter implements StatsCounter {
    private final MetricCollector metricCollector;

    /**
     * Constructs a new CacheStatsCounter.
     *
     * @param metricCollector The MetricCollector to use for reporting metrics
     */
    CacheStatsCounter(MetricCollector metricCollector) {
      this.metricCollector = metricCollector;
    }

    /**
     * Records a cache hit.
     *
     * @param count The number of hits to record
     */
    @Override
    public void recordHits(@NonNegative int count) {
      this.metricCollector.collectMetrics(new ContentCacheMetrics(count, 0, 0, 0, null));
    }

    /**
     * Records a cache miss.
     *
     * @param count The number of misses to record
     */
    @Override
    public void recordMisses(@NonNegative int count) {
      this.metricCollector.collectMetrics(new ContentCacheMetrics(0, count, 0, 0, null));
    }

    /**
     * Records the cache load success.
     *
     * @param loadTime The time taken to load the cache entry
     */
    @Override
    public void recordLoadSuccess(@NonNegative long loadTime) {}

    /**
     * Records the cache load failure.
     *
     * @param loadTime The time taken during the failed load attempt
     */
    @Override
    public void recordLoadFailure(@NonNegative long loadTime) {}

    /**
     * Records the eviction of an entry from the cache.
     *
     * @param weight The weight of the evicted entry.
     * @param cause The reason for the entry's removal.
     */
    @Override
    public void recordEviction(@NonNegative int weight, RemovalCause cause) {
      this.metricCollector.collectMetrics(new ContentCacheMetrics(0, 0, 1, weight, cause));
    }

    @Override
    public CacheStats snapshot() {
      return null;
    }
  }
}
