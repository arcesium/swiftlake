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

import com.arcesium.swiftlake.common.FileUtil;
import com.arcesium.swiftlake.common.InputFile;
import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.metrics.FileSystemCacheMetrics;
import com.arcesium.swiftlake.metrics.FileSystemCacheRuntimeMetrics;
import com.arcesium.swiftlake.metrics.MetricCollector;
import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.checkerframework.checker.index.qual.NonNegative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A cache implementation that stores files in the file system. */
public class FileSystemCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemCache.class);
  private final AsyncCache<String, CacheEntry> cache;
  private final Cache<String, CacheEntry> cacheView;
  private final String basePath;
  private final long maxTotalBytes;
  private final CacheStatsCounter statsCounter;

  /**
   * Constructs a new FileSystemCache.
   *
   * @param basePath The base path where cached files will be stored.
   * @param expireAfterAccessInSeconds The time in seconds after which a cache entry should expire
   *     if not accessed.
   * @param maxTotalBytes The maximum total size of the cache in bytes.
   * @param metricCollector The metric collector for cache statistics, or null if not required.
   * @throws ValidationException If any of the input parameters are invalid.
   */
  public FileSystemCache(
      String basePath,
      long expireAfterAccessInSeconds,
      long maxTotalBytes,
      MetricCollector metricCollector) {
    ValidationException.check(basePath != null && !basePath.isEmpty(), "basePath is null or empty");
    ValidationException.check(
        expireAfterAccessInSeconds > 0, "expireAfterAccessInSeconds is less than 0");
    ValidationException.check(maxTotalBytes > 0, "maxTotalBytes is equal or less than 0");
    this.basePath = FileUtil.stripTrailingSlash(basePath);
    new File(this.basePath).mkdirs();
    this.maxTotalBytes = maxTotalBytes;
    long expireAfterAccessNanos = expireAfterAccessInSeconds * 1_000_000_000L;
    Caffeine<String, CacheEntry> cacheBuilder =
        Caffeine.newBuilder()
            .maximumWeight(maxTotalBytes)
            .weigher(
                (String key, CacheEntry value) -> {
                  int weight;
                  if (value.refCount <= 0) {
                    weight = (int) Math.min(value.length, Integer.MAX_VALUE);
                  } else {
                    weight = 0;
                  }
                  LOGGER.debug("Weight of " + key + " is " + weight);
                  return weight;
                })
            .expireAfter(
                new Expiry<String, CacheEntry>() {
                  @Override
                  public long expireAfterCreate(String key, CacheEntry entry, long currentTime) {
                    long duration;
                    if (entry.refCount <= 0) {
                      duration = expireAfterAccessNanos;
                    } else {
                      duration = Long.MAX_VALUE;
                    }
                    LOGGER.debug("expireAfterCreate - Expiry of " + key + " is " + duration);
                    return duration;
                  }

                  @Override
                  public long expireAfterUpdate(
                      String key, CacheEntry entry, long currentTime, long currentDuration) {
                    long duration;
                    if (entry.refCount <= 0) {
                      duration = expireAfterAccessNanos;
                    } else {
                      duration = Long.MAX_VALUE;
                    }
                    LOGGER.debug("expireAfterUpdate - Expiry of " + key + " is " + duration);
                    return duration;
                  }

                  @Override
                  public long expireAfterRead(
                      String key, CacheEntry entry, long currentTime, long currentDuration) {
                    long duration;
                    if (entry.refCount <= 0) {
                      duration = expireAfterAccessNanos;
                    } else {
                      duration = Long.MAX_VALUE;
                    }
                    LOGGER.debug("expireAfterRead - Expiry of " + key + " is " + duration);
                    return duration;
                  }
                })
            .removalListener(
                (location, cacheEntry, cause) -> {
                  try {
                    if (cause.wasEvicted()) {
                      deleteFile(cacheEntry.localFileLocation);
                      LOGGER.debug("Evicted {} from FileSystemCache due to {}", location, cause);
                    }
                  } catch (Exception e) {
                    LOGGER.warn(
                        "An error occurred in the FileSystemCache entry removal listener", e);
                  }
                });

    if (metricCollector != null) {
      statsCounter = new CacheStatsCounter(metricCollector);
      cacheBuilder = cacheBuilder.recordStats(() -> statsCounter);
    } else {
      statsCounter = null;
    }

    this.cache = cacheBuilder.buildAsync();
    this.cacheView = this.cache.synchronous();
  }

  /**
   * Retrieves input files from the cache or downloads them if not present.
   *
   * @param fileIO The SwiftLakeFileIO instance for file operations
   * @param locations The collection of file locations to retrieve
   * @return An InputFiles instance containing the retrieved files
   */
  public InputFiles get(SwiftLakeFileIO fileIO, Collection<String> locations) {
    ConcurrentMap<String, CompletableFuture<CacheEntry>> cacheMap = cache.asMap();
    List<CompletableFuture<CacheEntry>> futures =
        locations.stream()
            .map(
                l -> {
                  return cacheMap.compute(
                      l,
                      (k, v) -> {
                        boolean miss = false;
                        if (v == null) {
                          LOGGER.debug("Starting download of " + k);
                          miss = true;
                          v = downloadFile(fileIO, l);
                        }
                        boolean recordMiss = miss;
                        return v.thenApply(
                            e -> {
                              e.refCount = e.refCount + 1;
                              LOGGER.debug(
                                  "After incrementing refCount of " + k + " is " + e.refCount);
                              if (statsCounter != null) {
                                try {
                                  if (recordMiss) {
                                    statsCounter.recordMisses(1);
                                  } else {
                                    statsCounter.recordHits(1);
                                  }
                                } catch (Throwable t) {
                                  LOGGER.error("An error occurred while recording stats", t);
                                }
                              }
                              return e;
                            });
                      });
                })
            .collect(Collectors.toList());

    List<InputFile> files =
        futures.stream()
            .map(
                f -> {
                  CacheEntry ce = f.join();
                  return new CachingInputFile(ce.localFileLocation, ce.location, ce.length);
                })
            .collect(Collectors.toList());

    return new CachingInputFiles(this, files);
  }

  /**
   * Returns the runtime metrics of the file system cache.
   *
   * @return A FileSystemCacheRuntimeMetrics instance with current cache statistics
   */
  public FileSystemCacheRuntimeMetrics getRuntimeMetrics() {
    long storageUsageInBytes =
        cacheView.policy().eviction().orElseThrow().weightedSize().orElseThrow();
    return new FileSystemCacheRuntimeMetrics(
        cacheView.estimatedSize(), maxTotalBytes, storageUsageInBytes);
  }

  /**
   * Decrements the reference count for the given locations in the cache.
   *
   * @param locations The list of file locations to decrement
   * @return A list of CompletableFuture<CacheEntry> for the decremented entries
   */
  private List<CompletableFuture<CacheEntry>> decrementReferenceCount(List<String> locations) {
    ConcurrentMap<String, CompletableFuture<CacheEntry>> cacheMap = this.cache.asMap();
    return locations.stream()
        .map(
            i -> {
              return cacheMap.compute(
                  i,
                  (k, v) -> {
                    return v.thenApply(
                        e -> {
                          e.refCount = e.refCount - 1;
                          LOGGER.debug("After decrementing refCount of " + k + " is " + e.refCount);
                          return e;
                        });
                  });
            })
        .collect(Collectors.toList());
  }

  /**
   * Downloads a file asynchronously and creates a new CacheEntry.
   *
   * @param fileIO The SwiftLakeFileIO instance for file operations
   * @param location The location of the file to download
   * @return A CompletableFuture<CacheEntry> for the downloaded file
   */
  private CompletableFuture<CacheEntry> downloadFile(SwiftLakeFileIO fileIO, String location) {
    try {
      Path destination = Path.of(basePath, getNewFileName());
      CompletableFuture<Void> downloadFuture = fileIO.downloadFileAsync(location, destination);

      return downloadFuture.handle(
          (unused, throwable) -> {
            if (throwable != null) {
              throw new SwiftLakeException(throwable, "Failed to download file: %s", location);
            }

            File file = destination.toFile();
            if (!file.exists()) {
              throw new ValidationException(
                  "Download completed but file does not exist: %s", destination);
            }

            return new CacheEntry(file.length(), location, destination.toString());
          });
    } catch (RuntimeException e) {
      return CompletableFuture.failedFuture(
          new SwiftLakeException(e, "Error setting up download for: %s", location));
    }
  }

  /**
   * Generates a new unique file name for caching.
   *
   * @return A UUID-based file name
   */
  private String getNewFileName() {
    return UUID.randomUUID().toString();
  }

  /**
   * Deletes a file from the local file system.
   *
   * @param location The path of the file to delete
   */
  private void deleteFile(String location) {
    try {
      Files.deleteIfExists(Paths.get(location));
    } catch (IOException e) {
      throw new SwiftLakeException(e, "An error occurred while deleting file %s", location);
    }
  }

  /** Represents a cache entry with file metadata and reference count. */
  private static class CacheEntry {
    private final long length;
    private final String location;
    private final String localFileLocation;
    private long refCount;

    private CacheEntry(long length, String location, String localFileLocation) {
      this.length = length;
      this.location = location;
      this.localFileLocation = localFileLocation;
    }
  }

  /** Implements InputFiles interface for cached files. */
  private static class CachingInputFiles implements InputFiles {
    private List<InputFile> inputFiles;
    private FileSystemCache cache;

    public CachingInputFiles(FileSystemCache cache, List<InputFile> inputFiles) {
      this.cache = cache;
      this.inputFiles = inputFiles;
    }

    @Override
    public void close() {
      if (this.inputFiles != null) {
        cache.decrementReferenceCount(
            this.inputFiles.stream().map(i -> i.getLocation()).collect(Collectors.toList()));
        this.inputFiles = null;
        this.cache = null;
      }
    }

    @Override
    public List<InputFile> getInputFiles() {
      return this.inputFiles;
    }
  }

  /** Implements InputFile interface for a cached file. */
  private static class CachingInputFile implements InputFile {
    private final long length;
    private final String localFileLocation;
    private final String location;

    public CachingInputFile(String localFileLocation, String location, long length) {
      this.length = length;
      this.localFileLocation = localFileLocation;
      this.location = location;
    }

    @Override
    public long getLength() {
      return length;
    }

    @Override
    public String getLocalFileLocation() {
      return localFileLocation;
    }

    @Override
    public String getLocation() {
      return location;
    }
  }

  /** Custom StatsCounter implementation for collecting cache metrics. */
  private static class CacheStatsCounter implements StatsCounter {
    private final MetricCollector metricCollector;

    CacheStatsCounter(MetricCollector metricCollector) {
      this.metricCollector = metricCollector;
    }

    @Override
    public void recordHits(@NonNegative int count) {
      this.metricCollector.collectMetrics(new FileSystemCacheMetrics(count, 0, 0, 0, null));
    }

    @Override
    public void recordMisses(@NonNegative int count) {
      this.metricCollector.collectMetrics(new FileSystemCacheMetrics(0, count, 0, 0, null));
    }

    @Override
    public void recordLoadSuccess(@NonNegative long loadTime) {}

    @Override
    public void recordLoadFailure(@NonNegative long loadTime) {}

    @Override
    public void recordEviction(@NonNegative int weight, RemovalCause cause) {
      this.metricCollector.collectMetrics(new FileSystemCacheMetrics(0, 0, 1, weight, cause));
    }

    @Override
    public CacheStats snapshot() {
      return null;
    }
  }
}
