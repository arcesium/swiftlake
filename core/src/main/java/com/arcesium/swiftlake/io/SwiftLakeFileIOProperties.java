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

import com.arcesium.swiftlake.metrics.MetricCollector;
import com.arcesium.swiftlake.metrics.MetricCollectorProvider;
import com.google.common.base.Strings;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.util.PropertyUtil;

/**
 * This class manages properties for SwiftLake file I/O operations. It includes configurations for
 * caching, metrics collection, and file system operations.
 */
public class SwiftLakeFileIOProperties {
  /** The key for specifying the delegate file I/O implementation. */
  public static final String DELEGATE_FILE_IO_IMPL = "delegate-io-impl";

  /** The key for specifying the cache file I/O provider. */
  public static final String CACHE_FILE_IO_PROVIDER = "cache-io-provider";

  /** The default value for the cache file I/O provider. */
  public static final String CACHE_FILE_IO_PROVIDER_DEFAULT =
      SingletonCacheFileIOProvider.class.getName();

  /** The key for specifying the metric collector provider. */
  public static final String METRIC_COLLECTOR_PROVIDER = "metric-collector-provider";

  /** The key for enabling/disabling the I/O manifest cache. */
  public static final String IO_MANIFEST_CACHE_ENABLED = "io.manifest-cache.enabled";

  /** The default value for enabling the I/O manifest cache. */
  public static final boolean IO_MANIFEST_CACHE_ENABLED_DEFAULT = true;

  /** The key for specifying the I/O manifest cache expiration interval in seconds. */
  public static final String IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_SECONDS =
      "io.manifest-cache.expiration-interval-seconds";

  /** The default value for the I/O manifest cache expiration interval (2 hours in seconds). */
  public static final long IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_SECONDS_DEFAULT =
      TimeUnit.HOURS.toSeconds(2);

  /** The key for specifying the maximum total bytes for the I/O manifest cache. */
  public static final String IO_MANIFEST_CACHE_MAX_TOTAL_BYTES =
      "io.manifest-cache.max-total-bytes";

  /** The default value for the maximum total bytes of the I/O manifest cache (256 MB). */
  public static final long IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT = 256L * 1024 * 1024;

  /** The key for specifying the maximum content length for the I/O manifest cache. */
  public static final String IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH =
      "io.manifest-cache.max-content-length";

  /** The default value for the maximum content length of the I/O manifest cache (16 MB). */
  public static final long IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT = 16L * 1024 * 1024;

  /** The key for enabling/disabling the I/O file system cache. */
  public static final String IO_FILE_SYSTEM_CACHE_ENABLED = "io.file-system-cache.enabled";

  /** The default value for enabling the I/O file system cache. */
  public static final boolean IO_FILE_SYSTEM_CACHE_ENABLED_DEFAULT = true;

  /** The key for specifying the base path of the I/O file system cache. */
  public static final String IO_FILE_SYSTEM_CACHE_BASE_PATH = "io.file-system-cache.base-path";

  /** The key for specifying the maximum total bytes for the I/O file system cache. */
  public static final String IO_FILE_SYSTEM_CACHE_MAX_TOTAL_BYTES =
      "io.file-system-cache.max-total-bytes";

  /** The default value for the maximum total bytes of the I/O file system cache (512 MB). */
  public static final long IO_FILE_SYSTEM_CACHE_MAX_TOTAL_BYTES_DEFAULT = 512L * 1024 * 1024;

  /** The key for specifying the I/O file system cache expiration interval in seconds. */
  public static final String IO_FILE_SYSTEM_CACHE_EXPIRATION_INTERVAL_SECONDS =
      "io.file-system-cache.expiration-interval-seconds";

  /** The default value for the I/O file system cache expiration interval (2 days in seconds). */
  public static final long IO_FILE_SYSTEM_CACHE_EXPIRATION_INTERVAL_SECONDS_DEFAULT =
      TimeUnit.DAYS.toSeconds(2);

  // Instance variables
  private String delegateFileIOImplClass;
  private String cacheFileIOProviderClass;
  private String metricCollectorProviderClass;
  private boolean manifestCacheEnabled;
  private long manifestCacheMaxTotalBytes;
  private long manifestCacheMaxContentLength;
  private long manifestCacheExpirationIntervalInSeconds;
  private boolean fileSystemCacheEnabled;
  private String fileSystemCacheBasePath;
  private long fileSystemCacheMaxTotalBytes;
  private long fileSystemCacheExpirationIntervalInSeconds;

  /** Default constructor that initializes properties with default values. */
  public SwiftLakeFileIOProperties() {
    super();
    this.manifestCacheEnabled = IO_MANIFEST_CACHE_ENABLED_DEFAULT;
    this.manifestCacheMaxTotalBytes = IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT;
    this.manifestCacheMaxContentLength = IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT;
    this.manifestCacheExpirationIntervalInSeconds =
        IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_SECONDS_DEFAULT;
    this.fileSystemCacheEnabled = IO_FILE_SYSTEM_CACHE_ENABLED_DEFAULT;
    this.fileSystemCacheBasePath =
        Path.of(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString()).toString();
    this.fileSystemCacheMaxTotalBytes = IO_FILE_SYSTEM_CACHE_MAX_TOTAL_BYTES_DEFAULT;
    this.fileSystemCacheExpirationIntervalInSeconds =
        IO_FILE_SYSTEM_CACHE_EXPIRATION_INTERVAL_SECONDS_DEFAULT;
  }

  /**
   * Constructor that initializes properties from a given Map.
   *
   * @param properties Map containing property key-value pairs
   */
  public SwiftLakeFileIOProperties(Map<String, String> properties) {
    this.delegateFileIOImplClass = properties.get(DELEGATE_FILE_IO_IMPL);
    this.cacheFileIOProviderClass =
        PropertyUtil.propertyAsString(
            properties, CACHE_FILE_IO_PROVIDER, CACHE_FILE_IO_PROVIDER_DEFAULT);
    this.metricCollectorProviderClass = properties.get(METRIC_COLLECTOR_PROVIDER);
    this.manifestCacheEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, IO_MANIFEST_CACHE_ENABLED, IO_MANIFEST_CACHE_ENABLED_DEFAULT);
    this.manifestCacheMaxTotalBytes =
        PropertyUtil.propertyAsLong(
            properties,
            IO_MANIFEST_CACHE_MAX_TOTAL_BYTES,
            IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT);
    this.manifestCacheMaxContentLength =
        PropertyUtil.propertyAsLong(
            properties,
            IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH,
            IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT);
    this.manifestCacheExpirationIntervalInSeconds =
        PropertyUtil.propertyAsLong(
            properties,
            IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_SECONDS,
            IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_SECONDS_DEFAULT);
    this.fileSystemCacheEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, IO_FILE_SYSTEM_CACHE_ENABLED, IO_FILE_SYSTEM_CACHE_ENABLED_DEFAULT);
    this.fileSystemCacheBasePath =
        PropertyUtil.propertyAsString(
            properties,
            IO_FILE_SYSTEM_CACHE_BASE_PATH,
            Path.of(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString()).toString());
    this.fileSystemCacheMaxTotalBytes =
        PropertyUtil.propertyAsLong(
            properties,
            IO_FILE_SYSTEM_CACHE_MAX_TOTAL_BYTES,
            IO_FILE_SYSTEM_CACHE_MAX_TOTAL_BYTES_DEFAULT);
    this.fileSystemCacheExpirationIntervalInSeconds =
        PropertyUtil.propertyAsLong(
            properties,
            IO_FILE_SYSTEM_CACHE_EXPIRATION_INTERVAL_SECONDS,
            IO_FILE_SYSTEM_CACHE_EXPIRATION_INTERVAL_SECONDS_DEFAULT);
  }

  /**
   * Gets the class name of the delegate FileIO implementation.
   *
   * @return The fully qualified class name of the delegate FileIO implementation.
   */
  public String getDelegateFileIOImplClass() {
    return delegateFileIOImplClass;
  }

  /**
   * Gets the class name of the cache FileIO provider.
   *
   * @return The fully qualified class name of the cache FileIO provider.
   */
  public String getCacheFileIOProviderClass() {
    return cacheFileIOProviderClass;
  }

  /**
   * Gets the class name of the metric collector provider.
   *
   * @return The fully qualified class name of the metric collector provider.
   */
  public String getMetricCollectorProviderClass() {
    return metricCollectorProviderClass;
  }

  /**
   * Checks if the manifest cache is enabled.
   *
   * @return true if manifest cache is enabled, false otherwise.
   */
  public boolean isManifestCacheEnabled() {
    return manifestCacheEnabled;
  }

  /**
   * Gets the maximum total bytes for the manifest cache.
   *
   * @return The maximum total bytes allowed for the manifest cache.
   */
  public long getManifestCacheMaxTotalBytes() {
    return manifestCacheMaxTotalBytes;
  }

  /**
   * Gets the maximum content length for the manifest cache.
   *
   * @return The maximum content length allowed for the manifest cache.
   */
  public long getManifestCacheMaxContentLength() {
    return manifestCacheMaxContentLength;
  }

  /**
   * Gets the expiration interval for the manifest cache in seconds.
   *
   * @return The expiration interval for the manifest cache in seconds.
   */
  public long getManifestCacheExpirationIntervalInSeconds() {
    return manifestCacheExpirationIntervalInSeconds;
  }

  /**
   * Checks if the file system cache is enabled.
   *
   * @return true if file system cache is enabled, false otherwise.
   */
  public boolean isFileSystemCacheEnabled() {
    return fileSystemCacheEnabled;
  }

  /**
   * Gets the base path for the file system cache.
   *
   * @return The base path for the file system cache.
   */
  public String getFileSystemCacheBasePath() {
    return fileSystemCacheBasePath;
  }

  /**
   * Gets the maximum total bytes for the file system cache.
   *
   * @return The maximum total bytes allowed for the file system cache.
   */
  public long getFileSystemCacheMaxTotalBytes() {
    return fileSystemCacheMaxTotalBytes;
  }

  /**
   * Gets the expiration interval for the file system cache in seconds.
   *
   * @return The expiration interval for the file system cache in seconds.
   */
  public long getFileSystemCacheExpirationIntervalInSeconds() {
    return fileSystemCacheExpirationIntervalInSeconds;
  }

  /**
   * Get a MetricCollector instance based on the configured provider.
   *
   * @param properties Map of properties to pass to the MetricCollectorProvider
   * @return MetricCollector instance or null if no provider is configured
   */
  public MetricCollector getMetricCollector(Map<String, String> properties) {
    if (Strings.isNullOrEmpty(metricCollectorProviderClass)) {
      return null;
    }
    MetricCollectorProvider provider =
        newInstance(metricCollectorProviderClass, MetricCollectorProvider.class);
    return provider.getMetricCollector(properties);
  }

  /**
   * Get a CacheFileIO instance based on the configured provider.
   *
   * @param properties Map of properties to pass to the CacheFileIOProvider
   * @return CacheFileIO instance or null if no provider is configured
   */
  public CacheFileIO getCacheFileIO(Map<String, String> properties) {
    if (Strings.isNullOrEmpty(cacheFileIOProviderClass)) {
      return null;
    }
    CacheFileIOProvider provider = newInstance(cacheFileIOProviderClass, CacheFileIOProvider.class);
    return provider.getCacheFileIO(properties);
  }

  private <T> T newInstance(String impl, Class<T> baseClass) {
    DynConstructors.Ctor<T> ctor;
    try {
      ctor =
          DynConstructors.builder(baseClass)
              .loader(SwiftLakeFileIOProperties.class.getClassLoader())
              .impl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize %s implementation %s: %s",
              baseClass.getName(), impl, e.getMessage()),
          e);
    }

    try {
      return ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize %s, %s does not implement %s.", impl, impl, baseClass.getName()),
          e);
    }
  }
}
