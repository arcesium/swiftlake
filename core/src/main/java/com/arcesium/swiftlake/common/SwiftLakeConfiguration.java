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
package com.arcesium.swiftlake.common;

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.metrics.MetricCollector;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.catalog.Catalog;

/**
 * Configuration class for SwiftLake. This class holds various settings and parameters used to
 * configure the SwiftLake engine.
 */
public class SwiftLakeConfiguration {
  private String applicationId;
  private Catalog catalog;
  private Integer threads;
  private Integer memoryLimitInMiB;
  private Double memoryLimitFraction;
  private Integer tempStorageLimitInMiB;
  private String localDir;
  private Set<String> cachingCatalogNamespaces;
  private Set<String> cachingCatalogTableNames;
  private int cachingCatalogExpirationIntervalInSeconds;
  private String mybatisConfigPath;
  private Integer maxPartitionWriterThreads;
  private boolean allowCommunityExtensions;
  private boolean autoInstallKnownExtensions;
  private boolean lockDuckDBConfiguration;
  private Integer totalFileSizePerScanLimitInMiB;
  private Integer queryTimeoutInSeconds;
  private Integer maxActiveConnections;
  private Integer connectionCreationTimeoutInSeconds;
  private String debugFileUploadPath;
  private MetricCollector metricCollector;
  private boolean processTablesDefaultValue;
  private boolean allowFullTableScan;
  private Function<SwiftLakeEngine, List<AutoCloseable>> configureFunc;

  /**
   * Gets the application ID.
   *
   * @return The application ID.
   */
  public String getApplicationId() {
    return applicationId;
  }

  /**
   * Sets the application ID.
   *
   * @param applicationId The application ID to set.
   */
  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }

  /**
   * Gets the catalog.
   *
   * @return The catalog.
   */
  public Catalog getCatalog() {
    return catalog;
  }

  /**
   * Sets the catalog.
   *
   * @param catalog The catalog to set.
   */
  public void setCatalog(Catalog catalog) {
    this.catalog = catalog;
  }

  /**
   * Gets the number of threads.
   *
   * @return The number of threads.
   */
  public Integer getThreads() {
    return threads;
  }

  /**
   * Sets the number of threads.
   *
   * @param threads The number of threads to set.
   */
  public void setThreads(Integer threads) {
    this.threads = threads;
  }

  /**
   * Gets the memory limit in MiB.
   *
   * @return The memory limit in MiB.
   */
  public Integer getMemoryLimitInMiB() {
    return memoryLimitInMiB;
  }

  /**
   * Sets the memory limit in MiB.
   *
   * @param memoryLimitInMiB The memory limit in MiB to set.
   */
  public void setMemoryLimitInMiB(Integer memoryLimitInMiB) {
    this.memoryLimitInMiB = memoryLimitInMiB;
  }

  /**
   * Gets the memory limit fraction.
   *
   * @return The memory limit fraction.
   */
  public Double getMemoryLimitFraction() {
    return memoryLimitFraction;
  }

  /**
   * Sets the memory limit fraction.
   *
   * @param memoryLimitFraction The memory limit fraction to set.
   */
  public void setMemoryLimitFraction(Double memoryLimitFraction) {
    this.memoryLimitFraction = memoryLimitFraction;
  }

  /**
   * Gets the temporary storage limit in MiB.
   *
   * @return The temporary storage limit in MiB.
   */
  public Integer getTempStorageLimitInMiB() {
    return tempStorageLimitInMiB;
  }

  /**
   * Sets the temporary storage limit in MiB.
   *
   * @param tempStorageLimitInMiB The temporary storage limit in MiB to set.
   */
  public void setTempStorageLimitInMiB(Integer tempStorageLimitInMiB) {
    this.tempStorageLimitInMiB = tempStorageLimitInMiB;
  }

  /**
   * Gets the local directory path.
   *
   * @return The local directory path.
   */
  public String getLocalDir() {
    return localDir;
  }

  /**
   * Sets the local directory path.
   *
   * @param localDir The local directory path to set.
   */
  public void setLocalDir(String localDir) {
    this.localDir = localDir;
  }

  /**
   * Gets the set of caching catalog namespaces.
   *
   * @return The set of caching catalog namespaces.
   */
  public Set<String> getCachingCatalogNamespaces() {
    return cachingCatalogNamespaces;
  }

  /**
   * Sets the caching catalog namespaces.
   *
   * @param cachingCatalogNamespaces The set of caching catalog namespaces to set.
   */
  public void setCachingCatalogNamespaces(Set<String> cachingCatalogNamespaces) {
    this.cachingCatalogNamespaces = cachingCatalogNamespaces;
  }

  /**
   * Gets the set of caching catalog table names.
   *
   * @return The set of caching catalog table names.
   */
  public Set<String> getCachingCatalogTableNames() {
    return cachingCatalogTableNames;
  }

  /**
   * Sets the caching catalog table names.
   *
   * @param cachingCatalogTableNames The set of caching catalog table names to set.
   */
  public void setCachingCatalogTableNames(Set<String> cachingCatalogTableNames) {
    this.cachingCatalogTableNames = cachingCatalogTableNames;
  }

  /**
   * Gets the caching catalog expiration interval in seconds.
   *
   * @return The caching catalog expiration interval in seconds.
   */
  public int getCachingCatalogExpirationIntervalInSeconds() {
    return cachingCatalogExpirationIntervalInSeconds;
  }

  /**
   * Sets the caching catalog expiration interval in seconds.
   *
   * @param cachingCatalogExpirationIntervalInSeconds The caching catalog expiration interval in
   *     seconds to set.
   */
  public void setCachingCatalogExpirationIntervalInSeconds(
      int cachingCatalogExpirationIntervalInSeconds) {
    this.cachingCatalogExpirationIntervalInSeconds = cachingCatalogExpirationIntervalInSeconds;
  }

  /**
   * Gets the MyBatis configuration path.
   *
   * @return The MyBatis configuration path.
   */
  public String getMybatisConfigPath() {
    return mybatisConfigPath;
  }

  /**
   * Sets the MyBatis configuration path.
   *
   * @param mybatisConfigPath The MyBatis configuration path to set.
   */
  public void setMybatisConfigPath(String mybatisConfigPath) {
    this.mybatisConfigPath = mybatisConfigPath;
  }

  /**
   * Gets the maximum number of partition writer threads.
   *
   * @return The maximum number of partition writer threads.
   */
  public Integer getMaxPartitionWriterThreads() {
    return maxPartitionWriterThreads;
  }

  /**
   * Sets the maximum number of partition writer threads.
   *
   * @param maxPartitionWriterThreads The maximum number of partition writer threads to set.
   */
  public void setMaxPartitionWriterThreads(Integer maxPartitionWriterThreads) {
    this.maxPartitionWriterThreads = maxPartitionWriterThreads;
  }

  /**
   * Checks if community extensions are allowed.
   *
   * @return true if community extensions are allowed, false otherwise
   */
  public boolean isAllowCommunityExtensions() {
    return allowCommunityExtensions;
  }

  /**
   * Sets whether community extensions are allowed.
   *
   * @param allowCommunityExtensions true to allow community extensions, false to disallow
   */
  public void setAllowCommunityExtensions(boolean allowCommunityExtensions) {
    this.allowCommunityExtensions = allowCommunityExtensions;
  }

  /**
   * Checks if known extensions should be automatically installed.
   *
   * @return true if auto-installation is enabled, false otherwise
   */
  public boolean isAutoInstallKnownExtensions() {
    return autoInstallKnownExtensions;
  }

  /**
   * Sets whether known extensions should be automatically installed.
   *
   * @param autoInstallKnownExtensions true to enable auto-installation, false to disable
   */
  public void setAutoInstallKnownExtensions(boolean autoInstallKnownExtensions) {
    this.autoInstallKnownExtensions = autoInstallKnownExtensions;
  }

  /**
   * Checks if the DuckDB configuration is locked.
   *
   * @return true if the configuration is locked, false otherwise
   */
  public boolean isLockDuckDBConfiguration() {
    return lockDuckDBConfiguration;
  }

  /**
   * Sets whether the DuckDB configuration should be locked.
   *
   * @param lockDuckDBConfiguration true to lock the configuration, false to unlock
   */
  public void setLockDuckDBConfiguration(boolean lockDuckDBConfiguration) {
    this.lockDuckDBConfiguration = lockDuckDBConfiguration;
  }

  /**
   * Gets the total file size limit per scan in MiB.
   *
   * @return the file size limit in MiB, or null if not set
   */
  public Integer getTotalFileSizePerScanLimitInMiB() {
    return totalFileSizePerScanLimitInMiB;
  }

  /**
   * Sets the total file size limit per scan in MiB.
   *
   * @param totalFileSizePerScanLimitInMiB the file size limit in MiB
   */
  public void setTotalFileSizePerScanLimitInMiB(Integer totalFileSizePerScanLimitInMiB) {
    this.totalFileSizePerScanLimitInMiB = totalFileSizePerScanLimitInMiB;
  }

  /**
   * Gets the query timeout in seconds.
   *
   * @return the query timeout in seconds, or null if not set
   */
  public Integer getQueryTimeoutInSeconds() {
    return queryTimeoutInSeconds;
  }

  /**
   * Sets the query timeout in seconds.
   *
   * @param queryTimeoutInSeconds the query timeout in seconds
   */
  public void setQueryTimeoutInSeconds(Integer queryTimeoutInSeconds) {
    this.queryTimeoutInSeconds = queryTimeoutInSeconds;
  }

  /**
   * Gets the maximum number of active connections.
   *
   * @return the maximum number of active connections, or null if not set
   */
  public Integer getMaxActiveConnections() {
    return maxActiveConnections;
  }

  /**
   * Sets the maximum number of active connections.
   *
   * @param maxActiveConnections the maximum number of active connections
   */
  public void setMaxActiveConnections(Integer maxActiveConnections) {
    this.maxActiveConnections = maxActiveConnections;
  }

  /**
   * Gets the connection creation timeout in seconds.
   *
   * @return the connection creation timeout in seconds, or null if not set
   */
  public Integer getConnectionCreationTimeoutInSeconds() {
    return connectionCreationTimeoutInSeconds;
  }

  /**
   * Sets the connection creation timeout in seconds.
   *
   * @param connectionCreationTimeoutInSeconds the connection creation timeout in seconds
   */
  public void setConnectionCreationTimeoutInSeconds(Integer connectionCreationTimeoutInSeconds) {
    this.connectionCreationTimeoutInSeconds = connectionCreationTimeoutInSeconds;
  }

  /**
   * Gets the debug file upload path.
   *
   * @return the debug file upload path
   */
  public String getDebugFileUploadPath() {
    return debugFileUploadPath;
  }

  /**
   * Sets the debug file upload path.
   *
   * @param debugFileUploadPath the debug file upload path
   */
  public void setDebugFileUploadPath(String debugFileUploadPath) {
    this.debugFileUploadPath = debugFileUploadPath;
  }

  /**
   * Gets the metric collector.
   *
   * @return the metric collector
   */
  public MetricCollector getMetricCollector() {
    return metricCollector;
  }

  /**
   * Sets the metric collector.
   *
   * @param metricCollector the metric collector to set
   */
  public void setMetricCollector(MetricCollector metricCollector) {
    this.metricCollector = metricCollector;
  }

  /**
   * Gets the default value for processing tables.
   *
   * @return true if tables should be processed by default, false otherwise
   */
  public boolean getProcessTablesDefaultValue() {
    return processTablesDefaultValue;
  }

  /**
   * Sets the default value for processing tables.
   *
   * @param processTablesDefaultValue true to process tables by default, false otherwise
   */
  public void setProcessTablesDefaultValue(boolean processTablesDefaultValue) {
    this.processTablesDefaultValue = processTablesDefaultValue;
  }

  /**
   * Checks if full table scan is allowed.
   *
   * @return true if full table scan is allowed, false otherwise
   */
  public boolean isAllowFullTableScan() {
    return allowFullTableScan;
  }

  /**
   * Sets whether full table scan is allowed.
   *
   * @param allowFullTableScan true to allow full table scan, false to disallow
   */
  public void setAllowFullTableScan(boolean allowFullTableScan) {
    this.allowFullTableScan = allowFullTableScan;
  }

  /**
   * Sets the configuration function for the SwiftLakeEngine.
   *
   * @param configureFunc the function to configure the SwiftLakeEngine
   */
  public void setConfigureFunc(Function<SwiftLakeEngine, List<AutoCloseable>> configureFunc) {
    this.configureFunc = configureFunc;
  }

  /**
   * Gets the configuration function for the SwiftLakeEngine.
   *
   * @return the function to configure the SwiftLakeEngine
   */
  public Function<SwiftLakeEngine, List<AutoCloseable>> getConfigureFunc() {
    return configureFunc;
  }
}
