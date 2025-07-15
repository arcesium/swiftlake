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

import com.google.common.base.Strings;
import java.util.Map;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * Properties for SwiftLakeS3FileIO configuration in SwiftLake. Extends S3FileIOProperties with
 * additional SwiftLake-specific settings.
 */
public class SwiftLakeS3FileIOProperties extends S3FileIOProperties {
  /** Property key for S3 transfer manager provider. */
  public static final String TRANSFER_MANAGER_PROVIDER = "s3.transfer-manager-provider";

  /** Default S3 transfer manager provider class. */
  public static final String TRANSFER_MANAGER_PROVIDER_DEFAULT =
      SingletonS3TransferManagerProvider.class.getName();

  /** Prefix for S3 transfer manager provider properties. */
  public static final String TRANSFER_MANAGER_PROVIDER_PREFIX = "s3.transfer-manager-provider.";

  /** Property key for CRT target throughput in Gbps. */
  public static final String CRT_TARGET_THROUGHPUT_GBPS = "s3.crt.target-throughput-gbps";

  /** Default CRT target throughput in Gbps. */
  public static final double CRT_TARGET_THROUGHPUT_GBPS_DEFAULT = 10.0;

  /** Property key for CRT max concurrency. */
  public static final String CRT_MAX_CONCURRENCY = "s3.crt.max-concurrency";

  /** Property key for CRT max native memory limit in bytes. */
  public static final String CRT_MAX_NATIVE_MEMORY_LIMIT_BYTES =
      "s3.crt.max-native-memory-limit-bytes";

  /** Property key for CRT multipart threshold in bytes. */
  public static final String CRT_MULTIPART_THRESHOLD_BYTES = "s3.crt.multipart.threshold-bytes";

  /** Property key for enabling DuckDB S3 extension. */
  public static final String DUCKDB_S3_EXTENSION_ENABLED = "s3.duckdb.s3-extension-enabled";

  /** Default value for enabling DuckDB S3 extension. */
  public static final boolean DUCKDB_S3_EXTENSION_ENABLED_DEFAULT = false;

  /** Property key for DuckDB S3 extension threshold in bytes. */
  public static final String DUCKDB_S3_EXTENSION_THRESHOLD_BYTES =
      "s3.duckdb.s3-extension-threshold-bytes";

  private String s3TransferManagerProviderClass;
  private Map<String, String> s3TransferManagerProviderProperties;
  private Double crtTargetThroughputInGbps;
  private Integer crtMaxConcurrency;
  private Long crtMaxNativeMemoryLimitInBytes;
  private Long crtMultipartThresholdInBytes;
  private boolean duckDBS3ExtensionEnabled;
  private Long duckDBS3ExtensionThresholdInBytes;

  /** Default constructor. */
  public SwiftLakeS3FileIOProperties() {
    super();
    this.crtTargetThroughputInGbps = CRT_TARGET_THROUGHPUT_GBPS_DEFAULT;
    this.duckDBS3ExtensionEnabled = DUCKDB_S3_EXTENSION_ENABLED_DEFAULT;
  }

  /**
   * Constructor with properties.
   *
   * @param properties Map of property key-value pairs
   */
  public SwiftLakeS3FileIOProperties(Map<String, String> properties) {
    super(properties);
    this.s3TransferManagerProviderClass =
        PropertyUtil.propertyAsString(
            properties, TRANSFER_MANAGER_PROVIDER, TRANSFER_MANAGER_PROVIDER_DEFAULT);
    this.s3TransferManagerProviderProperties =
        PropertyUtil.propertiesWithPrefix(properties, TRANSFER_MANAGER_PROVIDER_PREFIX);
    this.crtTargetThroughputInGbps =
        PropertyUtil.propertyAsDouble(
            properties, CRT_TARGET_THROUGHPUT_GBPS, CRT_TARGET_THROUGHPUT_GBPS_DEFAULT);
    this.crtMaxConcurrency = PropertyUtil.propertyAsNullableInt(properties, CRT_MAX_CONCURRENCY);
    this.crtMaxNativeMemoryLimitInBytes =
        PropertyUtil.propertyAsNullableLong(properties, CRT_MAX_NATIVE_MEMORY_LIMIT_BYTES);
    this.crtMultipartThresholdInBytes =
        PropertyUtil.propertyAsNullableLong(properties, CRT_MULTIPART_THRESHOLD_BYTES);
    this.duckDBS3ExtensionEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, DUCKDB_S3_EXTENSION_ENABLED, DUCKDB_S3_EXTENSION_ENABLED_DEFAULT);
    this.duckDBS3ExtensionThresholdInBytes =
        PropertyUtil.propertyAsNullableLong(properties, DUCKDB_S3_EXTENSION_THRESHOLD_BYTES);
  }

  /**
   * Get the S3 transfer manager provider class.
   *
   * @return S3 transfer manager provider class name
   */
  public String getS3TransferManagerProviderClass() {
    return s3TransferManagerProviderClass;
  }

  /**
   * Get the S3 transfer manager provider properties.
   *
   * @return Map of S3 transfer manager provider properties
   */
  public Map<String, String> getS3TransferManagerProviderProperties() {
    return s3TransferManagerProviderProperties;
  }

  /**
   * Get the CRT target throughput in Gbps.
   *
   * @return CRT target throughput in Gbps
   */
  public Double getCrtTargetThroughputInGbps() {
    return crtTargetThroughputInGbps;
  }

  /**
   * Get the CRT max concurrency.
   *
   * @return CRT max concurrency
   */
  public Integer getCrtMaxConcurrency() {
    return crtMaxConcurrency;
  }

  /**
   * Get the CRT max native memory limit in bytes.
   *
   * @return CRT max native memory limit in bytes
   */
  public Long getCrtMaxNativeMemoryLimitInBytes() {
    return crtMaxNativeMemoryLimitInBytes;
  }

  /**
   * Get the CRT multipart threshold in bytes.
   *
   * @return CRT multipart threshold in bytes
   */
  public Long getCrtMultipartThresholdInBytes() {
    return crtMultipartThresholdInBytes;
  }

  /**
   * Check if DuckDB S3 extension is enabled.
   *
   * @return true if DuckDB S3 extension is enabled, false otherwise
   */
  public boolean isDuckDBS3ExtensionEnabled() {
    return duckDBS3ExtensionEnabled;
  }

  /**
   * Get the DuckDB S3 extension threshold in bytes.
   *
   * @return DuckDB S3 extension threshold in bytes
   */
  public Long getDuckDBS3ExtensionThresholdInBytes() {
    return duckDBS3ExtensionThresholdInBytes;
  }

  /**
   * Apply credential configurations to the S3CrtAsyncClientBuilder.
   *
   * @param awsClientProperties AWS client properties
   * @param builder S3CrtAsyncClientBuilder to configure
   * @param <T> Type extending S3CrtAsyncClientBuilder
   */
  public <T extends S3CrtAsyncClientBuilder> void applyCredentialConfigurations(
      AwsClientProperties awsClientProperties, T builder) {
    builder.credentialsProvider(getAwsCredentialsProvider(awsClientProperties));
  }

  /**
   * Get the AWS credentials provider.
   *
   * @param awsClientProperties AWS client properties
   * @return AwsCredentialsProvider instance
   */
  public AwsCredentialsProvider getAwsCredentialsProvider(AwsClientProperties awsClientProperties) {
    return isRemoteSigningEnabled()
        ? AnonymousCredentialsProvider.create()
        : awsClientProperties.credentialsProvider(accessKeyId(), secretAccessKey(), sessionToken());
  }

  /**
   * Get the S3TransferManager instance.
   *
   * @param properties Map of property key-value pairs
   * @return S3TransferManager instance or null if provider class is not set
   */
  public S3TransferManager getS3TransferManager(Map<String, String> properties) {
    if (Strings.isNullOrEmpty(s3TransferManagerProviderClass)) {
      return null;
    }
    S3TransferManagerProvider provider =
        createS3TransferManagerProviderInstance(s3TransferManagerProviderClass);
    return provider.getS3TransferManager(properties);
  }

  /**
   * Create an instance of S3TransferManagerProvider.
   *
   * @param impl S3TransferManagerProvider implementation class name
   * @return S3TransferManagerProvider instance
   * @throws IllegalArgumentException if the instance cannot be created
   */
  private S3TransferManagerProvider createS3TransferManagerProviderInstance(String impl) {
    DynConstructors.Ctor<S3TransferManagerProvider> ctor;
    try {
      ctor =
          DynConstructors.builder(S3TransferManagerProvider.class)
              .loader(SwiftLakeS3FileIOProperties.class.getClassLoader())
              .impl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot create S3TransferManagerProvider implementation %s: %s",
              impl, e.getMessage()),
          e);
    }

    try {
      return ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot create %s, %s does not implement S3TransferManagerProvider.", impl, impl),
          e);
    }
  }
}
