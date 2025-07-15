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

import com.arcesium.swiftlake.SwiftLakeEngine;
import com.arcesium.swiftlake.common.SwiftLakeException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Request;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

/** Utility class for SwiftLake AWS operations. */
public class AwsUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(AwsUtil.class);

  /**
   * Creates and configures an S3AsyncClient using the AWS CRT-based implementation.
   *
   * @param properties Map of configuration properties for the S3AsyncClient
   * @return A configured S3AsyncClient instance
   */
  public static S3AsyncClient createS3CrtAsyncClient(Map<String, String> properties) {
    AwsClientProperties awsClientProperties = new AwsClientProperties(properties);
    SwiftLakeS3FileIOProperties s3FileIOProperties = new SwiftLakeS3FileIOProperties(properties);
    S3CrtAsyncClientBuilder builder = S3AsyncClient.crtBuilder();
    if (awsClientProperties.clientRegion() != null) {
      builder.region(Region.of(awsClientProperties.clientRegion()));
    }
    if (s3FileIOProperties.getCrtTargetThroughputInGbps() != null) {
      builder.targetThroughputInGbps(s3FileIOProperties.getCrtTargetThroughputInGbps());
    }
    if (s3FileIOProperties.getCrtMaxConcurrency() != null) {
      builder.maxConcurrency(s3FileIOProperties.getCrtMaxConcurrency());
    }
    builder.minimumPartSizeInBytes((long) s3FileIOProperties.multiPartSize());
    if (s3FileIOProperties.getCrtMultipartThresholdInBytes() != null) {
      builder.thresholdInBytes(s3FileIOProperties.getCrtMultipartThresholdInBytes());
    }
    if (s3FileIOProperties.getCrtMaxNativeMemoryLimitInBytes() != null) {
      builder.maxNativeMemoryLimitInBytes(s3FileIOProperties.getCrtMaxNativeMemoryLimitInBytes());
    }
    if (s3FileIOProperties.endpoint() != null) {
      builder.endpointOverride(URI.create(s3FileIOProperties.endpoint()));
    }
    builder.crossRegionAccessEnabled(s3FileIOProperties.isCrossRegionAccessEnabled());
    builder.accelerate(s3FileIOProperties.isAccelerationEnabled());
    builder.forcePathStyle(s3FileIOProperties.isPathStyleAccess());
    builder.retryConfiguration(r -> r.numRetries(s3FileIOProperties.s3RetryNumRetries()));
    builder.applyMutation(
        s3CrtAsyncClientBuilder ->
            s3FileIOProperties.applyCredentialConfigurations(
                awsClientProperties, s3CrtAsyncClientBuilder));
    return builder.build();
  }

  /**
   * Configures encryption for a PutObjectRequest.
   *
   * @param s3FileIOProperties The S3FileIOProperties containing encryption settings.
   * @param requestBuilder The PutObjectRequest.Builder to configure.
   */
  public static void configureEncryption(
      S3FileIOProperties s3FileIOProperties, PutObjectRequest.Builder requestBuilder) {
    configureEncryption(
        s3FileIOProperties,
        requestBuilder::serverSideEncryption,
        requestBuilder::ssekmsKeyId,
        requestBuilder::sseCustomerAlgorithm,
        requestBuilder::sseCustomerKey,
        requestBuilder::sseCustomerKeyMD5);
  }

  /**
   * Configures encryption for a GetObjectRequest.
   *
   * @param s3FileIOProperties The S3FileIOProperties containing encryption settings.
   * @param requestBuilder The GetObjectRequest.Builder to configure.
   */
  public static void configureEncryption(
      S3FileIOProperties s3FileIOProperties, GetObjectRequest.Builder requestBuilder) {
    configureEncryption(
        s3FileIOProperties,
        sse -> null,
        s -> null,
        requestBuilder::sseCustomerAlgorithm,
        requestBuilder::sseCustomerKey,
        requestBuilder::sseCustomerKeyMD5);
  }

  @SuppressWarnings("ReturnValueIgnored")
  static void configureEncryption(
      S3FileIOProperties s3FileIOProperties,
      Function<ServerSideEncryption, S3Request.Builder> encryptionSetter,
      Function<String, S3Request.Builder> kmsKeySetter,
      Function<String, S3Request.Builder> customAlgorithmSetter,
      Function<String, S3Request.Builder> customKeySetter,
      Function<String, S3Request.Builder> customMd5Setter) {
    if (s3FileIOProperties.sseType() == null) {
      return;
    }

    switch (s3FileIOProperties.sseType().toLowerCase(Locale.ENGLISH)) {
      case S3FileIOProperties.SSE_TYPE_NONE:
        break;

      case S3FileIOProperties.SSE_TYPE_KMS:
        encryptionSetter.apply(ServerSideEncryption.AWS_KMS);
        kmsKeySetter.apply(s3FileIOProperties.sseKey());
        break;

      case S3FileIOProperties.DSSE_TYPE_KMS:
        encryptionSetter.apply(ServerSideEncryption.AWS_KMS_DSSE);
        kmsKeySetter.apply(s3FileIOProperties.sseKey());
        break;

      case S3FileIOProperties.SSE_TYPE_S3:
        encryptionSetter.apply(ServerSideEncryption.AES256);
        break;

      case S3FileIOProperties.SSE_TYPE_CUSTOM:
        // setters for SSE-C exist for all request builders, no need to check null
        customAlgorithmSetter.apply(ServerSideEncryption.AES256.name());
        customKeySetter.apply(s3FileIOProperties.sseKey());
        customMd5Setter.apply(s3FileIOProperties.sseMd5());
        break;

      default:
        throw new IllegalArgumentException(
            "Cannot support given S3 encryption type: " + s3FileIOProperties.sseType());
    }
  }

  /**
   * Configures permissions for a PutObjectRequest.
   *
   * @param s3FileIOProperties The S3FileIOProperties containing permission settings.
   * @param requestBuilder The PutObjectRequest.Builder to configure.
   */
  public static void configurePermission(
      S3FileIOProperties s3FileIOProperties, PutObjectRequest.Builder requestBuilder) {
    configurePermission(s3FileIOProperties, requestBuilder::acl);
  }

  @SuppressWarnings("ReturnValueIgnored")
  static void configurePermission(
      S3FileIOProperties s3FileIOProperties,
      Function<ObjectCannedACL, S3Request.Builder> aclSetter) {
    aclSetter.apply(s3FileIOProperties.acl());
  }

  /**
   * Initializes DuckDB S3 integration for SwiftLake.
   *
   * @param swiftLakeEngine The SwiftLakeEngine instance.
   * @param properties A map containing configuration properties.
   * @param credentialRefreshIntervalInSeconds The interval for refreshing credentials, in seconds.
   * @return A list of AutoCloseable objects for resource management.
   */
  public static List<AutoCloseable> initializeDuckDBS3Integration(
      SwiftLakeEngine swiftLakeEngine,
      Map<String, String> properties,
      Integer credentialRefreshIntervalInSeconds) {
    SwiftLakeS3FileIOProperties s3FileIOProperties = new SwiftLakeS3FileIOProperties(properties);
    if (!s3FileIOProperties.isDuckDBS3ExtensionEnabled()) {
      return null;
    }
    AwsClientProperties awsClientProperties = new AwsClientProperties(properties);
    List<AutoCloseable> closeableList = new ArrayList<>();
    try (Connection connection = swiftLakeEngine.createDuckDBConnection();
        Statement stmt = connection.createStatement()) {
      String basePath = System.getProperty("duckdb.extensions.path");
      List<String> extensionNames = Arrays.asList("httpfs", "aws");
      if (basePath == null) {
        swiftLakeEngine.loadDuckDBExtensionsFromClassPath(
            stmt, "/duckdb_extensions/", extensionNames);
      } else {
        swiftLakeEngine.loadDuckDBExtensions(stmt, basePath, extensionNames);
      }
      AwsCredentialsProvider credentialsProvider =
          s3FileIOProperties.getAwsCredentialsProvider(awsClientProperties);
      refreshDuckDBS3Credentials(
          stmt, s3FileIOProperties, awsClientProperties, credentialsProvider);

      if (credentialRefreshIntervalInSeconds != null && credentialRefreshIntervalInSeconds > 0) {
        ScheduledExecutorService credentialRefresher = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> unused =
            credentialRefresher.scheduleAtFixedRate(
                () -> {
                  try {
                    refreshDuckDBS3Credentials(
                        swiftLakeEngine,
                        s3FileIOProperties,
                        awsClientProperties,
                        credentialsProvider);
                  } catch (Throwable t) {
                    LOGGER.error("Unable to refresh s3 credentials.", t);
                  }
                },
                credentialRefreshIntervalInSeconds,
                credentialRefreshIntervalInSeconds,
                TimeUnit.SECONDS);
        closeableList.add(credentialRefresher::shutdown);
      }
    } catch (SQLException e) {
      throw new SwiftLakeException(e, "An error occurred while configuring duckdb s3 integration.");
    }
    return closeableList;
  }

  private static void refreshDuckDBS3Credentials(
      SwiftLakeEngine swiftLakeEngine,
      SwiftLakeS3FileIOProperties s3FileIOProperties,
      AwsClientProperties awsClientProperties,
      AwsCredentialsProvider credentialsProvider) {
    try (Connection conn = swiftLakeEngine.createDuckDBConnection();
        Statement stmt = conn.createStatement(); ) {
      refreshDuckDBS3Credentials(
          stmt, s3FileIOProperties, awsClientProperties, credentialsProvider);
    } catch (SQLException e) {
      throw new SwiftLakeException(e, "An error occurred while refreshing s3 credentials.");
    }
  }

  private static void refreshDuckDBS3Credentials(
      Statement stmt,
      SwiftLakeS3FileIOProperties s3FileIOProperties,
      AwsClientProperties awsClientProperties,
      AwsCredentialsProvider credentialsProvider)
      throws SQLException {
    List<Pair<String, String>> config = new ArrayList<>();
    if (awsClientProperties.clientRegion() != null) {
      config.add(Pair.of("REGION", awsClientProperties.clientRegion()));
    }
    if (s3FileIOProperties.endpoint() != null) {
      URI uri = URI.create(s3FileIOProperties.endpoint());
      config.add(Pair.of("ENDPOINT", uri.getAuthority() + uri.getPath()));
    }
    AwsCredentials credentials = credentialsProvider.resolveCredentials();
    config.add(Pair.of("KEY_ID", credentials.accessKeyId()));
    config.add(Pair.of("SECRET", credentials.secretAccessKey()));
    if (s3FileIOProperties.isPathStyleAccess()) {
      config.add(Pair.of("URL_STYLE", "path"));
    }
    if (credentials instanceof AwsSessionCredentials awsSessionCredentials) {
      config.add(Pair.of("SESSION_TOKEN", awsSessionCredentials.sessionToken()));
    }
    stmt.execute(
        String.format(
            "CREATE OR REPLACE SECRET (TYPE S3, %s);",
            config.stream()
                .map(p -> p.getLeft() + " '" + p.getRight() + "'")
                .collect(Collectors.joining(","))));

    LOGGER.debug("Refreshed s3 credentials successfully.");
  }
}
