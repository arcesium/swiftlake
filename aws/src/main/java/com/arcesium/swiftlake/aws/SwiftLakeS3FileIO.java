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

import com.arcesium.swiftlake.common.FileUtil;
import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.io.SwiftLakeFileIO;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

/**
 * Implements SwiftLakeFileIO for S3 file operations. Extends S3FileIO to provide additional
 * functionality for SwiftLake.
 */
public class SwiftLakeS3FileIO extends S3FileIO implements SwiftLakeFileIO {
  private static final Logger LOGGER = LoggerFactory.getLogger(SwiftLakeS3FileIO.class);
  private static final String SCHEME_DELIM = "://";
  private static final String PATH_DELIM = "/";
  private SwiftLakeS3FileIOProperties s3FileIOProperties;
  private S3TransferManager s3TransferManager;
  private String localDir;
  private boolean duckDBS3ExtensionEnabled;
  private Long duckDBS3ExtensionThresholdInBytes;

  /** Default constructor for SwiftLakeS3FileIO. */
  public SwiftLakeS3FileIO() {}

  /**
   * Constructs a SwiftLakeS3FileIO with a specified S3TransferManager.
   *
   * @param s3TransferManager The S3TransferManager to be used for file operations.
   */
  public SwiftLakeS3FileIO(S3TransferManager s3TransferManager) {
    this.s3TransferManager = s3TransferManager;
  }

  /**
   * Constructs a SwiftLakeS3FileIO with specified S3TransferManager, S3Client supplier, and
   * properties.
   *
   * @param s3TransferManager The S3TransferManager to be used for file operations.
   * @param s3 A SerializableSupplier that provides an S3Client.
   * @param s3FileIOProperties The SwiftLakeS3FileIOProperties to configure the file I/O operations.
   */
  public SwiftLakeS3FileIO(
      S3TransferManager s3TransferManager,
      SerializableSupplier<S3Client> s3,
      SwiftLakeS3FileIOProperties s3FileIOProperties) {
    super(s3, s3FileIOProperties);
    this.s3FileIOProperties = s3FileIOProperties;
    this.s3TransferManager = s3TransferManager;
  }

  /**
   * Initializes the SwiftLakeS3FileIO with the given properties.
   *
   * @param properties The properties to initialize with.
   */
  @Override
  public void initialize(Map<String, String> properties) {
    super.initialize(properties);
    this.s3FileIOProperties = new SwiftLakeS3FileIOProperties(properties);
    this.localDir =
        FileUtil.normalizePath(s3FileIOProperties.stagingDirectory(), false, true)
            + "/s3_files_stage_dir/";
    new File(localDir).mkdirs();
    if (s3TransferManager == null) {
      this.s3TransferManager = s3FileIOProperties.getS3TransferManager(properties);
    }
    this.duckDBS3ExtensionEnabled = s3FileIOProperties.isDuckDBS3ExtensionEnabled();
    this.duckDBS3ExtensionThresholdInBytes =
        s3FileIOProperties.getDuckDBS3ExtensionThresholdInBytes();
    if (duckDBS3ExtensionEnabled && duckDBS3ExtensionThresholdInBytes == null) {
      this.duckDBS3ExtensionThresholdInBytes = 0L;
    }
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public InputFiles newInputFiles(List<String> paths) {
    return downloadFiles(paths);
  }

  @Override
  public boolean isDownloadable(String path, long length) {
    return !duckDBS3ExtensionEnabled || length < duckDBS3ExtensionThresholdInBytes;
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  @Override
  public String getLocalDir() {
    return localDir;
  }

  @Override
  public CompletableFuture<Void> downloadFileAsync(String source, Path destination) {
    ValidationException.checkNotNull(source, "source cannot be null.");
    ValidationException.checkNotNull(destination, "destination cannot be null.");
    Pair<String, String> parts = splitS3Path(source);
    String bucket = resolveBucket(parts.getLeft());
    String prefix = parts.getRight();
    DownloadFileRequest.Builder builder =
        DownloadFileRequest.builder()
            .getObjectRequest(
                requestBuilder -> {
                  requestBuilder.bucket(bucket).key(prefix);
                  AwsUtil.configureEncryption(s3FileIOProperties, requestBuilder);
                })
            .destination(destination);
    return s3TransferManager.downloadFile(builder.build()).completionFuture().thenAccept((d) -> {});
  }

  @Override
  public CompletableFuture<Void> uploadFileAsync(Path source, String destination) {
    ValidationException.checkNotNull(source, "source cannot be null.");
    ValidationException.checkNotNull(destination, "destination cannot be null.");
    Pair<String, String> parts = splitS3Path(destination);
    String bucket = resolveBucket(parts.getLeft());
    String prefix = parts.getRight();
    UploadFileRequest.Builder builder =
        UploadFileRequest.builder()
            .putObjectRequest(
                requestBuilder -> {
                  requestBuilder.bucket(bucket).key(prefix);
                  AwsUtil.configureEncryption(s3FileIOProperties, requestBuilder);
                  AwsUtil.configurePermission(s3FileIOProperties, requestBuilder);
                })
            .source(source);
    return s3TransferManager.uploadFile(builder.build()).completionFuture().thenAccept(u -> {});
  }

  private Pair<String, String> splitS3Path(String path) {
    String[] schemeSplit = path.split(SCHEME_DELIM, -1);
    ValidationException.check(
        schemeSplit.length == 2, "Invalid S3 URI, cannot determine scheme: %s", path);
    ValidationException.check(schemeSplit[0].equalsIgnoreCase("s3"), "Invalid S3 URI: %s", path);
    String[] authoritySplit = schemeSplit[1].split(PATH_DELIM, 2);
    ValidationException.check(
        authoritySplit.length == 2, "Invalid S3 URI, cannot determine prefix: %s", path);
    return Pair.of(authoritySplit[0], authoritySplit[1]);
  }

  private String resolveBucket(String bucket) {
    return (s3FileIOProperties.bucketToAccessPointMapping() != null
            && s3FileIOProperties.bucketToAccessPointMapping().containsKey(bucket))
        ? s3FileIOProperties.bucketToAccessPointMapping().get(bucket)
        : bucket;
  }
}
