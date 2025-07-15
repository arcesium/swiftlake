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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.arcesium.swiftlake.common.FileUtil;
import com.arcesium.swiftlake.common.InputFiles;
import com.arcesium.swiftlake.common.ValidationException;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.util.SerializableSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

@ExtendWith(MockitoExtension.class)
class SwiftLakeS3FileIOTest {

  @TempDir Path tempDir;

  @Mock private S3TransferManager mockTransferManager;

  @Mock private SerializableSupplier<S3Client> mockS3ClientSupplier;

  @Mock private SwiftLakeS3FileIOProperties mockProperties;

  @Mock private InputFiles mockInputFiles;

  private SwiftLakeS3FileIO s3FileIO;
  private Map<String, String> properties;

  @BeforeEach
  void setUp() {
    s3FileIO = new SwiftLakeS3FileIO(mockTransferManager, mockS3ClientSupplier, mockProperties);

    properties = new HashMap<>();
    properties.put(SwiftLakeS3FileIOProperties.STAGING_DIRECTORY, tempDir.toString());
    properties.put(SwiftLakeS3FileIOProperties.DUCKDB_S3_EXTENSION_ENABLED, "true");
    properties.put(
        SwiftLakeS3FileIOProperties.DUCKDB_S3_EXTENSION_THRESHOLD_BYTES, "1048576"); // 1MB
    S3TransferManagerProviderImpl.s3TransferManager = null;
  }

  @Test
  void testConstructors() {
    // Default constructor
    SwiftLakeS3FileIO defaultIO = new SwiftLakeS3FileIO();
    assertThat(defaultIO).isNotNull();

    // Constructor with transfer manager
    SwiftLakeS3FileIO transferManagerIO = new SwiftLakeS3FileIO(mockTransferManager);
    assertThat(transferManagerIO).isNotNull();

    // Constructor with all parameters
    SwiftLakeS3FileIO fullIO =
        new SwiftLakeS3FileIO(mockTransferManager, mockS3ClientSupplier, mockProperties);
    assertThat(fullIO).isNotNull();
  }

  @Test
  void testInitialize() {
    try (MockedStatic<FileUtil> mockedFileUtil = mockStatic(FileUtil.class)) {
      mockedFileUtil
          .when(() -> FileUtil.normalizePath(anyString(), anyBoolean(), anyBoolean()))
          .thenReturn(tempDir.toString());

      properties.put(SwiftLakeS3FileIOProperties.STAGING_DIRECTORY, tempDir.toString());
      properties.put(SwiftLakeS3FileIOProperties.DUCKDB_S3_EXTENSION_ENABLED, "true");
      properties.put(
          SwiftLakeS3FileIOProperties.DUCKDB_S3_EXTENSION_THRESHOLD_BYTES, "1048576"); // 1MB
      S3TransferManagerProviderImpl.s3TransferManager = mockTransferManager;
      properties.put(
          SwiftLakeS3FileIOProperties.TRANSFER_MANAGER_PROVIDER,
          S3TransferManagerProviderImpl.class.getName());

      SwiftLakeS3FileIO testIO = new SwiftLakeS3FileIO();

      // Execute
      testIO.initialize(properties);

      // Verify
      String localDir = testIO.getLocalDir();
      assertThat(localDir).endsWith("s3_files_stage_dir/");
      File dirFile = new File(localDir);
      assertThat(dirFile.exists()).isTrue();
      assertThat(dirFile.isDirectory()).isTrue();

      // Verify fields were set
      assertThat(getPrivateField(testIO, "duckDBS3ExtensionEnabled", Boolean.class))
          .isEqualTo(true);
      assertThat(getPrivateField(testIO, "duckDBS3ExtensionThresholdInBytes", Long.class))
          .isEqualTo(1048576L);
      assertThat(getPrivateField(testIO, "s3TransferManager", S3TransferManager.class))
          .isEqualTo(mockTransferManager);
    }
  }

  @Test
  void testIsDownloadableWithExtensionEnabled() {
    setPrivateField(s3FileIO, "duckDBS3ExtensionEnabled", true);
    setPrivateField(s3FileIO, "duckDBS3ExtensionThresholdInBytes", 1048576L); // 1MB

    // Test cases
    assertThat(s3FileIO.isDownloadable("path", 500000)).isTrue(); // 500KB < 1MB threshold
    assertThat(s3FileIO.isDownloadable("path", 2000000)).isFalse(); // 2MB > 1MB threshold
  }

  @Test
  void testIsDownloadableWithExtensionDisabled() {
    setPrivateField(s3FileIO, "duckDBS3ExtensionEnabled", false);
    setPrivateField(s3FileIO, "duckDBS3ExtensionThresholdInBytes", 1048576L); // 1MB

    // Should always return true if extension is disabled
    assertThat(s3FileIO.isDownloadable("path", 500000)).isTrue();
    assertThat(s3FileIO.isDownloadable("path", 2000000)).isTrue();
  }

  @Test
  void testNewInputFiles() {
    s3FileIO = spy(s3FileIO);
    doReturn(mockInputFiles).when(s3FileIO).downloadFiles(any());

    List<String> paths = Arrays.asList("s3://bucket/file1.parquet", "s3://bucket/file2.parquet");
    InputFiles result = s3FileIO.newInputFiles(paths);

    // Verify
    assertThat(result).isEqualTo(mockInputFiles);
    verify(s3FileIO).downloadFiles(paths);
  }

  @Test
  void testDownloadFileAsync() {
    Path destination = tempDir.resolve("test.parquet");
    String source = "s3://bucket/path/to/file.parquet";

    when(mockProperties.bucketToAccessPointMapping()).thenReturn(null);

    FileDownload mockDownload = mock(FileDownload.class);
    CompletableFuture<Void> mockFuture = CompletableFuture.completedFuture(null);
    when(mockDownload.completionFuture()).thenReturn((CompletableFuture) mockFuture);
    when(mockTransferManager.downloadFile(any(DownloadFileRequest.class))).thenReturn(mockDownload);

    // Execute
    CompletableFuture<Void> result = s3FileIO.downloadFileAsync(source, destination);

    // Verify
    assertThat(result).isNotNull();
    result.join();

    ArgumentCaptor<DownloadFileRequest> requestCaptor =
        ArgumentCaptor.forClass(DownloadFileRequest.class);
    verify(mockTransferManager).downloadFile(requestCaptor.capture());

    DownloadFileRequest capturedRequest = requestCaptor.getValue();
    assertThat(capturedRequest.destination()).isEqualTo(destination);
  }

  @Test
  void testUploadFileAsync() {
    Path source = tempDir.resolve("test.parquet");
    String destination = "s3://bucket/path/to/file.parquet";

    when(mockProperties.bucketToAccessPointMapping()).thenReturn(null);

    FileUpload mockUpload = mock(FileUpload.class);
    CompletableFuture<Void> mockFuture = CompletableFuture.completedFuture(null);
    when(mockUpload.completionFuture()).thenReturn((CompletableFuture) mockFuture);
    when(mockTransferManager.uploadFile(any(UploadFileRequest.class))).thenReturn(mockUpload);

    // Execute
    CompletableFuture<Void> result = s3FileIO.uploadFileAsync(source, destination);

    // Verify
    assertThat(result).isNotNull();
    result.join();

    ArgumentCaptor<UploadFileRequest> requestCaptor =
        ArgumentCaptor.forClass(UploadFileRequest.class);
    verify(mockTransferManager).uploadFile(requestCaptor.capture());

    UploadFileRequest capturedRequest = requestCaptor.getValue();
    assertThat(capturedRequest.source()).isEqualTo(source);
  }

  @Test
  void testSplitS3Path() throws Exception {
    // Use reflection to access the private method
    Method splitMethod = SwiftLakeS3FileIO.class.getDeclaredMethod("splitS3Path", String.class);
    splitMethod.setAccessible(true);

    // Test valid S3 paths
    Pair<String, String> result1 =
        (Pair<String, String>) splitMethod.invoke(s3FileIO, "s3://bucket/path/to/file.parquet");
    assertThat(result1.getLeft()).isEqualTo("bucket");
    assertThat(result1.getRight()).isEqualTo("path/to/file.parquet");

    Pair<String, String> result2 =
        (Pair<String, String>) splitMethod.invoke(s3FileIO, "s3://another-bucket/file.parquet");
    assertThat(result2.getLeft()).isEqualTo("another-bucket");
    assertThat(result2.getRight()).isEqualTo("file.parquet");

    // Test invalid paths
    assertThatThrownBy(() -> splitMethod.invoke(s3FileIO, "not-s3://bucket/path"))
        .hasCauseInstanceOf(ValidationException.class)
        .cause()
        .hasMessageContaining("Invalid S3 URI");

    assertThatThrownBy(() -> splitMethod.invoke(s3FileIO, "s3://"))
        .hasCauseInstanceOf(ValidationException.class)
        .cause()
        .hasMessageContaining("cannot determine prefix");

    assertThatThrownBy(() -> splitMethod.invoke(s3FileIO, "s3://bucket"))
        .hasCauseInstanceOf(ValidationException.class)
        .cause()
        .hasMessageContaining("cannot determine prefix");
  }

  @Test
  void testResolveBucket() throws Exception {
    Map<String, String> bucketMapping = new HashMap<>();
    bucketMapping.put("mapped-bucket", "access-point");
    bucketMapping.put("another-bucket", "another-access-point");
    when(mockProperties.bucketToAccessPointMapping()).thenReturn(bucketMapping);

    // Use reflection to access the private method
    Method resolveMethod = SwiftLakeS3FileIO.class.getDeclaredMethod("resolveBucket", String.class);
    resolveMethod.setAccessible(true);

    // Test with mapped bucket
    String result1 = (String) resolveMethod.invoke(s3FileIO, "mapped-bucket");
    assertThat(result1).isEqualTo("access-point");

    // Test with unmapped bucket
    String result2 = (String) resolveMethod.invoke(s3FileIO, "unmapped-bucket");
    assertThat(result2).isEqualTo("unmapped-bucket");
  }

  @Test
  void testDownloadFileAsync_NullSource() {
    Path destination = tempDir.resolve("test.parquet");

    assertThatThrownBy(() -> s3FileIO.downloadFileAsync(null, destination))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("source cannot be null");
  }

  @Test
  void testDownloadFileAsync_NullDestination() {
    String source = "s3://bucket/path/to/file.parquet";

    assertThatThrownBy(() -> s3FileIO.downloadFileAsync(source, null))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("destination cannot be null");
  }

  @Test
  void testUploadFileAsync_NullSource() {
    String destination = "s3://bucket/path/to/file.parquet";

    assertThatThrownBy(() -> s3FileIO.uploadFileAsync(null, destination))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("source cannot be null");
  }

  @Test
  void testUploadFileAsync_NullDestination() {
    Path source = tempDir.resolve("test.parquet");

    assertThatThrownBy(() -> s3FileIO.uploadFileAsync(source, null))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("destination cannot be null");
  }

  @Test
  void testGetLogger() {
    assertThat(s3FileIO.getLogger()).isNotNull();
    assertThat(s3FileIO.getLogger().getName()).isEqualTo(SwiftLakeS3FileIO.class.getName());
  }

  @Test
  void testClose() {
    SwiftLakeS3FileIO spy = spy(s3FileIO);
    spy.close();
    verify(spy).close();
  }

  // Helper methods for reflection access to private fields

  @SuppressWarnings("unchecked")
  private <T> T getPrivateField(Object obj, String fieldName, Class<T> objClass) {
    try {
      Field field = obj.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return (T) field.get(obj);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get private field: " + fieldName, e);
    }
  }

  private void setPrivateField(Object obj, String fieldName, Object value) {
    try {
      Field field = obj.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(obj, value);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set private field: " + fieldName, e);
    }
  }
}
