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

import java.util.Map;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * A singleton provider for S3TransferManager instances. This class implements the
 * S3TransferManagerProvider interface and ensures that only one instance of S3TransferManager is
 * created and reused.
 */
public class SingletonS3TransferManagerProvider implements S3TransferManagerProvider {
  /** The single instance of S3TransferManager. */
  private static volatile S3TransferManager s3TransferManager;

  /**
   * Retrieves the S3TransferManager instance, creating it if it doesn't exist.
   *
   * @param properties A map of properties used to configure the S3 client.
   * @return The singleton instance of S3TransferManager.
   */
  @Override
  public S3TransferManager getS3TransferManager(Map<String, String> properties) {
    if (s3TransferManager == null) {
      synchronized (SingletonS3TransferManagerProvider.class) {
        if (s3TransferManager == null) {
          s3TransferManager =
              S3TransferManager.builder()
                  .s3Client(AwsUtil.createS3CrtAsyncClient(properties))
                  .build();
        }
      }
    }
    return s3TransferManager;
  }
}
