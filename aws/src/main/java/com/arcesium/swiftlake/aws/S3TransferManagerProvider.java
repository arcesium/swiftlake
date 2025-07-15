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

/** Provides an interface for obtaining an S3TransferManager instance. */
public interface S3TransferManagerProvider {

  /**
   * Returns an S3TransferManager instance based on the provided properties.
   *
   * @param properties A map containing configuration properties for the S3TransferManager
   * @return An instance of S3TransferManager
   */
  S3TransferManager getS3TransferManager(Map<String, String> properties);
}
