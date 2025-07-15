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
package com.arcesium.swiftlake.writer;

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.util.LocationUtil;

public class UUIDBasedLocationProvider implements LocationProvider {
  private final String dataLocation;

  public UUIDBasedLocationProvider(String tableLocation, Map<String, String> properties) {
    this.dataLocation = LocationUtil.stripTrailingSlash(dataLocation(properties, tableLocation));
  }

  private static String dataLocation(Map<String, String> properties, String tableLocation) {
    String dataLocation = (String) properties.get("write.data.path");
    if (dataLocation == null) {
      dataLocation = (String) properties.get("write.folder-storage.path");
      if (dataLocation == null) {
        dataLocation = String.format("%s/data", tableLocation);
      }
    }

    return dataLocation;
  }

  public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
    return String.format("%s/%s/%s", this.dataLocation, UUID.randomUUID(), filename);
  }

  public String newDataLocation(String filename) {
    return String.format("%s/%s", this.dataLocation, filename);
  }
}
