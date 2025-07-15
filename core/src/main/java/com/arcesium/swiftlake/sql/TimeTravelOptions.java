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
package com.arcesium.swiftlake.sql;

import java.time.LocalDateTime;

/**
 * Represents options for time travel queries. This class encapsulates different ways to specify a
 * point in time for data retrieval.
 */
public class TimeTravelOptions {
  private final LocalDateTime timestamp;
  private final String branchOrTagName;
  private final Long snapshotId;

  /**
   * Constructs a TimeTravelOptions object with the specified parameters.
   *
   * @param timestamp The timestamp for time travel query
   * @param branchOrTagName The name of the branch or tag for time travel query
   * @param snapshotId The ID of the snapshot for time travel query
   */
  public TimeTravelOptions(LocalDateTime timestamp, String branchOrTagName, Long snapshotId) {
    this.timestamp = timestamp;
    this.branchOrTagName = branchOrTagName;
    this.snapshotId = snapshotId;
  }

  /**
   * Gets the timestamp for time travel query.
   *
   * @return The timestamp
   */
  public LocalDateTime getTimestamp() {
    return timestamp;
  }

  /**
   * Gets the name of the branch or tag for time travel query.
   *
   * @return The branch or tag name
   */
  public String getBranchOrTagName() {
    return branchOrTagName;
  }

  /**
   * Gets the ID of the snapshot for time travel query.
   *
   * @return The snapshot ID
   */
  public Long getSnapshotId() {
    return snapshotId;
  }

  /**
   * Returns a string representation of this TimeTravelOptions object.
   *
   * @return A string representation of this object
   */
  @Override
  public String toString() {
    return "TimeTravelOptions{"
        + "timestamp="
        + timestamp
        + ", branchOrTagName='"
        + branchOrTagName
        + '\''
        + ", snapshotId="
        + snapshotId
        + '}';
  }
}
