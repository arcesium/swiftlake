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
package com.arcesium.swiftlake.commands;

/**
 * This class represents metadata for value columns in SwiftLake operations.
 *
 * @param <T> The type of the null replacement value
 */
public class ValueColumnMetadata<T> {
  private Double maxDeltaValue; // Numeric tolerance for value comparisons
  private T nullReplacement; // Value to use for NULL during comparison

  /**
   * Constructs a new ValueColumnsMetadata instance.
   *
   * @param maxDeltaValue The maximum delta value for numeric change tolerance
   * @param nullReplacement The replacement value for null values
   */
  public ValueColumnMetadata(Double maxDeltaValue, T nullReplacement) {
    this.maxDeltaValue = maxDeltaValue;
    this.nullReplacement = nullReplacement;
  }

  /**
   * Gets the maximum delta value for numeric change tolerance.
   *
   * @return The maximum delta value
   */
  public Double getMaxDeltaValue() {
    return maxDeltaValue;
  }

  /**
   * Gets the replacement value for null values.
   *
   * @return The null replacement value
   */
  public T getNullReplacement() {
    return nullReplacement;
  }

  /**
   * Returns a string representation of the ValueColumnsMetadata object.
   *
   * @return A string representation of the object
   */
  @Override
  public String toString() {
    return "ValueColumnsMetadata{"
        + "maxDeltaValue="
        + maxDeltaValue
        + ", nullReplacement="
        + nullReplacement
        + '}';
  }
}
