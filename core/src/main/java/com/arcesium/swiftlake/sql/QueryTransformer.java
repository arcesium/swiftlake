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

/**
 * Functional interface for transforming SQL queries. This interface provides a single method to
 * transform an original SQL query into a modified version.
 */
@FunctionalInterface
public interface QueryTransformer {

  /**
   * Transforms the given SQL query.
   *
   * @param originalSql The original SQL query to be transformed.
   * @return The transformed SQL query as a String.
   */
  String transform(String originalSql);
}
