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

/** Enum representing the modes of SCD1 (Slowly Changing Dimension Type 1) merge operations. */
public enum SCD1MergeMode {
  /** Represents a snapshot mode for SCD1 merge. */
  SNAPSHOT,
  /** Represents a changes-only mode for SCD1 merge. */
  CHANGES
}
