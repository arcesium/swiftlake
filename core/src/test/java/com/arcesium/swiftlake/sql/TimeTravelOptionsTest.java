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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TimeTravelOptionsTest {

  @Test
  void testConstructorAndGetters() {
    LocalDateTime timestamp = LocalDateTime.of(2023, 5, 15, 10, 30);
    String branchName = "main";
    Long snapshotId = 123456L;

    TimeTravelOptions options = new TimeTravelOptions(timestamp, branchName, snapshotId);

    assertThat(options.getTimestamp()).isEqualTo(timestamp);
    assertThat(options.getBranchOrTagName()).isEqualTo(branchName);
    assertThat(options.getSnapshotId()).isEqualTo(snapshotId);
  }

  @Test
  void testConstructorWithNullValues() {
    TimeTravelOptions options = new TimeTravelOptions(null, null, null);

    assertThat(options.getTimestamp()).isNull();
    assertThat(options.getBranchOrTagName()).isNull();
    assertThat(options.getSnapshotId()).isNull();
  }

  @ParameterizedTest
  @MethodSource("provideTimeTravelOptions")
  void testToString(
      LocalDateTime timestamp, String branchOrTagName, Long snapshotId, String expectedString) {
    TimeTravelOptions options = new TimeTravelOptions(timestamp, branchOrTagName, snapshotId);
    assertThat(options.toString()).isEqualTo(expectedString);
  }

  private static Stream<Arguments> provideTimeTravelOptions() {
    LocalDateTime timestamp = LocalDateTime.of(2023, 5, 15, 10, 30);
    return Stream.of(
        Arguments.of(
            timestamp,
            "main",
            123456L,
            "TimeTravelOptions{timestamp=2023-05-15T10:30, branchOrTagName='main', snapshotId=123456}"),
        Arguments.of(
            null,
            "develop",
            null,
            "TimeTravelOptions{timestamp=null, branchOrTagName='develop', snapshotId=null}"),
        Arguments.of(
            timestamp,
            null,
            789012L,
            "TimeTravelOptions{timestamp=2023-05-15T10:30, branchOrTagName='null', snapshotId=789012}"),
        Arguments.of(
            null,
            null,
            null,
            "TimeTravelOptions{timestamp=null, branchOrTagName='null', snapshotId=null}"));
  }
}
