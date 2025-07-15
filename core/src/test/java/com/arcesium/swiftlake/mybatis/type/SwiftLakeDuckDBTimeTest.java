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
package com.arcesium.swiftlake.mybatis.type;

import static org.assertj.core.api.Assertions.*;

import java.sql.Time;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class SwiftLakeDuckDBTimeTest {

  @Test
  void testConstructorWithTime() {
    Time time = Time.valueOf("12:34:56");
    SwiftLakeDuckDBTime duckDBTime = new SwiftLakeDuckDBTime(time);

    assertThat(TimeUnit.MICROSECONDS.toMillis(duckDBTime.getMicrosEpoch()))
        .isEqualTo(time.getTime());
  }

  @ParameterizedTest
  @MethodSource("provideLocalTimes")
  void testConstructorWithLocalTime(LocalTime localTime) {
    SwiftLakeDuckDBTime duckDBTime = new SwiftLakeDuckDBTime(localTime);

    assertThat(duckDBTime.getMicrosEpoch())
        .isEqualTo(TimeUnit.NANOSECONDS.toMicros(localTime.toNanoOfDay()));
  }

  @Test
  void testConstructorWithNullTime() {
    assertThatThrownBy(() -> new SwiftLakeDuckDBTime((Time) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void testConstructorWithNullLocalTime() {
    assertThatThrownBy(() -> new SwiftLakeDuckDBTime((LocalTime) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void testToString() {
    LocalTime localTime = LocalTime.of(12, 34, 56);
    SwiftLakeDuckDBTime duckDBTime = new SwiftLakeDuckDBTime(localTime);

    assertThat(duckDBTime.toString()).contains("12:34:56");
  }

  private static Stream<LocalTime> provideLocalTimes() {
    return Stream.of(
        LocalTime.of(0, 0, 0),
        LocalTime.of(23, 59, 59, 999999999),
        LocalTime.NOON,
        LocalTime.MIDNIGHT,
        LocalTime.now(),
        LocalTime.MIN,
        LocalTime.MAX);
  }
}
