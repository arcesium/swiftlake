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
package com.arcesium.swiftlake.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class DateTimeUtilTest {

  @Test
  void testParseLocalDate_validInput() {
    LocalDate result = DateTimeUtil.parseLocalDate("2023-12-31");
    assertThat(result).isEqualTo(LocalDate.of(2023, 12, 31));
  }

  @Test
  void testParseLocalDate_nullInput() {
    assertThatThrownBy(() -> DateTimeUtil.parseLocalDate(null))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Date string cannot be null");
  }

  @Test
  void testParseLocalDate_emptyInput() {
    assertThatThrownBy(() -> DateTimeUtil.parseLocalDate(""))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Date string cannot be empty");
  }

  @Test
  void testParseLocalDate_invalidFormat() {
    assertThatThrownBy(() -> DateTimeUtil.parseLocalDate("31/12/2023"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Date '31/12/2023' must be in ISO format");
  }

  @Test
  void testParseLocalTimeToMicros_validInput() {
    LocalTime result = DateTimeUtil.parseLocalTimeToMicros("14:30:45.123456");
    assertThat(result).isEqualTo(LocalTime.of(14, 30, 45, 123456000));
  }

  @Test
  void testParseLocalTimeToMicros_simplifiedInput() {
    LocalTime result = DateTimeUtil.parseLocalTimeToMicros("14:30:45");
    assertThat(result).isEqualTo(LocalTime.of(14, 30, 45));
  }

  @Test
  void testParseLocalTimeToMicros_truncatesNanoseconds() {
    LocalTime result = DateTimeUtil.parseLocalTimeToMicros("14:30:45.1234567");
    assertThat(result).isEqualTo(LocalTime.of(14, 30, 45, 123456000));
  }

  @Test
  void testParseLocalTimeToMicros_nullInput() {
    assertThatThrownBy(() -> DateTimeUtil.parseLocalTimeToMicros(null))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Time string cannot be null");
  }

  @Test
  void testParseLocalTimeToMicros_emptyInput() {
    assertThatThrownBy(() -> DateTimeUtil.parseLocalTimeToMicros(""))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Time string cannot be empty");
  }

  @Test
  void testParseLocalTimeToMicros_invalidFormat() {
    assertThatThrownBy(() -> DateTimeUtil.parseLocalTimeToMicros("2:30pm"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Time '2:30pm' must be in ISO format");
  }

  @Test
  void testParseLocalDateTimeToMicros_validInput() {
    LocalDateTime result = DateTimeUtil.parseLocalDateTimeToMicros("2023-12-31T14:30:45.123456");
    assertThat(result).isEqualTo(LocalDateTime.of(2023, 12, 31, 14, 30, 45, 123456000));
  }

  @Test
  void testParseLocalDateTimeToMicros_simplifiedInput() {
    LocalDateTime result = DateTimeUtil.parseLocalDateTimeToMicros("2023-12-31T14:30:45");
    assertThat(result).isEqualTo(LocalDateTime.of(2023, 12, 31, 14, 30, 45));
  }

  @Test
  void testParseLocalDateTimeToMicros_truncatesNanoseconds() {
    LocalDateTime result = DateTimeUtil.parseLocalDateTimeToMicros("2023-12-31T14:30:45.1234567");
    assertThat(result).isEqualTo(LocalDateTime.of(2023, 12, 31, 14, 30, 45, 123456000));
  }

  @Test
  void testParseLocalDateTimeToMicros_nullInput() {
    assertThatThrownBy(() -> DateTimeUtil.parseLocalDateTimeToMicros(null))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Timestamp string cannot be null");
  }

  @Test
  void testParseLocalDateTimeToMicros_emptyInput() {
    assertThatThrownBy(() -> DateTimeUtil.parseLocalDateTimeToMicros(""))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Timestamp string cannot be empty");
  }

  @Test
  void testParseLocalDateTimeToMicros_invalidFormat() {
    assertThatThrownBy(() -> DateTimeUtil.parseLocalDateTimeToMicros("12/31/2023 14:30:45"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Timestamp '12/31/2023 14:30:45' must be in ISO format");
  }

  @Test
  void testParseOffsetDateTimeToMicros_validInput() {
    OffsetDateTime result =
        DateTimeUtil.parseOffsetDateTimeToMicros("2023-12-31T14:30:45.123456+01:00");
    assertThat(result)
        .isEqualTo(OffsetDateTime.of(2023, 12, 31, 14, 30, 45, 123456000, ZoneOffset.ofHours(1)));
  }

  @Test
  void testParseOffsetDateTimeToMicros_zulu() {
    OffsetDateTime result = DateTimeUtil.parseOffsetDateTimeToMicros("2023-12-31T14:30:45.123456Z");
    assertThat(result)
        .isEqualTo(OffsetDateTime.of(2023, 12, 31, 14, 30, 45, 123456000, ZoneOffset.UTC));
  }

  @Test
  void testParseOffsetDateTimeToMicros_simplifiedInput() {
    OffsetDateTime result = DateTimeUtil.parseOffsetDateTimeToMicros("2023-12-31T14:30:45+01:00");
    assertThat(result)
        .isEqualTo(OffsetDateTime.of(2023, 12, 31, 14, 30, 45, 0, ZoneOffset.ofHours(1)));
  }

  @Test
  void testParseOffsetDateTimeToMicros_truncatesNanoseconds() {
    OffsetDateTime result =
        DateTimeUtil.parseOffsetDateTimeToMicros("2023-12-31T14:30:45.1234567+01:00");
    assertThat(result)
        .isEqualTo(OffsetDateTime.of(2023, 12, 31, 14, 30, 45, 123456000, ZoneOffset.ofHours(1)));
  }

  @Test
  void testParseOffsetDateTimeToMicros_nullInput() {
    assertThatThrownBy(() -> DateTimeUtil.parseOffsetDateTimeToMicros(null))
        .isInstanceOf(ValidationException.class)
        .hasMessage("TimestampTZ string cannot be null");
  }

  @Test
  void testParseOffsetDateTimeToMicros_emptyInput() {
    assertThatThrownBy(() -> DateTimeUtil.parseOffsetDateTimeToMicros(""))
        .isInstanceOf(ValidationException.class)
        .hasMessage("TimestampTZ string cannot be empty");
  }

  @Test
  void testParseOffsetDateTimeToMicros_invalidFormat() {
    assertThatThrownBy(() -> DateTimeUtil.parseOffsetDateTimeToMicros("12/31/2023 14:30:45 +01:00"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("TimestampTZ '12/31/2023 14:30:45 +01:00' must be in ISO format");
  }

  @Test
  void testFormatLocalDate_validInput() {
    LocalDate date = LocalDate.of(2023, 12, 31);
    String result = DateTimeUtil.formatLocalDate(date);
    assertThat(result).isEqualTo("2023-12-31");
  }

  @Test
  void testFormatLocalDate_nullInput() {
    assertThatThrownBy(() -> DateTimeUtil.formatLocalDate(null))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Date value cannot be null");
  }

  @Test
  void testFormatLocalTimeWithMicros_validInput() {
    LocalTime time = LocalTime.of(14, 30, 45, 123456000);
    String result = DateTimeUtil.formatLocalTimeWithMicros(time);
    assertThat(result).isEqualTo("14:30:45.123456");
  }

  @Test
  void testFormatLocalTimeWithMicros_truncatesNanoseconds() {
    LocalTime time = LocalTime.of(14, 30, 45, 123456789);
    String result = DateTimeUtil.formatLocalTimeWithMicros(time);
    assertThat(result).isEqualTo("14:30:45.123456");
  }

  @Test
  void testFormatLocalTimeWithMicros_noFractionalSeconds() {
    LocalTime time = LocalTime.of(14, 30, 45);
    String result = DateTimeUtil.formatLocalTimeWithMicros(time);
    assertThat(result).isEqualTo("14:30:45");
  }

  @Test
  void testFormatLocalTimeWithMicros_nullInput() {
    assertThatThrownBy(() -> DateTimeUtil.formatLocalTimeWithMicros(null))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Time value cannot be null");
  }

  @Test
  void testFormatLocalDateTimeWithMicros_validInput() {
    LocalDateTime dateTime = LocalDateTime.of(2023, 12, 31, 14, 30, 45, 123456000);
    String result = DateTimeUtil.formatLocalDateTimeWithMicros(dateTime);
    assertThat(result).isEqualTo("2023-12-31T14:30:45.123456");
  }

  @Test
  void testFormatLocalDateTimeWithMicros_truncatesNanoseconds() {
    LocalDateTime dateTime = LocalDateTime.of(2023, 12, 31, 14, 30, 45, 123456789);
    String result = DateTimeUtil.formatLocalDateTimeWithMicros(dateTime);
    assertThat(result).isEqualTo("2023-12-31T14:30:45.123456");
  }

  @Test
  void testFormatLocalDateTimeWithMicros_noFractionalSeconds() {
    LocalDateTime dateTime = LocalDateTime.of(2023, 12, 31, 14, 30, 45);
    String result = DateTimeUtil.formatLocalDateTimeWithMicros(dateTime);
    assertThat(result).isEqualTo("2023-12-31T14:30:45");
  }

  @Test
  void testFormatLocalDateTimeWithMicros_nullInput() {
    assertThatThrownBy(() -> DateTimeUtil.formatLocalDateTimeWithMicros(null))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Timestamp value cannot be null");
  }

  @Test
  void testFormatOffsetDateTimeWithMicros_validInput() {
    OffsetDateTime dateTime =
        OffsetDateTime.of(2023, 12, 31, 14, 30, 45, 123456000, ZoneOffset.ofHours(1));
    String result = DateTimeUtil.formatOffsetDateTimeWithMicros(dateTime);
    assertThat(result).isEqualTo("2023-12-31T14:30:45.123456+01:00");
  }

  @Test
  void testFormatOffsetDateTimeWithMicros_utcZone() {
    OffsetDateTime dateTime =
        OffsetDateTime.of(2023, 12, 31, 14, 30, 45, 123456789, ZoneOffset.UTC);
    String result = DateTimeUtil.formatOffsetDateTimeWithMicros(dateTime);
    assertThat(result).isEqualTo("2023-12-31T14:30:45.123456Z");
  }

  @Test
  void testFormatOffsetDateTimeWithMicros_truncatesNanoseconds() {
    OffsetDateTime dateTime =
        OffsetDateTime.of(2023, 12, 31, 14, 30, 45, 123456789, ZoneOffset.ofHours(1));
    String result = DateTimeUtil.formatOffsetDateTimeWithMicros(dateTime);
    assertThat(result).isEqualTo("2023-12-31T14:30:45.123456+01:00");
  }

  @Test
  void testFormatOffsetDateTimeWithMicros_noFractionalSeconds() {
    OffsetDateTime dateTime = OffsetDateTime.of(2023, 12, 31, 14, 30, 45, 0, ZoneOffset.ofHours(1));
    String result = DateTimeUtil.formatOffsetDateTimeWithMicros(dateTime);
    assertThat(result).isEqualTo("2023-12-31T14:30:45+01:00");
  }

  @Test
  void testFormatOffsetDateTimeWithMicros_nullInput() {
    assertThatThrownBy(() -> DateTimeUtil.formatOffsetDateTimeWithMicros(null))
        .isInstanceOf(ValidationException.class)
        .hasMessage("TimestampTZ value cannot be null");
  }
}
