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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

/** Utility class for date and time operations */
public class DateTimeUtil {
  private DateTimeUtil() {}

  /**
   * Parses a string representation into a LocalDate using ISO_LOCAL_DATE format.
   *
   * @param value The string to parse (e.g., "2023-12-31")
   * @return The parsed LocalDate
   * @throws ValidationException if the string cannot be parsed as a valid ISO date
   */
  public static LocalDate parseLocalDate(String value) {
    ValidationException.checkNotNull(value, "Date string cannot be null");
    if (value.trim().isEmpty()) {
      throw new ValidationException("Date string cannot be empty");
    }

    try {
      return LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE);
    } catch (DateTimeParseException e) {
      throw new ValidationException(
          "Date '%s' must be in ISO format (yyyy-MM-dd, e.g., 2023-12-31)", value);
    }
  }

  /**
   * Parses a string representation into a LocalTime with microsecond precision. Any nanosecond
   * precision beyond microseconds will be truncated.
   *
   * @param value The string to parse (e.g., "14:30:45.123456")
   * @return The parsed LocalTime truncated to microsecond precision
   * @throws ValidationException if the string cannot be parsed as a valid ISO time
   */
  public static LocalTime parseLocalTimeToMicros(String value) {
    ValidationException.checkNotNull(value, "Time string cannot be null");
    if (value.trim().isEmpty()) {
      throw new ValidationException("Time string cannot be empty");
    }

    try {
      return LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME)
          .truncatedTo(ChronoUnit.MICROS);
    } catch (DateTimeParseException e) {
      throw new ValidationException(
          "Time '%s' must be in ISO format (HH:mm:ss[.SSSSSS], e.g., 14:30:45.123456)", value);
    }
  }

  /**
   * Parses a string representation into a LocalDateTime with microsecond precision. Any nanosecond
   * precision beyond microseconds will be truncated.
   *
   * @param value The string to parse (e.g., "2023-12-31T14:30:45.123456")
   * @return The parsed LocalDateTime truncated to microsecond precision
   * @throws ValidationException if the string cannot be parsed as a valid ISO date-time
   */
  public static LocalDateTime parseLocalDateTimeToMicros(String value) {
    ValidationException.checkNotNull(value, "Timestamp string cannot be null");
    if (value.trim().isEmpty()) {
      throw new ValidationException("Timestamp string cannot be empty");
    }

    try {
      return LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          .truncatedTo(ChronoUnit.MICROS);
    } catch (DateTimeParseException e) {
      throw new ValidationException(
          "Timestamp '%s' must be in ISO format (yyyy-MM-ddTHH:mm:ss[.SSSSSS], e.g., 2023-12-31T14:30:45.123456)",
          value);
    }
  }

  /**
   * Parses a string representation into an OffsetDateTime with microsecond precision. Any
   * nanosecond precision beyond microseconds will be truncated.
   *
   * @param value The string to parse (e.g., "2023-12-31T14:30:45.123456+01:00")
   * @return The parsed OffsetDateTime truncated to microsecond precision
   * @throws ValidationException if the string cannot be parsed as a valid ISO offset date-time
   */
  public static OffsetDateTime parseOffsetDateTimeToMicros(String value) {
    ValidationException.checkNotNull(value, "TimestampTZ string cannot be null");
    if (value.trim().isEmpty()) {
      throw new ValidationException("TimestampTZ string cannot be empty");
    }

    try {
      return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          .truncatedTo(ChronoUnit.MICROS);
    } catch (DateTimeParseException e) {
      throw new ValidationException(
          "TimestampTZ '%s' must be in ISO format (yyyy-MM-ddTHH:mm:ss[.SSSSSS]±HH:MM, e.g., 2023-12-31T14:30:45.123456+01:00)",
          value);
    }
  }

  /**
   * Formats a LocalDate to ISO date string.
   *
   * @param value The LocalDate to format
   * @return The formatted date string (e.g., "2023-12-31")
   */
  public static String formatLocalDate(LocalDate value) {
    ValidationException.checkNotNull(value, "Date value cannot be null");
    return DateTimeFormatter.ISO_LOCAL_DATE.format(value);
  }

  /**
   * Formats a LocalTime to ISO time string with microsecond precision.
   *
   * @param value The LocalTime to format
   * @return The formatted time string with microsecond precision (e.g., "14:30:45.123456")
   */
  public static String formatLocalTimeWithMicros(LocalTime value) {
    ValidationException.checkNotNull(value, "Time value cannot be null");
    return DateTimeFormatter.ISO_LOCAL_TIME.format(value.truncatedTo(ChronoUnit.MICROS));
  }

  /**
   * Formats a LocalDateTime to ISO date-time string with microsecond precision.
   *
   * @param value The LocalDateTime to format
   * @return The formatted date-time string with microsecond precision (e.g.,
   *     "2023-12-31T14:30:45.123456")
   */
  public static String formatLocalDateTimeWithMicros(LocalDateTime value) {
    ValidationException.checkNotNull(value, "Timestamp value cannot be null");
    return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(value.truncatedTo(ChronoUnit.MICROS));
  }

  /**
   * Formats an OffsetDateTime to ISO offset date-time string with microsecond precision.
   *
   * @param value The OffsetDateTime to format
   * @return The formatted offset date-time string with microsecond precision (e.g.,
   *     "2023-12-31T14:30:45.123456+01:00")
   */
  public static String formatOffsetDateTimeWithMicros(OffsetDateTime value) {
    ValidationException.checkNotNull(value, "TimestampTZ value cannot be null");
    return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(value.truncatedTo(ChronoUnit.MICROS));
  }
}
