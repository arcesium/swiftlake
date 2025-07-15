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
package com.arcesium.swiftlake.expressions;

import static org.assertj.core.api.Assertions.*;

import com.arcesium.swiftlake.common.ValidationException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.*;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.UnboundTerm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ExpressionsTest {

  @Test
  void testAndWithTwoExpressions() {
    Expression expr1 = Expressions.equal("id", 1);
    Expression expr2 = Expressions.equal("name", "John");
    Expression result = Expressions.and(expr1, expr2);
    result = Expressions.resolveExpression(result);
    assertThat(result.toString())
        .isEqualTo("(ref(name=\"id\") == 1 and ref(name=\"name\") == \"John\")");
  }

  @Test
  void testAndWithMultipleExpressions() {
    Expression expr1 = Expressions.equal("id", 1);
    Expression expr2 = Expressions.equal("name", "John");
    Expression expr3 = Expressions.greaterThan("age", 18);
    Expression result = Expressions.and(expr1, expr2, expr3);
    result = Expressions.resolveExpression(result);
    assertThat(result.toString())
        .isEqualTo(
            "((ref(name=\"id\") == 1 and ref(name=\"name\") == \"John\") and ref(name=\"age\") > 18)");
  }

  @Test
  void testOr() {
    Expression expr1 = Expressions.equal("id", 1);
    Expression expr2 = Expressions.equal("name", "John");
    Expression result = Expressions.or(expr1, expr2);
    result = Expressions.resolveExpression(result);
    assertThat(result.toString())
        .isEqualTo("(ref(name=\"id\") == 1 or ref(name=\"name\") == \"John\")");
  }

  @Test
  void testNot() {
    Expression expr = Expressions.equal("id", 1);
    Expression result = Expressions.not(expr);
    result = Expressions.resolveExpression(result);
    assertThat(result.toString()).isEqualTo("ref(name=\"id\") != 1");
  }

  @Test
  void testIsNull() {
    Expression result = Expressions.isNull("name");
    result = Expressions.resolveExpression(result);
    assertThat(result.toString()).isEqualTo("is_null(ref(name=\"name\"))");
  }

  @Test
  void testNotNull() {
    Expression result = Expressions.notNull("name");
    result = Expressions.resolveExpression(result);
    assertThat(result.toString()).isEqualTo("not_null(ref(name=\"name\"))");
  }

  @ParameterizedTest
  @MethodSource("provideComparisonExpressions")
  void testComparisonExpressions(Expression expression, String expectedString) {
    expression = Expressions.resolveExpression(expression);
    assertThat(expression.toString()).isEqualTo(expectedString);
  }

  private static Stream<Arguments> provideComparisonExpressions() {
    return Stream.of(
        Arguments.of(Expressions.lessThan("age", 18), "ref(name=\"age\") < 18"),
        Arguments.of(Expressions.lessThanOrEqual("age", 18), "ref(name=\"age\") <= 18"),
        Arguments.of(Expressions.greaterThan("age", 18), "ref(name=\"age\") > 18"),
        Arguments.of(Expressions.greaterThanOrEqual("age", 18), "ref(name=\"age\") >= 18"),
        Arguments.of(Expressions.equal("name", "John"), "ref(name=\"name\") == \"John\""),
        Arguments.of(Expressions.notEqual("name", "John"), "ref(name=\"name\") != \"John\""));
  }

  @Test
  void testStartsWith() {
    Expression result = Expressions.startsWith("name", "Jo");
    result = Expressions.resolveExpression(result);
    assertThat(result.toString()).isEqualTo("ref(name=\"name\") startsWith \"\"Jo\"\"");
  }

  @Test
  void testNotStartsWith() {
    Expression result = Expressions.notStartsWith("name", "Jo");
    result = Expressions.resolveExpression(result);
    assertThat(result.toString()).isEqualTo("ref(name=\"name\") notStartsWith \"\"Jo\"\"");
  }

  @Test
  void testInWithVarargs() {
    Expression result = Expressions.in("id", 1, 2, 3);
    result = Expressions.resolveExpression(result);
    assertThat(result.toString()).isEqualTo("ref(name=\"id\") in (1, 2, 3)");
  }

  @Test
  void testInWithIterable() {
    Expression result = Expressions.in("id", Arrays.asList(1, 2, 3));
    result = Expressions.resolveExpression(result);
    assertThat(result.toString()).isEqualTo("ref(name=\"id\") in (1, 2, 3)");
  }

  @Test
  void testNotInWithVarargs() {
    Expression result = Expressions.notIn("id", 1, 2, 3);
    result = Expressions.resolveExpression(result);
    assertThat(result.toString()).isEqualTo("ref(name=\"id\") not in (1, 2, 3)");
  }

  @Test
  void testNotInWithIterable() {
    Expression result = Expressions.notIn("id", Arrays.asList(1, 2, 3));
    result = Expressions.resolveExpression(result);
    assertThat(result.toString()).isEqualTo("ref(name=\"id\") not in (1, 2, 3)");
  }

  @Test
  void testAlwaysTrue() {
    Expression result = Expressions.alwaysTrue();
    result = Expressions.resolveExpression(result);
    assertThat(result.toString()).isEqualTo("true");
  }

  @Test
  void testAlwaysFalse() {
    Expression result = Expressions.alwaysFalse();
    result = Expressions.resolveExpression(result);
    assertThat(result.toString()).isEqualTo("false");
  }

  @ParameterizedTest
  @MethodSource("provideTransformExpressions")
  void testTransformExpressions(UnboundTerm<?> term, String expectedString) {
    assertThat(term.toString()).isEqualTo(expectedString);
  }

  private static Stream<Arguments> provideTransformExpressions() {
    return Stream.of(
        Arguments.of(Expressions.bucket("id", 4), "bucket[4](ref(name=\"id\"))"),
        Arguments.of(Expressions.year("date"), "year(ref(name=\"date\"))"),
        Arguments.of(Expressions.month("date"), "month(ref(name=\"date\"))"),
        Arguments.of(Expressions.day("date"), "day(ref(name=\"date\"))"),
        Arguments.of(Expressions.hour("timestamp"), "hour(ref(name=\"timestamp\"))"),
        Arguments.of(Expressions.truncate("value", 2), "truncate[2](ref(name=\"value\"))"));
  }

  @ParameterizedTest
  @MethodSource("provideSupportedTypes")
  void testSupportedTypes(Object value, String expectedString) {
    Expression result = Expressions.equal("column", value);
    result = Expressions.resolveExpression(result);
    assertThat(result.toString()).isEqualTo(expectedString);
  }

  private static Stream<Arguments> provideSupportedTypes() {
    return Stream.of(
        Arguments.of(null, "false"),
        Arguments.of(true, "ref(name=\"column\") == true"),
        Arguments.of(1, "ref(name=\"column\") == 1"),
        Arguments.of(1L, "ref(name=\"column\") == 1"),
        Arguments.of(1.0f, "ref(name=\"column\") == 1.0"),
        Arguments.of(1.0f, "ref(name=\"column\") == 1.0"),
        Arguments.of(Float.NaN, "is_nan(ref(name=\"column\"))"),
        Arguments.of(Float.POSITIVE_INFINITY, "ref(name=\"column\") == Infinity"),
        Arguments.of(Float.NEGATIVE_INFINITY, "ref(name=\"column\") == -Infinity"),
        Arguments.of(1.0, "ref(name=\"column\") == 1.0"),
        Arguments.of(Double.NaN, "is_nan(ref(name=\"column\"))"),
        Arguments.of(Double.POSITIVE_INFINITY, "ref(name=\"column\") == Infinity"),
        Arguments.of(Double.NEGATIVE_INFINITY, "ref(name=\"column\") == -Infinity"),
        Arguments.of("test", "ref(name=\"column\") == \"test\""),
        Arguments.of(
            UUID.fromString("123e4567-e89b-12d3-a456-556642440000"),
            "ref(name=\"column\") == 123e4567-e89b-12d3-a456-556642440000"),
        Arguments.of(new byte[] {1, 2, 3}, "ref(name=\"column\") == X'010203'"),
        Arguments.of(ByteBuffer.wrap(new byte[] {1, 2, 3}), "ref(name=\"column\") == X'010203'"),
        Arguments.of(new BigDecimal("123.45"), "ref(name=\"column\") == 123.45"),
        Arguments.of(LocalDate.of(2023, 1, 1), "ref(name=\"column\") == \"2023-01-01\""),
        Arguments.of(java.sql.Date.valueOf("2023-01-01"), "ref(name=\"column\") == \"2023-01-01\""),
        Arguments.of(LocalTime.of(12, 0), "ref(name=\"column\") == \"12:00:00\""),
        Arguments.of(
            LocalDateTime.of(2023, 1, 1, 12, 0), "ref(name=\"column\") == \"2023-01-01T12:00:00\""),
        Arguments.of(
            Timestamp.valueOf("2023-01-01 12:00:00"),
            "ref(name=\"column\") == \"2023-01-01T12:00:00\""),
        Arguments.of(
            OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC),
            "ref(name=\"column\") == \"2023-01-01T12:00:00Z\""));
  }

  @Test
  void testUnsupportedType() {
    assertThatThrownBy(() -> Expressions.equal("column", new Object()))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot create expression from");
  }
}
