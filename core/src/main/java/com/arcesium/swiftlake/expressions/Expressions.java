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

import com.arcesium.swiftlake.common.ValidationException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.util.NaNUtil;

/** Utility class for creating and manipulating expressions. */
public class Expressions {
  private static final Expression FALSE =
      new PredicateExpression(org.apache.iceberg.expressions.Expressions.alwaysFalse(), false);
  private static final Expression TRUE =
      new PredicateExpression(org.apache.iceberg.expressions.Expressions.alwaysTrue(), false);
  private static final Expression NULL = new PredicateExpression(null, true);

  /**
   * Creates an AND expression from two expressions.
   *
   * @param left The left expression.
   * @param right The right expression.
   * @return A new Expression representing the AND of the two input expressions.
   */
  public static Expression and(Expression left, Expression right) {
    ValidationException.checkNotNull(left, "Left expression cannot be null.");
    ValidationException.checkNotNull(right, "Right expression cannot be null.");
    ValidationException.check(
        !(left instanceof ResolvedExpression), "Left expression cannot be a ResolvedExpression.");
    ValidationException.check(
        !(right instanceof ResolvedExpression), "Right expression cannot be a ResolvedExpression.");
    return new AndExpression(left, right);
  }

  /**
   * Creates an AND expression from multiple expressions.
   *
   * @param left The left expression.
   * @param right The right expression.
   * @param expressions Additional expressions to AND.
   * @return A new Expression representing the AND of all input expressions.
   */
  public static Expression and(Expression left, Expression right, Expression... expressions) {
    return Stream.of(expressions).reduce(and(left, right), Expressions::and);
  }

  /**
   * Creates an OR expression from two expressions.
   *
   * @param left The left expression.
   * @param right The right expression.
   * @return A new Expression representing the OR of the two input expressions.
   */
  public static Expression or(Expression left, Expression right) {
    ValidationException.checkNotNull(left, "Left expression cannot be null.");
    ValidationException.checkNotNull(right, "Right expression cannot be null.");
    ValidationException.check(
        !(left instanceof ResolvedExpression), "Left expression cannot be a ResolvedExpression.");
    ValidationException.check(
        !(right instanceof ResolvedExpression), "Right expression cannot be a ResolvedExpression.");
    return new OrExpression(left, right);
  }

  /**
   * Creates a NOT expression from the given expression.
   *
   * @param child The expression to negate.
   * @return A new Expression representing the NOT of the input expression.
   */
  public static Expression not(Expression child) {
    ValidationException.checkNotNull(child, "Child expression cannot be null.");
    ValidationException.check(
        !(child instanceof ResolvedExpression), "Child expression cannot be a ResolvedExpression.");
    return new NotExpression(child);
  }

  /**
   * Creates an IS NULL expression for the given column.
   *
   * @param name The name of the column.
   * @return A new Expression representing IS NULL for the specified column.
   */
  public static Expression isNull(String name) {
    return isNull(ref(name));
  }

  /**
   * Creates an IS NULL expression for the given unbound term.
   *
   * @param <T> The type of the unbound term.
   * @param term The unbound term to check for null.
   * @return A new Expression representing IS NULL for the specified unbound term.
   */
  public static <T> Expression isNull(UnboundTerm<T> term) {
    return new PredicateExpression(org.apache.iceberg.expressions.Expressions.isNull(term), false);
  }

  /**
   * Creates a NOT NULL expression for the given column.
   *
   * @param name The name of the column.
   * @return A new Expression representing NOT NULL for the specified column.
   */
  public static Expression notNull(String name) {
    return new PredicateExpression(org.apache.iceberg.expressions.Expressions.notNull(name), false);
  }

  /**
   * Creates a LESS THAN expression for the given column and value.
   *
   * @param name The name of the column.
   * @param value The value to compare against.
   * @param <T> The type of the value.
   * @return A new Expression representing LESS THAN for the specified column and value.
   */
  public static <T> Expression lessThan(String name, T value) {
    if (value == null) {
      return new PredicateExpression(null, true);
    } else if (NaNUtil.isNaN(value)) {
      return notEqual(name, value);
    } else {
      return new PredicateExpression(
          org.apache.iceberg.expressions.Expressions.lessThan(name, convertValue(value)), false);
    }
  }

  /**
   * Creates a LESS THAN OR EQUAL TO expression for the given column and value.
   *
   * @param name The name of the column.
   * @param value The value to compare against.
   * @param <T> The type of the value.
   * @return A new Expression representing LESS THAN OR EQUAL TO for the specified column and value.
   */
  public static <T> Expression lessThanOrEqual(String name, T value) {
    if (value == null) {
      return new PredicateExpression(null, true);
    } else if (NaNUtil.isNaN(value)) {
      return TRUE;
    } else {
      return new PredicateExpression(
          org.apache.iceberg.expressions.Expressions.lessThanOrEqual(name, convertValue(value)),
          false);
    }
  }

  /**
   * Creates a GREATER THAN expression for the given column and value.
   *
   * @param name The name of the column.
   * @param value The value to compare against.
   * @param <T> The type of the value.
   * @return A new Expression representing GREATER THAN for the specified column and value.
   */
  public static <T> Expression greaterThan(String name, T value) {
    if (value == null) {
      return new PredicateExpression(null, true);
    } else if (NaNUtil.isNaN(value)) {
      return FALSE;
    } else {
      Expression expression =
          new PredicateExpression(
              org.apache.iceberg.expressions.Expressions.greaterThan(name, convertValue(value)),
              false);
      if (value instanceof Float || value instanceof Double) {
        return or(
            new PredicateExpression(org.apache.iceberg.expressions.Expressions.isNaN(name), false),
            expression);
      } else {
        return expression;
      }
    }
  }

  /**
   * Creates a GREATER THAN OR EQUAL TO expression for the given column and value.
   *
   * @param name The name of the column.
   * @param value The value to compare against.
   * @param <T> The type of the value.
   * @return A new Expression representing GREATER THAN OR EQUAL TO for the specified column and
   *     value.
   */
  public static <T> Expression greaterThanOrEqual(String name, T value) {
    if (value == null) {
      return new PredicateExpression(null, true);
    } else if (NaNUtil.isNaN(value)) {
      return equal(name, value);
    } else {
      Expression expression =
          new PredicateExpression(
              org.apache.iceberg.expressions.Expressions.greaterThanOrEqual(
                  name, convertValue(value)),
              false);
      if (value instanceof Float || value instanceof Double) {
        return or(
            new PredicateExpression(org.apache.iceberg.expressions.Expressions.isNaN(name), false),
            expression);
      } else {
        return expression;
      }
    }
  }

  /**
   * Creates an EQUAL TO expression for the given column and value.
   *
   * @param name The name of the column.
   * @param value The value to compare against.
   * @param <T> The type of the value.
   * @return A new Expression representing EQUAL TO for the specified column and value.
   */
  public static <T> Expression equal(String name, T value) {
    return equal(ref(name), value);
  }

  /**
   * Creates an EQUAL TO expression for the given unbound term and value.
   *
   * @param term The unbound term representing the expression to compare.
   * @param value The value to compare against.
   * @param <T> The type of the value.
   * @return A new Expression representing EQUAL TO for the specified term and value
   */
  public static <T> Expression equal(UnboundTerm<Object> term, T value) {
    if (value == null) {
      return new PredicateExpression(null, true);
    } else if (NaNUtil.isNaN(value)) {
      return new PredicateExpression(org.apache.iceberg.expressions.Expressions.isNaN(term), false);
    } else {
      return new PredicateExpression(
          org.apache.iceberg.expressions.Expressions.equal(term, convertValue(value)), false);
    }
  }

  /**
   * Creates a NOT EQUAL TO expression for the given column and value.
   *
   * @param name The name of the column.
   * @param value The value to compare against.
   * @param <T> The type of the value.
   * @return A new Expression representing NOT EQUAL TO for the specified column and value.
   */
  public static <T> Expression notEqual(String name, T value) {
    if (value == null) {
      return new PredicateExpression(null, true);
    } else if (NaNUtil.isNaN(value)) {
      return new PredicateExpression(
          org.apache.iceberg.expressions.Expressions.notNaN(name), false);
    } else {
      return new PredicateExpression(
          org.apache.iceberg.expressions.Expressions.notEqual(name, convertValue(value)), false);
    }
  }

  /**
   * Creates a STARTS WITH expression for the given column and value.
   *
   * @param name The name of the column.
   * @param value The value to compare against.
   * @return A new Expression representing STARTS WITH for the specified column and value.
   */
  public static Expression startsWith(String name, String value) {
    if (value == null) {
      return new PredicateExpression(null, true);
    } else {
      return new PredicateExpression(
          org.apache.iceberg.expressions.Expressions.startsWith(name, value), false);
    }
  }

  /**
   * Creates a NOT STARTS WITH expression for the given column and value.
   *
   * @param name The name of the column.
   * @param value The value to compare against.
   * @return A new Expression representing NOT STARTS WITH for the specified column and value.
   */
  public static Expression notStartsWith(String name, String value) {
    if (value == null) {
      return new PredicateExpression(null, true);
    } else {
      return new PredicateExpression(
          org.apache.iceberg.expressions.Expressions.notStartsWith(name, value), false);
    }
  }

  /**
   * Creates an IN expression for the given column and set of values.
   *
   * @param name The name of the column.
   * @param values The set of values to compare against.
   * @param <T> The type of the values.
   * @return A new Expression representing IN for the specified column and values.
   */
  public static <T> Expression in(String name, T... values) {
    ValidationException.checkNotNull(values, "Cannot create expression from null");
    return in(name, Arrays.asList(values));
  }

  /**
   * Creates an IN expression for the given column and set of values.
   *
   * @param name The name of the column.
   * @param values The set of values to compare against.
   * @param <T> The type of the values.
   * @return A new Expression representing IN for the specified column and values.
   */
  public static <T> Expression in(String name, Iterable<T> values) {
    ValidationException.checkNotNull(values, "Cannot create expression from null");
    boolean hasNullValue = false;
    T nanValue = null;
    List<T> nonNullAndNaNValues = new ArrayList<>();
    for (T value : values) {
      if (value == null) {
        hasNullValue = true;
      } else if (NaNUtil.isNaN(value)) {
        nanValue = value;
      } else {
        nonNullAndNaNValues.add(value);
      }
    }
    Expression expression =
        new PredicateExpression(
            org.apache.iceberg.expressions.Expressions.in(name, convertValues(nonNullAndNaNValues)),
            hasNullValue);
    if (nanValue != null) {
      return or(equal(name, nanValue), expression);
    } else {
      return expression;
    }
  }

  /**
   * Creates a NOT IN expression for the given column and set of values.
   *
   * @param name The name of the column.
   * @param values The set of values to compare against.
   * @param <T> The type of the values.
   * @return A new Expression representing NOT IN for the specified column and values.
   */
  public static <T> Expression notIn(String name, T... values) {
    ValidationException.checkNotNull(values, "Cannot create expression from null");
    return notIn(name, Arrays.asList(values));
  }

  /**
   * Creates a NOT IN expression for the given column and set of values.
   *
   * @param name The name of the column.
   * @param values The set of values to compare against.
   * @param <T> The type of the values.
   * @return A new Expression representing NOT IN for the specified column and values.
   */
  public static <T> Expression notIn(String name, Iterable<T> values) {
    ValidationException.checkNotNull(values, "Cannot create expression from null");
    boolean hasNullValue = false;
    T nanValue = null;
    List<T> nonNullAndNaNValues = new ArrayList<>();
    for (T value : values) {
      if (value == null) {
        hasNullValue = true;
      } else if (NaNUtil.isNaN(value)) {
        nanValue = value;
      } else {
        nonNullAndNaNValues.add(value);
      }
    }
    Expression expression =
        new PredicateExpression(
            org.apache.iceberg.expressions.Expressions.notIn(
                name, convertValues(nonNullAndNaNValues)),
            hasNullValue);
    if (nanValue != null) {
      return and(notEqual(name, nanValue), expression);
    } else {
      return expression;
    }
  }

  /**
   * Creates an ALWAYS TRUE expression.
   *
   * @return A new Expression representing ALWAYS TRUE.
   */
  public static Expression alwaysTrue() {
    return TRUE;
  }

  /**
   * Creates an ALWAYS FALSE expression.
   *
   * @return A new Expression representing ALWAYS FALSE.
   */
  public static Expression alwaysFalse() {
    return FALSE;
  }

  /**
   * Converts a Boolean value to its equivalent expression. Returns NULL for null value, ALWAYS TRUE
   * for true, and ALWAYS FALSE for false.
   *
   * @param value The Boolean value to convert
   * @return The corresponding expression
   */
  public static Expression fromBoolean(Boolean value) {
    return value == null ? NULL : (value ? alwaysTrue() : alwaysFalse());
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> bucket(String name, int numBuckets) {
    return org.apache.iceberg.expressions.Expressions.bucket(name, numBuckets);
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> year(String name) {
    return org.apache.iceberg.expressions.Expressions.year(name);
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> month(String name) {
    return org.apache.iceberg.expressions.Expressions.month(name);
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> day(String name) {
    return org.apache.iceberg.expressions.Expressions.day(name);
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> hour(String name) {
    return org.apache.iceberg.expressions.Expressions.hour(name);
  }

  public static <T> UnboundTerm<T> truncate(String name, int width) {
    return org.apache.iceberg.expressions.Expressions.truncate(name, width);
  }

  private static <T> Iterable<Object> convertValues(Iterable<T> values) {
    return StreamSupport.stream(values.spliterator(), false)
        .map(v -> convertValue(v))
        .collect(Collectors.toList());
  }

  /**
   * Converts a value to the appropriate type for Iceberg expressions.
   *
   * @param value The value to convert.
   * @param <T> The type of the value.
   * @return The converted value.
   * @throws ValidationException if the value type is not supported.
   */
  private static <T> Object convertValue(T value) {
    ValidationException.checkNotNull(value, "Cannot create expression from null");
    ValidationException.check(!NaNUtil.isNaN(value), "Cannot create expression from NaN");

    if (value instanceof Boolean
        || value instanceof Integer
        || value instanceof Long
        || value instanceof Float
        || value instanceof Double
        || value instanceof CharSequence
        || value instanceof UUID
        || value instanceof byte[]
        || value instanceof ByteBuffer
        || value instanceof BigDecimal) {
      return value;
    } else if (value instanceof LocalDate localDate) {
      DateTimeFormatter dtf = DateTimeFormatter.ISO_LOCAL_DATE;
      return dtf.format(localDate);
    } else if (value instanceof java.sql.Date sqlDate) {
      DateTimeFormatter dtf = DateTimeFormatter.ISO_LOCAL_DATE;
      return dtf.format(sqlDate.toLocalDate());
    } else if (value instanceof LocalTime localTime) {
      DateTimeFormatter dtf = DateTimeFormatter.ISO_LOCAL_TIME;
      return dtf.format(localTime);
    } else if (value instanceof LocalDateTime localDateTime) {
      DateTimeFormatter dtf = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
      return dtf.format(localDateTime);
    } else if (value instanceof Timestamp timestamp) {
      LocalDateTime localDateTime = timestamp.toLocalDateTime();
      DateTimeFormatter dtf = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
      return dtf.format(localDateTime);
    } else if (value instanceof OffsetDateTime offsetDateTime) {
      DateTimeFormatter dtf = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
      return dtf.format(offsetDateTime);
    }

    throw new ValidationException(
        "Cannot create expression from %s: %s", value.getClass().getName(), value);
  }

  /**
   * Creates a ResolvedExpression from an Iceberg expression.
   *
   * @param expression The Iceberg expression to wrap
   * @return A new ResolvedExpression containing the given expression
   */
  public static ResolvedExpression resolved(org.apache.iceberg.expressions.Expression expression) {
    return new ResolvedExpression(expression);
  }

  /**
   * Resolves an Expression to its ResolvedExpression form, handling null values. If the expression
   * is already resolved, returns it directly. Otherwise, resolves it with appropriate null
   * handling.
   *
   * @param expression The expression to resolve
   * @return The resolved expression
   */
  public static ResolvedExpression resolveExpression(Expression expression) {
    if (expression instanceof ResolvedExpression resolvedExpression) {
      return resolvedExpression;
    }
    return resolved(resolveWithNullHandling(expression, false));
  }

  /**
   * Recursively resolves an Expression to an Iceberg expression with proper null handling. Handles
   * AND, OR, NOT, and predicate expressions, applying appropriate transformations when negation is
   * needed.
   *
   * @param expression The expression to resolve
   * @param negate Whether to negate the expression during resolution
   * @return The resolved Iceberg expression
   * @throws ValidationException If the expression is invalid or unsupported
   */
  private static org.apache.iceberg.expressions.Expression resolveWithNullHandling(
      Expression expression, boolean negate) {
    if (expression instanceof AndExpression andExpr) {
      if (negate) {
        return org.apache.iceberg.expressions.Expressions.or(
            resolveWithNullHandling(andExpr.getLeft(), true),
            resolveWithNullHandling(andExpr.getRight(), true));
      } else {
        return org.apache.iceberg.expressions.Expressions.and(
            resolveWithNullHandling(andExpr.getLeft(), false),
            resolveWithNullHandling(andExpr.getRight(), false));
      }
    } else if (expression instanceof OrExpression orExpr) {
      if (negate) {
        return org.apache.iceberg.expressions.Expressions.and(
            resolveWithNullHandling(orExpr.getLeft(), true),
            resolveWithNullHandling(orExpr.getRight(), true));
      } else {
        return org.apache.iceberg.expressions.Expressions.or(
            resolveWithNullHandling(orExpr.getLeft(), false),
            resolveWithNullHandling(orExpr.getRight(), false));
      }
    } else if (expression instanceof NotExpression notExpr) {
      return resolveWithNullHandling(notExpr.getChild(), !negate);
    } else if (expression instanceof PredicateExpression predicateExpr) {
      org.apache.iceberg.expressions.Expression predicate = predicateExpr.getExpression();
      if (negate && predicate != null) {
        predicate = predicate.negate();
      }
      if (predicateExpr.hasNullLiteral()) {
        if (predicate == null
            || predicate.op() != org.apache.iceberg.expressions.Expression.Operation.IN) {
          predicate = org.apache.iceberg.expressions.Expressions.alwaysFalse();
        }
      }
      return predicate;
    } else {
      throw new ValidationException("Invalid Expression %s.", expression);
    }
  }

  /**
   * Creates a named reference to a column.
   *
   * @param name The name of the column to reference
   * @param <T> The type of the reference
   * @return A NamedReference for the specified column
   */
  public static <T> NamedReference<T> ref(String name) {
    return org.apache.iceberg.expressions.Expressions.ref(name);
  }
}
