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

/**
 * Represents a predicate expression that wraps an Iceberg expression. Tracks whether the predicate
 * contains null literals.
 */
public class PredicateExpression implements Expression {
  private final org.apache.iceberg.expressions.Expression expression;
  private final boolean hasNullLiteral;

  PredicateExpression(
      org.apache.iceberg.expressions.Expression expression, boolean hasNullLiteral) {
    this.expression = expression;
    this.hasNullLiteral = hasNullLiteral;
  }

  public org.apache.iceberg.expressions.Expression getExpression() {
    return expression;
  }

  public boolean hasNullLiteral() {
    return hasNullLiteral;
  }

  @Override
  public String toString() {
    return String.format("(predicate: %s, hasNullLiteral: %s)", expression, hasNullLiteral);
  }
}
