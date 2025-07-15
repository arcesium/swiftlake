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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.Test;

class ResolvedExpressionTest {

  @Test
  void testConstructorAndGetter() {
    org.apache.iceberg.expressions.Expression icebergExpr = Expressions.equal("id", 1);
    ResolvedExpression expression =
        com.arcesium.swiftlake.expressions.Expressions.resolved(icebergExpr);

    assertThat(expression.getExpression()).isSameAs(icebergExpr);
  }

  @Test
  void testToString() {
    ResolvedExpression expression =
        com.arcesium.swiftlake.expressions.Expressions.resolved(Expressions.equal("id", 1));
    assertThat(expression.toString()).isEqualTo("ref(name=\"id\") == 1");
  }

  @Test
  void testWithComplexExpression() {
    org.apache.iceberg.expressions.Expression icebergExpr =
        Expressions.and(
            Expressions.equal("id", 1),
            Expressions.or(Expressions.greaterThan("age", 18), Expressions.isNull("name")));
    ResolvedExpression expression =
        com.arcesium.swiftlake.expressions.Expressions.resolved(icebergExpr);

    assertThat(expression.getExpression()).isSameAs(icebergExpr);
    assertThat(expression.toString())
        .isEqualTo(
            "(ref(name=\"id\") == 1 and (ref(name=\"age\") > 18 or is_null(ref(name=\"name\"))))");
  }
}
