/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.expressions.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.List;

import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.FLATTEN;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.OVER;

/**
 * Verifies that there is no more unresolved expressions. Checks for expression like:
 * <ul>
 *     <li>non-{@link ResolvedExpression}</li>
 *     <li>{@link org.apache.flink.table.expressions.BuiltInFunctionDefinitions#OVER} that still contains
 *     just alias to corresponding window</li>
 *     <li>{@link org.apache.flink.table.expressions.BuiltInFunctionDefinitions#FLATTEN}</li>
 * </ul>
 */
@Internal
final class VerifyNoUnresolvedExpressionsRule implements ResolverRule {

	private static final NoUnresolvedCallsChecker checker = new NoUnresolvedCallsChecker();

	@Override
	public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
		expression.forEach(expr -> expr.accept(checker));
		return expression;
	}

	private static class NoUnresolvedCallsChecker extends ApiExpressionDefaultVisitor<Void> {

		@Override
		public Void visitCall(CallExpression call) {
			if (call.getFunctionDefinition() == OVER && call.getChildren().size() <= 2) {
				throw getException("OVER call", call);
			} else if (call.getFunctionDefinition() == FLATTEN) {
				throw getException("FLATTEN call", call);
			}
			call.getChildren().forEach(expr -> expr.accept(this));

			return null;
		}

		@Override
		protected Void defaultMethod(Expression expression) {
			if (!(expression instanceof ResolvedExpression)) {
				throw getException(expression.getClass().getSimpleName(), expression);
			}

			return null;
		}

		private TableException getException(String expressionType, Expression expr) {
			return new TableException(String.format(
				"Unexpected unresolved %s: %s. All expressions should be resolved by now",
				expressionType, expr));
		}
	}
}
