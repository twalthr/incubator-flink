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

package org.apache.flink.table.expressions;

import java.util.List;
import java.util.stream.Stream;

public class ApiExpression extends BaseExpressionOperations<ApiExpression> implements Expression {

	private Expression wrappedExpression;

	public ApiExpression(Expression wrappedExpression) {
		this.wrappedExpression = wrappedExpression;
	}

	@Override
	public Expression toExpr() {
		return wrappedExpression;
	}

	@Override
	public List<Expression> getChildren() {
		return wrappedExpression.getChildren();
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		final UnwrapVisitor<R> unwrapVisitor = new UnwrapVisitor<>(visitor);
		return wrappedExpression.accept(unwrapVisitor);
	}

	private static class UnwrapVisitor<R> implements ExpressionVisitor<R> {

		private ExpressionVisitor<R> visitor;

		public UnwrapVisitor(ExpressionVisitor<R> visitor) {
			this.visitor = visitor;
		}

		@Override
		public R visitCall(CallExpression call) {
			return visitor.visitCall(call);
		}

		@Override
		public R visitSymbol(SymbolExpression symbol) {
			return visitor.visitSymbol(symbol);
		}

		@Override
		public R visitValueLiteral(ValueLiteralExpression valueLiteral) {
			return visitor.visitValueLiteral(valueLiteral);
		}

		@Override
		public R visitFieldReference(FieldReferenceExpression fieldReference) {
			return visitor.visitFieldReference(fieldReference);
		}

		@Override
		public R visitTypeLiteral(TypeLiteralExpression typeLiteral) {
			return visitor.visitTypeLiteral(typeLiteral);
		}

		@Override
		public R visit(Expression other) {
			return visitor.visit(other);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Additional API operations
	// --------------------------------------------------------------------------------------------

	public ApiExpression as(String... names) {
		return ApiExpressionUtils.call(
			BuiltInFunctionDefinitions.AS,
			Stream.concat(
				Stream.of(toExpr()),
				Stream.of(names).map(ApiExpressionUtils::valueLiteral)
			).toArray(Expression[]::new));
	}
}
