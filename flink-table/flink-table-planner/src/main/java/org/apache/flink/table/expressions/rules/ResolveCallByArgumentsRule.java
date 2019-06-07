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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.InputTypeSpec;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UntypedCallExpression;
import org.apache.flink.table.typeutils.TypeCoercion;
import org.apache.flink.table.validate.ValidationFailure;
import org.apache.flink.table.validate.ValidationResult;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.apache.flink.table.expressions.ApiExpressionUtils.typeLiteral;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;
import static org.apache.flink.table.util.JavaScalaConversionUtil.toJava;

/**
 * It checks if a {@link CallExpression} can work with given arguments.
 * If the call expects different types of arguments, but the given arguments
 * have types that can be casted, a {@link BuiltInFunctionDefinitions#CAST}
 * expression is inserted.
 */
@Internal
final class ResolveCallByArgumentsRule implements ResolverRule {

	@Override
	public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
		return expression.stream()
			.map(expr -> expr.accept(new CallArgumentsCastingVisitor(context)))
			.collect(Collectors.toList());
	}

	private class CallArgumentsCastingVisitor extends RuleExpressionVisitor<ResolvedExpression> {

		CallArgumentsCastingVisitor(ResolutionContext context) {
			super(context);
		}

		@Override
		public ResolvedExpression visitUntypedCall(UntypedCallExpression untypedCall) {
			final PlannerExpression plannerCall = resolutionContext.bridge(untypedCall);

			final List<PlannerExpression> args = untypedCall.getChildren()
				.stream()
				.map(resolutionContext::bridge)
				.collect(Collectors.toList());

			if (plannerCall instanceof InputTypeSpec) {
				final List<TypeInformation<?>> expectedInputTypes =
					toJava(((InputTypeSpec) plannerCall).expectedTypes());

				final List<ResolvedExpression> newArgs = IntStream.range(0, args.size())
					.mapToObj(idx -> castIfNeeded(args.get(idx), expectedInputTypes.get(idx)))
					.collect(Collectors.toList());

				return untypedCall.resolve(
					newArgs,
					fromLegacyInfoToDataType(plannerCall.resultType()));
			} else {
				validateArguments(untypedCall, plannerCall);
				return untypedCall.resolve(
					new ArrayList<>(args),
					fromLegacyInfoToDataType(plannerCall.resultType()));
			}
		}

		private void validateArguments(UntypedCallExpression untypedCall, PlannerExpression plannerCall) {
			if (!plannerCall.valid()) {
				final String errorMessage;
				ValidationResult validationResult = plannerCall.validateInput();
				if (validationResult instanceof ValidationFailure) {
					errorMessage = ((ValidationFailure) validationResult).message();
				} else {
					errorMessage = String.format("Invalid arguments %s for function: %s",
						untypedCall.getChildren(),
						untypedCall.getFunctionDefinition());
				}
				throw new ValidationException(errorMessage);
			}
		}

		private ResolvedExpression castIfNeeded(PlannerExpression childExpression, TypeInformation<?> expectedType) {
			TypeInformation<?> actualType = childExpression.resultType();
			if (actualType.equals(expectedType)) {
				return childExpression;
			} else if (TypeCoercion.canSafelyCast(actualType, expectedType)) {
				return new CallExpression(
					ObjectIdentifier.of(BuiltInFunctionDefinitions.CAST.getName()),
					BuiltInFunctionDefinitions.CAST,
					asList(
						childExpression,
						typeLiteral(fromLegacyInfoToDataType(expectedType))),
					fromLegacyInfoToDataType(expectedType)
				);
			} else {
				throw new ValidationException(String.format("Incompatible type of argument: %s Expected: %s",
					childExpression,
					expectedType));
			}
		}

		@Override
		protected ResolvedExpression defaultMethod(Expression expression) {
			if (expression instanceof ResolvedExpression) {
				return (ResolvedExpression) expression;
			}
			throw new TableException("Unexpected unresolved expression: " + expression);
		}
	}
}
