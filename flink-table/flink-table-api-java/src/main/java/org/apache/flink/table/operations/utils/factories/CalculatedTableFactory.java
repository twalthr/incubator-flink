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

package org.apache.flink.table.operations.utils.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.utils.ResolvedExpressionDefaultVisitor;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.operations.CalculatedQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.typeutils.FieldInfoUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.isFunction;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.isFunctionOfKind;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.functions.FunctionKind.TABLE;

/**
 * Utility class for creating a valid {@link CalculatedQueryOperation} operation.
 */
@Internal
public class CalculatedTableFactory {

	/**
	 * Creates a valid {@link CalculatedQueryOperation} operation.
	 *
	 * @param callExpr call to table function as expression
	 * @return valid calculated table
	 */
	public QueryOperation create(ResolvedExpression callExpr, String[] leftTableFieldNames) {
		FunctionTableCallVisitor calculatedTableCreator = new FunctionTableCallVisitor(leftTableFieldNames);
		return callExpr.accept(calculatedTableCreator);
	}

	private class FunctionTableCallVisitor extends ResolvedExpressionDefaultVisitor<CalculatedQueryOperation<?>> {

		private String[] leftTableFieldNames;

		public FunctionTableCallVisitor(String[] leftTableFieldNames) {
			this.leftTableFieldNames = leftTableFieldNames;
		}

		@Override
		public CalculatedQueryOperation<?> visit(CallExpression call) {
			if (isFunction(call, AS)) {
				return unwrapFromAlias(call);
			} else if (isFunctionOfKind(call, TABLE)) {
				return createFunctionCall(call, Collections.emptyList());
			} else {
				return defaultMethod(call);
			}
		}

		private CalculatedQueryOperation<?> unwrapFromAlias(CallExpression call) {
			List<Expression> children = call.getChildren();
			List<String> aliases = children.subList(1, children.size())
				.stream()
				.map(alias -> ExpressionUtils.extractValue(alias, String.class)
					.orElseThrow(() -> new ValidationException("Unexpected alias: " + alias)))
				.collect(toList());

			if (!isFunctionOfKind(children.get(0), TABLE)) {
				throw fail();
			}

			CallExpression tableCall = (CallExpression) children.get(0);
			return createFunctionCall(tableCall, aliases);
		}

		private CalculatedQueryOperation<?> createFunctionCall(CallExpression call, List<String> aliases) {

			final TableFunctionDefinition definition = (TableFunctionDefinition) call.getFunctionDefinition();

			TypeInformation<?> resultType = TypeConversions.fromDataTypeToLegacyInfo(call.getOutputDataType());

			int callArity = resultType.getTotalFields();
			int aliasesSize = aliases.size();

			String[] fieldNames;
			if (aliasesSize == 0) {
				fieldNames = FieldInfoUtils.getFieldNames(resultType, Arrays.asList(leftTableFieldNames));
			} else if (aliasesSize != callArity) {
				throw new ValidationException(String.format(
					"List of column aliases must have same degree as table; " +
						"the returned table of function '%s' has " +
						"%d columns, whereas alias list has %d columns",
					definition.toString(),
					callArity,
					aliasesSize));
			} else {
				fieldNames = aliases.toArray(new String[aliasesSize]);
			}

			TypeInformation<?>[] fieldTypes = FieldInfoUtils.getFieldTypes(resultType);

			return new CalculatedQueryOperation(
				definition.getTableFunction(),
				call.getResolvedChildren(),
				definition.getResultType(),
				new TableSchema(fieldNames, fieldTypes));
		}

		@Override
		protected CalculatedQueryOperation<?> defaultMethod(ResolvedExpression expression) {
			throw fail();
		}

		private ValidationException fail() {
			return new ValidationException(
				"A lateral join only accepts a string expression which defines a table function " +
					"call that might be followed by some alias.");
		}
	}
}
