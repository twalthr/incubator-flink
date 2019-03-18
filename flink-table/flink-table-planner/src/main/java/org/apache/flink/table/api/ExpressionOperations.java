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

package org.apache.flink.table.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.expressions.AggregateFunctionDefinition;
import org.apache.flink.table.expressions.ApiExpression;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ScalarFunctionDefinition;
import org.apache.flink.table.expressions.TableFunctionDefinition;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.AND;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.OR;

public final class ExpressionOperations {

	public static ApiExpression field(String name) {
		return ApiExpressionUtils.fieldRef(name);
	}

	public static ApiExpression value(long v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	public static ApiExpression value(byte v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	public static ApiExpression value(short v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	public static ApiExpression value(int v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	public static ApiExpression value(float v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	public static ApiExpression value(double v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	public static ApiExpression value(String v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	public static ApiExpression value(boolean v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	public static ApiExpression value(BigDecimal v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	public static ApiExpression value(Date v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	public static ApiExpression value(Time v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	public static ApiExpression value(Timestamp v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	/**
	 * Boolean AND in three-valued logic.
	 */
	public static ApiExpression and(Expression predicate0, Expression predicate1, Expression... predicates) {
		ApiExpression call = ApiExpressionUtils.call(AND, predicate0, predicate1);
		for (Expression predicate: predicates) {
			call = and(call, predicate);
		}
		return call;
	}

	/**
	 * Boolean OR in three-valued logic.
	 */
	public static ApiExpression or(Expression predicate0, Expression predicate1, Expression... predicates) {
		ApiExpression call = ApiExpressionUtils.call(OR, predicate0, predicate1);
		for (Expression predicate: predicates) {
			call = and(call, predicate);
		}
		return call;
	}

	public static ApiExpression call(String functionName, Expression... params) {
		return ApiExpressionUtils.unresolvedCall(functionName, params);
	}

	public static ApiExpression call(ScalarFunction scalarFunction, Expression... params) {
		return ApiExpressionUtils.call(
			new ScalarFunctionDefinition(scalarFunction.getClass().getName(), scalarFunction),
			params);
	}

	public static ApiExpression call(TableFunction<?> tableFunction, Expression... params) {
		final TypeInformation<?> resultType = TypeExtractor
			.createTypeInfo(tableFunction, TableFunction.class, tableFunction.getClass(), 0);
		return ApiExpressionUtils.call(
			new TableFunctionDefinition(tableFunction.getClass().getName(), tableFunction, resultType),
			params);
	}

	public static ApiExpression call(AggregateFunction<?, ?> aggregateFunction, Expression... params) {
		return ApiExpressionUtils.call(
			new AggregateFunctionDefinition(
				aggregateFunction.getClass().getName(),
				aggregateFunction,
				UserDefinedFunctionUtils.getResultTypeOfAggregateFunction(aggregateFunction, null),
				UserDefinedFunctionUtils.getAccumulatorTypeOfAggregateFunction(aggregateFunction, null)),
			params);
	}
}
