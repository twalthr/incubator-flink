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

import org.apache.flink.table.expressions.ApiExpression;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;

import static org.apache.flink.table.expressions.ApiExpressionUtils.call;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.AND;

public final class ExpressionOperations {

	public static ApiExpression field(String name) {
		return ApiExpressionUtils.fieldRef(name);
	}

	public static ApiExpression value(int v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	public static ApiExpression value(String v) {
		return ApiExpressionUtils.valueLiteral(v);
	}

	public static ApiExpression and(Expression predicate0, Expression predicate1, Expression... predicates) {
		ApiExpression call = call(AND, predicate0, predicate1);
		for (Expression predicate: predicates) {
			call = and(call, predicate);
		}
		return call;
	}
}
