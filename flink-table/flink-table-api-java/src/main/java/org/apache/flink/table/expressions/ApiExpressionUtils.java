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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.CAST;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.TIMES;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * Utilities for API-specific {@link Expression}s.
 */
@Internal
public final class ApiExpressionUtils {

	public static final long MILLIS_PER_SECOND = 1000L;

	public static final long MILLIS_PER_MINUTE = 60000L;

	public static final long MILLIS_PER_HOUR = 3600000L; // = 60 * 60 * 1000

	public static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

	private ApiExpressionUtils() {
		// private
	}

	public static CallExpression call(FunctionDefinition functionDefinition, Expression... args) {
		return new CallExpression(functionDefinition, Arrays.asList(args));
	}

	public static ValueLiteralExpression valueLiteral(Object value) {
		if (value instanceof BigDecimal) {
			// decimal value literals need special treatment as their semantics differ from SQL;
			// once we remove the legacy types, we need to read precision and scale from a decimal value and
			// set the those for every literal accordingly; we decided against default precision and scale
			// that would change the value itself
			return valueLiteral(value, fromLegacyInfoToDataType(Types.BIG_DEC));
		} else if (value instanceof java.sql.Time) {
			// once we remove the legacy types, we need to read precision from a value and
			// set it for every literal accordingly
			return valueLiteral(value, fromLegacyInfoToDataType(Types.SQL_TIME));
		} else if (value instanceof java.sql.Timestamp) {
			// once we remove the legacy types, we need to read precision from a value and
			// set it for every literal accordingly
			return valueLiteral(value, fromLegacyInfoToDataType(Types.SQL_TIMESTAMP));
		}
		return new ValueLiteralExpression(value);
	}

	public static ValueLiteralExpression valueLiteral(Object value, DataType dataType) {
		return new ValueLiteralExpression(value, dataType);
	}

	public static TypeLiteralExpression typeLiteral(DataType dataType) {
		return new TypeLiteralExpression(dataType);
	}

	public static SymbolExpression symbol(TableSymbol symbol) {
		return new SymbolExpression(symbol);
	}

	public static UnresolvedReferenceExpression unresolvedRef(String name) {
		return new UnresolvedReferenceExpression(name);
	}

	public static TableReferenceExpression tableRef(String name, Table table) {
		return new TableReferenceExpression(name, table.getTableOperation());
	}

	public static LookupCallExpression lookupCall(String name, Expression... args) {
		return new LookupCallExpression(name, Arrays.asList(args));
	}

	public static Expression toMonthInterval(Expression e, int multiplier) {
		// check for constant
		return ExpressionUtils.extractValue(e, Integer.class)
			.map((v) -> (Expression) valueLiteral(
				v * multiplier,
				DataTypes.INTERVAL(DataTypes.MONTH()).bridgedTo(Integer.class)))
			.orElse(
				call(
					CAST,
					call(
						TIMES,
						e,
						valueLiteral(multiplier)
					),
					typeLiteral(DataTypes.INTERVAL(DataTypes.MONTH()))
				)
			);
	}

	public static Expression toMilliInterval(Expression e, long multiplier) {
		final Optional<Expression> intInterval = ExpressionUtils.extractValue(e, Integer.class)
			.map((v) -> valueLiteral(
				v * multiplier,
				DataTypes.INTERVAL(DataTypes.SECOND(3)).bridgedTo(Long.class)));

		final Optional<Expression> longInterval = ExpressionUtils.extractValue(e, Long.class)
			.map((v) -> valueLiteral(
				v * multiplier,
				DataTypes.INTERVAL(DataTypes.SECOND(3)).bridgedTo(Long.class)));

		if (intInterval.isPresent()) {
			return intInterval.get();
		} else if (longInterval.isPresent()) {
			return longInterval.get();
		}
		return call(
			CAST,
			call(
				TIMES,
				e,
				valueLiteral(multiplier)
			),
			typeLiteral(DataTypes.INTERVAL(DataTypes.SECOND(3)))
		);
	}

	public static Expression toRowInterval(Expression e) {
		final Optional<Expression> intInterval = ExpressionUtils.extractValue(e, Integer.class)
			.map((v) -> valueLiteral((long) v, DataTypes.BIGINT()));

		final Optional<Expression> longInterval = ExpressionUtils.extractValue(e, Long.class)
			.map((v) -> valueLiteral(v, DataTypes.BIGINT()));

		if (intInterval.isPresent()) {
			return intInterval.get();
		} else if (longInterval.isPresent()) {
			return longInterval.get();
		}
		throw new ValidationException("Invalid value for row interval literal: " + e);
	}
}
