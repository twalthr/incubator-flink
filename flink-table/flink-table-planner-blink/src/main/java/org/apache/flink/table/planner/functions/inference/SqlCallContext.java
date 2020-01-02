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

package org.apache.flink.table.planner.functions.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Period;
import java.util.AbstractList;
import java.util.List;
import java.util.Optional;

/**
 * A {@link CallContext} backed by {@link SqlCallBinding}.
 */
@Internal
public final class SqlCallContext implements CallContext {

	private final DataTypeLookup lookup;

	private final FunctionDefinition definition;

	private final SqlCallBinding binding;

	private final @Nullable DataType outputType;

	private final List<SqlNode> adaptedArguments;

	private final List<DataType> argumentDataTypes;

	public SqlCallContext(
			DataTypeLookup lookup,
			FunctionDefinition definition,
			SqlCallBinding binding,
			@Nullable RelDataType outputType) {
		this.lookup = lookup;
		this.definition = definition;
		this.binding = binding;
		this.outputType = convertOutputType(binding, outputType);
		this.adaptedArguments = binding.operands(); // reorders the operands
		this.argumentDataTypes = new AbstractList<DataType>() {
			@Override
			public DataType get(int pos) {
				final RelDataType relDataType = binding.getValidator().deriveType(
					binding.getScope(),
					adaptedArguments.get(pos));
				final LogicalType logicalType = FlinkTypeFactory.toLogicalType(relDataType);
				return TypeConversions.fromLogicalToDataType(logicalType);
			}

			@Override
			public int size() {
				return binding.getOperandCount();
			}
		};
	}

	@Override
	public DataTypeLookup getDataTypeLookup() {
		return lookup;
	}

	@Override
	public FunctionDefinition getFunctionDefinition() {
		return definition;
	}

	@Override
	public boolean isArgumentLiteral(int pos) {
		return SqlUtil.isLiteral(adaptedArguments.get(pos), false);
	}

	@Override
	public boolean isArgumentNull(int pos) {
		return SqlUtil.isNullLiteral(adaptedArguments.get(pos), false);
	}

	@Override
	public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
		try {
			return Optional.ofNullable(
				getLiteralValue(
					SqlLiteral.unchain(adaptedArguments.get(pos)),
					clazz));
		} catch (IllegalArgumentException e) {
			return Optional.empty();
		}
	}

	@Override
	public String getName() {
		return binding.getCall().getOperator().getNameAsId().toString();
	}

	@Override
	public List<DataType> getArgumentDataTypes() {
		return argumentDataTypes;
	}

	@Override
	public Optional<DataType> getOutputDataType() {
		return Optional.ofNullable(outputType);
	}

	// --------------------------------------------------------------------------------------------

	private static @Nullable DataType convertOutputType(SqlCallBinding binding, @Nullable RelDataType returnType) {
		if (returnType == null || returnType.equals(binding.getValidator().getUnknownType())) {
			return null;
		} else {
			final LogicalType logicalType = FlinkTypeFactory.toLogicalType(returnType);
			return TypeConversions.fromLogicalToDataType(logicalType);
		}
	}

	/**
	 * Bridges {@link SqlLiteral#getValueAs(Class)} and {@link ValueLiteralExpression#getValueAs(Class)}.
	 */
	@SuppressWarnings("unchecked")
	private static <T> T getLiteralValue(SqlLiteral literal, Class<T> clazz) {

		final Object value = literal.getValueAs(Object.class);
		final Class<?> valueClass = value.getClass();

		Object convertedValue = null;

		if (clazz == Duration.class) {
			final long longVal = literal.getValueAs(Long.class);
			convertedValue = Duration.ofMillis(longVal);
		}

		else if (clazz == Period.class) {
			final long longVal = literal.getValueAs(Long.class);
			if (longVal <= Integer.MAX_VALUE && longVal >= Integer.MIN_VALUE) {
				convertedValue = Period.ofMonths((int) longVal);
			}
		}

		else if (valueClass == SqlIntervalLiteral.IntervalValue.class && clazz == Integer.class) {
			final long longVal = literal.getValueAs(Long.class);
			if (longVal <= Integer.MAX_VALUE && longVal >= Integer.MIN_VALUE) {
				convertedValue = (int) longVal;
			}
		}

		else if (clazz == java.sql.Date.class) {
			final DateString dateString = literal.getValueAs(DateString.class);
			convertedValue = java.sql.Date.valueOf(dateString.toString());
		}

		else if (clazz == java.time.LocalDate.class) {
			final DateString dateString = literal.getValueAs(DateString.class);
			convertedValue = java.time.LocalDate.parse(dateString.toString());
		}

		else if (clazz == java.sql.Time.class) {
			final TimeString timeString = literal.getValueAs(TimeString.class);
			convertedValue = java.sql.Time.valueOf(timeString.toString());
		}

		else if (clazz == java.time.LocalTime.class) {
			final TimeString timeString = literal.getValueAs(TimeString.class);
			convertedValue = java.time.LocalTime.parse(timeString.toString());
		}

		else if (clazz == java.sql.Timestamp.class) {
			final TimestampString timestampString = literal.getValueAs(TimestampString.class);
			convertedValue = java.sql.Timestamp.valueOf(timestampString.toString());
		}

		else if (clazz == java.time.LocalDateTime.class) {
			final TimestampString timestampString = literal.getValueAs(TimestampString.class);
			convertedValue = java.time.LocalDateTime.parse(timestampString.toString().replace(' ', 'T'));
		}

		if (convertedValue != null) {
			return (T) convertedValue;
		}

		return literal.getValueAs(clazz);
	}
}
