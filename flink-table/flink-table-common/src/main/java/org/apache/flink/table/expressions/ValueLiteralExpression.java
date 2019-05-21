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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.types.utils.ClassDataTypeConverter.extractDataType;

/**
 * Expression for constant literal values.
 */
@PublicEvolving
public final class ValueLiteralExpression implements Expression {

	private final Object value;

	private final DataType dataType;

	public ValueLiteralExpression(Object value) {
		this(value, deriveDataTypeFromValue(value));
	}

	public ValueLiteralExpression(Object value, DataType dataType) {
		validateValueDataType(value, Preconditions.checkNotNull(dataType, "Data type must not be null."));
		this.value = value; // can be null
		this.dataType = dataType;
	}

	public Object getValue() {
		return value;
	}

	public DataType getDataType() {
		return dataType;
	}

	@Override
	public List<Expression> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visitValueLiteral(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ValueLiteralExpression that = (ValueLiteralExpression) o;
		return Objects.equals(value, that.value) && dataType.equals(that.dataType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value, dataType);
	}

	@Override
	public String toString() {
		return StringUtils.arrayAwareToString(value);
	}

	/**
	 * Returns the non-null literal value.
	 */
	public <T> Optional<T> getValueAs(Class<T> clazz) {
		if (clazz.isInstance(value)) {
			return Optional.of(clazz.cast(value));
		}
		// we could offer more conversions in the future
		return Optional.empty();
	}

	/**
	 * Returns the literal value including null.
	 */
	public <T> T getNullableValueAs(Class<T> clazz) {
		if (value == null || clazz.isInstance(value)) {
			return clazz.cast(value);
		}
		// we could offer more conversions in the future
		throw new TableException("Unsupported target class: " + clazz.getName());
	}

	// --------------------------------------------------------------------------------------------

	private static DataType deriveDataTypeFromValue(Object value) {
		if (value == null) {
			throw new ValidationException(
				"Cannot derive a data type from a null value. The type must be specified explicitly.");
		}
		return extractDataType(value.getClass())
			.orElseThrow(() ->
				new ValidationException("Cannot derive a data type for value '" + value + "'. " +
					"The data type must be specified explicitly."));
	}

	private static void validateValueDataType(Object value, DataType dataType) {
		final LogicalType logicalType = dataType.getLogicalType();
		if (value == null) {
			return;
		}
		final Class<?> candidate = value.getClass();
		if (!dataType.getConversionClass().equals(candidate)) {
			throw new ValidationException(
				String.format(
					"Data type '%s' with conversion class '%s' does not support a value literal of class '%s'.",
					dataType,
					dataType.getConversionClass().getName(),
					value.getClass().getName()));
		}
		if (!logicalType.supportsInputConversion(candidate)) {
			throw new ValidationException(
				String.format(
					"Data type '%s' does not support a conversion from class '%s'.",
					logicalType.asSummaryString(),
					candidate.getName()));
		}
	}
}
