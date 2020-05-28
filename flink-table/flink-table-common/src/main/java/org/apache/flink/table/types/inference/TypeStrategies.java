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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.strategies.CommonTypeStrategy;
import org.apache.flink.table.types.inference.strategies.ExplicitTypeStrategy;
import org.apache.flink.table.types.inference.strategies.FirstTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MappingTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MatchFamilyTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MissingTypeStrategy;
import org.apache.flink.table.types.inference.strategies.UseArgumentTypeStrategy;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Strategies for inferring an output or accumulator data type of a function call.
 *
 * @see TypeStrategy
 */
@Internal
public final class TypeStrategies {

	/**
	 * Placeholder for a missing type strategy.
	 */
	public static final TypeStrategy MISSING = new MissingTypeStrategy();

	public static final TypeStrategy COMMON = new CommonTypeStrategy();

	/**
	 * Type strategy that returns a fixed {@link DataType}.
	 */
	public static TypeStrategy explicit(DataType dataType) {
		return new ExplicitTypeStrategy(dataType);
	}

	/**
	 * Type strategy that returns the n-th input argument.
	 */
	public static TypeStrategy argument(int pos) {
		return new UseArgumentTypeStrategy(pos);
	}

	/**
	 * Type strategy that returns the first type that could be inferred.
	 */
	public static TypeStrategy first(TypeStrategy... strategies) {
		return new FirstTypeStrategy(Arrays.asList(strategies));
	}

	/**
	 * Type strategy that returns the given argument if it is of the same logical type family.
	 */
	public static TypeStrategy matchFamily(int argumentPos, LogicalTypeFamily family) {
		return new MatchFamilyTypeStrategy(argumentPos, family);
	}

	/**
	 * Type strategy that maps an {@link InputTypeStrategy} to a {@link TypeStrategy} if the input strategy
	 * infers identical types.
	 */
	public static TypeStrategy mapping(Map<InputTypeStrategy, TypeStrategy> mappings) {
		return new MappingTypeStrategy(mappings);
	}

	/**
	 * Type strategy that returns a nullable type if any of the call's arguments are nullable.
	 */
	public static TypeStrategy nullable(TypeStrategy strategy) {
		Preconditions.checkNotNull(strategy);
		return callContext -> {
			final List<DataType> arguments = callContext.getArgumentDataTypes();
			final Optional<DataType> inferredDataType = strategy.inferType(callContext);
			if (arguments.stream().map(DataType::getLogicalType).anyMatch(LogicalType::isNullable)) {
				return inferredDataType.map(AbstractDataType::nullable);
			}
			return inferredDataType;
		};
	}

	// --------------------------------------------------------------------------------------------
	// Specific type strategies
	// --------------------------------------------------------------------------------------------

	/**
	 * Type strategy that returns a {@link DataTypes#ROW()} with fields types equal to input types.
	 */
	public static final TypeStrategy ROW = callContext -> {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		DataTypes.Field[] fields = IntStream.range(0, argumentDataTypes.size())
			.mapToObj(idx -> DataTypes.FIELD("f" + idx, argumentDataTypes.get(idx)))
			.toArray(DataTypes.Field[]::new);

		return Optional.of(DataTypes.ROW(fields).notNull());
	};

	/**
	 * Type strategy that returns a {@link DataTypes#MAP(DataType, DataType)} with a key type equal to type
	 * of the first argument and a value type equal to the type of second argument.
	 */
	public static final TypeStrategy MAP = callContext -> {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		if (argumentDataTypes.size() < 2) {
			return Optional.empty();
		}
		return Optional.of(DataTypes.MAP(argumentDataTypes.get(0), argumentDataTypes.get(1)).notNull());
	};

	/**
	 * Type strategy that returns a {@link DataTypes#ARRAY(DataType)} with element type equal to the type of
	 * the first argument.
	 */
	public static final TypeStrategy ARRAY = callContext -> {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		if (argumentDataTypes.size() < 1) {
			return Optional.empty();
		}
		return Optional.of(DataTypes.ARRAY(argumentDataTypes.get(0)).notNull());
	};

	/**
	 * Type strategy that returns the quotient of a exact numeric division that includes at least
	 * one decimal.
	 */
	public static final TypeStrategy DECIMAL_QUOTIENT = callContext -> {
		final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		final LogicalType numerator = argumentDataTypes.get(0).getLogicalType();
		final LogicalType denominator = argumentDataTypes.get(1).getLogicalType();
		// one decimal must be present
		if (!hasRoot(numerator, LogicalTypeRoot.DECIMAL) && !hasRoot(denominator, LogicalTypeRoot.DECIMAL)) {
			return Optional.empty();
		}
		// both must be exact numeric
		if (!hasFamily(numerator, LogicalTypeFamily.EXACT_NUMERIC) || !hasFamily(denominator, LogicalTypeFamily.EXACT_NUMERIC)) {
			return Optional.empty();
		}
		final DecimalType decimalType = LogicalTypeMerging.findDivisionDecimalType(
			getPrecision(numerator),
			getScale(numerator),
			getPrecision(denominator),
			getScale(denominator));
		return Optional.of(fromLogicalToDataType(decimalType));
	};

	// --------------------------------------------------------------------------------------------

	private TypeStrategies() {
		// no instantiation
	}
}
