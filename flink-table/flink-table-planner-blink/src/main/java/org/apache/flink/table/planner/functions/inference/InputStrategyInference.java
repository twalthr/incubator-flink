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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * A {@link SqlOperandTypeInference} backed by {@link InputTypeStrategy}.
 *
 * <p>Note: This class must be kept in sync with {@link TypeInferenceUtil}.
 */
public final class InputStrategyInference implements SqlOperandTypeInference {

	private final DataTypeLookup lookup;

	private final FunctionDefinition definition;

	private final InputTypeStrategy strategy;

	private final @Nullable List<DataType> typedArguments;

	public InputStrategyInference(
			DataTypeLookup lookup,
			FunctionDefinition definition,
			InputTypeStrategy strategy,
			@Nullable List<DataType> typedArguments) {
		this.lookup = lookup;
		this.definition = definition;
		this.strategy = strategy;
		this.typedArguments = typedArguments;
	}

	@Override
	public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
		final CallContext callContext = new SqlCallContext(lookup, definition, callBinding, returnType);
		try {
			inferOperandTypesOrError(callBinding.getTypeFactory(), callContext, operandTypes);
		} catch (ValidationException e) {
			throw new ValidationException(
				String.format(
					"Invalid function call:\n%s(%s)",
					callContext.getName(),
					callContext.getArgumentDataTypes().stream()
						.map(DataType::toString)
						.collect(Collectors.joining(", "))),
				e);
		} catch (Throwable t) {
			throw new TableException(
				String.format(
					"Unexpected error in type inference logic of function '%s'. This is a bug.",
					callContext.getName()),
				t);
		}
	}

	private void inferOperandTypesOrError(RelDataTypeFactory typeFactory, CallContext callContext, RelDataType[] operandTypes) {
		final FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) typeFactory;
		final List<DataType> inferredDataTypes;
		// typed arguments have highest priority
		if (typedArguments != null) {
			inferredDataTypes = typedArguments;
		} else {
			inferredDataTypes = strategy.inferInputTypes(callContext, false)
				.orElseThrow(() -> new ValidationException("Invalid input arguments."));
		}

		// input must not contain unknown types at this point
		if (inferredDataTypes.stream().anyMatch(InputStrategyInference::isUnknown)) {
			throw new ValidationException("Invalid use of untyped NULL in arguments.");
		}
		if (inferredDataTypes.size() != operandTypes.length) {
			return;
		}
		for (int i = 0; i < operandTypes.length; i++) {
			final LogicalType inferredType = inferredDataTypes.get(i).getLogicalType();
			operandTypes[i] = flinkTypeFactory.createFieldTypeFromLogicalType(inferredType);
		}
	}

	private static boolean isUnknown(DataType dataType) {
		return hasRoot(dataType.getLogicalType(), LogicalTypeRoot.NULL);
	}
}
