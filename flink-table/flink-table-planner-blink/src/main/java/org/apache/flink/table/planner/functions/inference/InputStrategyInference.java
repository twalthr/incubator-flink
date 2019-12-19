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

import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

/**
 * A {@link SqlOperandTypeInference} backed by {@link InputTypeStrategy}.
 */
public final class InputStrategyInference implements SqlOperandTypeInference {

	private final DataTypeLookup lookup;

	private final FunctionDefinition definition;

	private final InputTypeStrategy strategy;

	public InputStrategyInference(
			DataTypeLookup lookup,
			FunctionDefinition definition,
			InputTypeStrategy strategy) {
		this.lookup = lookup;
		this.definition = definition;
		this.strategy = strategy;
	}

	@Override
	public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
		final DataType outputDataType;
		if (returnType.equals(callBinding.getValidator().getUnknownType())) {
			outputDataType = null;
		} else {
			final LogicalType logicalType = FlinkTypeFactory.toLogicalType(returnType);
			outputDataType = TypeConversions.fromLogicalToDataType(logicalType);
		}
		final CallContext callContext = new SqlCallContext(lookup, definition, callBinding, outputDataType);
		strategy.inferInputTypes(callContext, false)
			.ifPresent(arg);
	}
}
