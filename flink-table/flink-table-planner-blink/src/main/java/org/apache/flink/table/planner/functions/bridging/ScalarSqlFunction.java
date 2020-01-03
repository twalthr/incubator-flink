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

package org.apache.flink.table.planner.functions.bridging;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.inference.TypeInferenceOperandChecker;
import org.apache.flink.table.planner.functions.inference.TypeInferenceOperandInference;
import org.apache.flink.table.planner.functions.inference.TypeInferenceReturnInference;
import org.apache.flink.table.types.inference.TypeInference;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Calcite's representation of a scalar function (either system or user-defined).
 */
@Internal
public final class ScalarSqlFunction extends SqlFunction {

	private final FunctionIdentifier identifier;

	private final FunctionDefinition definition;

	private final TypeInference typeInference;

	private ScalarSqlFunction(
			FlinkTypeFactory typeFactory,
			SqlKind kind,
			FunctionIdentifier identifier,
			FunctionDefinition definition,
			TypeInference typeInference) {
		super(
			createName(identifier),
			createSqlIdentifier(identifier),
			kind,
			createSqlReturnTypeInference(definition, typeInference),
			createSqlOperandTypeInference(definition, typeInference),
			createSqlOperandTypeChecker(definition, typeInference),
			createParamTypes(typeFactory, typeInference),
			createSqlFunctionCategory(identifier));

		this.identifier = identifier;
		this.definition = definition;
		this.typeInference = typeInference;
	}

	/**
	 * Creates an instance of a scalar function (either system or user-defined).
	 *
	 * @param typeFactory used for resolving typed arguments
	 * @param kind commonly used SQL standard function; use {@link SqlKind#OTHER_FUNCTION} if this function
	 *             cannot be mapped to a common function kind.
	 * @param identifier catalog identifier
	 * @param definition system or user-defined {@link FunctionDefinition}
	 * @param typeInference type inference logic
	 */
	public static ScalarSqlFunction of(
			FlinkTypeFactory typeFactory,
			SqlKind kind,
			FunctionIdentifier identifier,
			FunctionDefinition definition,
			TypeInference typeInference) {
		return new ScalarSqlFunction(
			typeFactory,
			kind,
			identifier,
			definition,
			typeInference);
	}

	public FunctionIdentifier getIdentifier() {
		return identifier;
	}

	public FunctionDefinition getDefinition() {
		return definition;
	}

	public TypeInference getTypeInference() {
		return typeInference;
	}

	@Override
	public List<String> getParamNames() {
		if (typeInference.getNamedArguments().isPresent()) {
			return typeInference.getNamedArguments().get();
		}
		return super.getParamNames();
	}

	@Override
	public boolean isDeterministic() {
		return definition.isDeterministic();
	}

	// --------------------------------------------------------------------------------------------

	private static String createName(FunctionIdentifier identifier) {
		if (identifier.getSimpleName().isPresent()) {
			return identifier.getSimpleName().get();
		}
		return identifier.getIdentifier()
			.map(ObjectIdentifier::getObjectName)
			.orElseThrow(IllegalStateException::new);
	}

	private static SqlIdentifier createSqlIdentifier(FunctionIdentifier identifier) {
		return identifier.getIdentifier()
			.map(i -> new SqlIdentifier(i.toList(), SqlParserPos.ZERO))
			.orElse(null);
	}

	private static SqlReturnTypeInference createSqlReturnTypeInference(
			FunctionDefinition definition,
			TypeInference typeInference) {
		return new TypeInferenceReturnInference(definition, typeInference);
	}

	private static SqlOperandTypeInference createSqlOperandTypeInference(
			FunctionDefinition definition,
			TypeInference typeInference) {
		return new TypeInferenceOperandInference(definition, typeInference);
	}

	private static SqlOperandTypeChecker createSqlOperandTypeChecker(
			FunctionDefinition definition,
			TypeInference typeInference) {
		return new TypeInferenceOperandChecker(definition, typeInference);
	}

	private static List<RelDataType> createParamTypes(FlinkTypeFactory typeFactory, TypeInference typeInference) {
		return typeInference.getTypedArguments()
			.map(dataTypes ->
				dataTypes.stream()
					.map(dataType -> typeFactory.createFieldTypeFromLogicalType(dataType.getLogicalType()))
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static SqlFunctionCategory createSqlFunctionCategory(FunctionIdentifier identifier) {
		if (identifier.getSimpleName().isPresent()) {
			return SqlFunctionCategory.SYSTEM;
		}
		return SqlFunctionCategory.USER_DEFINED_FUNCTION;
	}
}
