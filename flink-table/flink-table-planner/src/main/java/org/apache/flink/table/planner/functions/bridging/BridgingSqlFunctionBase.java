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
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createName;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createParamTypes;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlFunctionCategory;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlIdentifier;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeChecker;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeInference;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlReturnTypeInference;

/**
 * Bridges {@link FunctionDefinition} to Calcite's representation for either a system or
 * user-defined function.
 */
@Internal
public class BridgingSqlFunctionBase extends SqlFunction {

    private final DataTypeFactory dataTypeFactory;

    private final FlinkTypeFactory typeFactory;

    private final ContextResolvedFunction resolvedFunction;

    private final TypeInference typeInference;

    BridgingSqlFunctionBase(
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            SqlKind kind,
            ContextResolvedFunction resolvedFunction,
            TypeInference typeInference) {
        super(
                createName(resolvedFunction),
                createSqlIdentifier(resolvedFunction),
                kind,
                createSqlReturnTypeInference(
                        dataTypeFactory, resolvedFunction.getDefinition(), typeInference),
                createSqlOperandTypeInference(
                        dataTypeFactory, resolvedFunction.getDefinition(), typeInference),
                createSqlOperandTypeChecker(
                        dataTypeFactory, resolvedFunction.getDefinition(), typeInference),
                createParamTypes(typeFactory, typeInference),
                createSqlFunctionCategory(resolvedFunction));

        this.dataTypeFactory = dataTypeFactory;
        this.typeFactory = typeFactory;
        this.resolvedFunction = resolvedFunction;
        this.typeInference = typeInference;
    }

    public DataTypeFactory getDataTypeFactory() {
        return dataTypeFactory;
    }

    public FlinkTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public ContextResolvedFunction getResolvedFunction() {
        return resolvedFunction;
    }

    public FunctionDefinition getDefinition() {
        return resolvedFunction.getDefinition();
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
        return resolvedFunction.getDefinition().isDeterministic();
    }
}
