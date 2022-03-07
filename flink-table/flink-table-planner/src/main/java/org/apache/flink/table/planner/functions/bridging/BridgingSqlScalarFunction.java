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
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

import static org.apache.flink.util.Preconditions.checkState;

/** Bridges {@link FunctionDefinition} to Calcite's representation of a scalar function. */
@Internal
public class BridgingSqlScalarFunction extends BridgingSqlFunctionBase {

    private BridgingSqlScalarFunction(
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            SqlKind kind,
            ContextResolvedFunction resolvedFunction,
            TypeInference typeInference) {
        super(dataTypeFactory, typeFactory, kind, resolvedFunction, typeInference);
    }

    /**
     * Creates an instance of a scalar function (either a system or user-defined function).
     *
     * @param dataTypeFactory used for creating {@link DataType}
     * @param typeFactory used for bridging to {@link RelDataType}
     * @param kind commonly used SQL standard function; use {@link SqlKind#OTHER_FUNCTION} if this
     *     function cannot be mapped to a common function kind.
     * @param resolvedFunction system or user-defined {@link FunctionDefinition} with context
     * @param typeInference type inference logic
     */
    public static BridgingSqlScalarFunction of(
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            SqlKind kind,
            ContextResolvedFunction resolvedFunction,
            TypeInference typeInference) {
        final FunctionKind functionKind = resolvedFunction.getDefinition().getKind();
        checkState(functionKind == FunctionKind.SCALAR, "Scalar function kind expected.");

        return new BridgingSqlScalarFunction(
                dataTypeFactory, typeFactory, kind, resolvedFunction, typeInference);
    }

    /** Creates an instance of a scalar function during translation. */
    public static BridgingSqlScalarFunction of(
            FlinkContext context,
            FlinkTypeFactory typeFactory,
            ContextResolvedFunction resolvedFunction) {
        final DataTypeFactory dataTypeFactory = context.getCatalogManager().getDataTypeFactory();
        final TypeInference typeInference =
                resolvedFunction.getDefinition().getTypeInference(dataTypeFactory);
        return of(
                dataTypeFactory,
                typeFactory,
                SqlKind.OTHER_FUNCTION,
                resolvedFunction,
                typeInference);
    }

    /** Creates an instance of a scalar function during translation. */
    public static BridgingSqlScalarFunction of(
            RelOptCluster cluster, ContextResolvedFunction resolvedFunction) {
        final FlinkContext context = ShortcutUtils.unwrapContext(cluster);
        final FlinkTypeFactory typeFactory = ShortcutUtils.unwrapTypeFactory(cluster);
        return of(context, typeFactory, resolvedFunction);
    }

    /** Creates an instance of a scalar built-in function during translation. */
    public static BridgingSqlScalarFunction of(
            RelOptCluster cluster, BuiltInFunctionDefinition functionDefinition) {
        return of(
                cluster,
                ContextResolvedFunction.permanent(
                        FunctionIdentifier.of(functionDefinition.getName()), functionDefinition));
    }
}
