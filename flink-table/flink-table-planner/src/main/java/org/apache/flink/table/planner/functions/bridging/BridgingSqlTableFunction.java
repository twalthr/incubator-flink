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
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkState;

/** Bridges {@link FunctionDefinition} to Calcite's representation of a table function. */
@Internal
public final class BridgingSqlTableFunction extends BridgingSqlFunctionBase
        implements SqlTableFunction {

    private BridgingSqlTableFunction(
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            SqlKind kind,
            ContextResolvedFunction resolvedFunction,
            TypeInference typeInference) {
        super(dataTypeFactory, typeFactory, kind, resolvedFunction, typeInference);
    }

    /**
     * Creates an instance of a table function (either a system or user-defined function).
     *
     * @param dataTypeFactory used for creating {@link DataType}
     * @param typeFactory used for bridging to {@link RelDataType}
     * @param kind commonly used SQL standard function; use {@link SqlKind#OTHER_FUNCTION} if this
     *     function cannot be mapped to a common function kind.
     * @param resolvedFunction system or user-defined {@link FunctionDefinition} with context
     * @param typeInference type inference logic
     */
    public static BridgingSqlTableFunction of(
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            SqlKind kind,
            ContextResolvedFunction resolvedFunction,
            TypeInference typeInference) {
        final FunctionKind functionKind = resolvedFunction.getDefinition().getKind();
        checkState(functionKind == FunctionKind.TABLE, "Table function kind expected.");

        return new BridgingSqlTableFunction(
                dataTypeFactory, typeFactory, kind, resolvedFunction, typeInference);
    }

    /** Creates an instance of a table function during translation. */
    public static BridgingSqlTableFunction of(
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

    /** Creates an instance of a table function during translation. */
    public static BridgingSqlTableFunction of(
            RelOptCluster cluster, ContextResolvedFunction resolvedFunction) {
        final FlinkContext context = ShortcutUtils.unwrapContext(cluster);
        final FlinkTypeFactory typeFactory = ShortcutUtils.unwrapTypeFactory(cluster);
        return of(context, typeFactory, resolvedFunction);
    }

    /** Creates an instance of a table built-in function during translation. */
    public static BridgingSqlTableFunction of(
            RelOptCluster cluster, BuiltInFunctionDefinition functionDefinition) {
        return of(
                cluster,
                ContextResolvedFunction.permanent(
                        FunctionIdentifier.of(functionDefinition.getName()), functionDefinition));
    }

    @Override
    public SqlReturnTypeInference getRowTypeInference() {
        return opBinding -> {
            final RelDataType originalType = opBinding.getOperator().inferReturnType(opBinding);
            final RelDataType wrappedType;
            if (originalType.isStruct()) {
                wrappedType = originalType;
            } else {
                wrappedType =
                        opBinding
                                .getTypeFactory()
                                .createStructType(
                                        StructKind.NONE,
                                        Collections.singletonList(originalType),
                                        Collections.singletonList("f0"));
            }
            return opBinding.getTypeFactory().createTypeWithNullability(wrappedType, false);
        };
    }
}
