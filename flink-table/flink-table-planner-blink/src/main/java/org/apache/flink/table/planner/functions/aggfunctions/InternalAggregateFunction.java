package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.plan.utils.AggFunctionFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

import static org.apache.flink.table.types.inference.TypeStrategies.explicit;

/**
 * Base class for fully resolved {@link AggregateFunction}s provided by {@link AggFunctionFactory}.
 */
@Internal
public abstract class InternalAggregateFunction<T, ACC> extends AggregateFunction<T, ACC> {

	public abstract DataType[] getInputDataTypes();

	public abstract DataType getAccumulatorDataType();

	public abstract DataType getOutputDataType();

	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		return TypeInference.newBuilder()
			.typedArguments(getInputDataTypes())
			.accumulatorTypeStrategy(explicit(getAccumulatorDataType()))
			.outputTypeStrategy(explicit(getOutputDataType()))
			.build();
	}
}
