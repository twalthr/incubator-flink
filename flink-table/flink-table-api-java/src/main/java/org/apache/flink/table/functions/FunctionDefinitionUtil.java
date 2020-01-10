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

package org.apache.flink.table.functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * A util to instantiate {@link FunctionDefinition} in the default way.
 */
public class FunctionDefinitionUtil {

	/**
	 * Currently only handles Java class-based functions until FLIP-65 is implemented for all
	 * function kinds.
	 */
	public static FunctionDefinition createFunctionDefinition(String name, String className) {
		Object func;
		try {
			func = Thread.currentThread().getContextClassLoader().loadClass(className).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			throw new IllegalStateException(
				String.format("Failed instantiating '%s'", className), e);
		}

		UserDefinedFunction udf = (UserDefinedFunction) func;

		if (udf instanceof ScalarFunction) {
			return new ScalarFunctionDefinition(
				name,
				(ScalarFunction) udf
			);
		} else if (udf instanceof TableFunction) {
			TableFunction<?> tableFunction = (TableFunction<?>) udf;
			TypeInformation<?> returnTypeInfo = UserDefinedFunctionHelper
				.getReturnTypeOfTableFunction(tableFunction);
			return new TableFunctionDefinition(
				name,
				tableFunction,
				returnTypeInfo
			);
		} else if (udf instanceof AggregateFunction) {
			AggregateFunction<?, ?> aggregateFunction = (AggregateFunction<?, ?>) udf;
			TypeInformation<?> returnTypeInfo = UserDefinedFunctionHelper
				.getReturnTypeOfAggregateFunction(aggregateFunction);
			TypeInformation<?> accTypeInfo = UserDefinedFunctionHelper
				.getAccumulatorTypeOfAggregateFunction(aggregateFunction);

			return new AggregateFunctionDefinition(
				name,
				aggregateFunction,
				accTypeInfo,
				returnTypeInfo
			);
		} else if (udf instanceof TableAggregateFunction) {
			TableAggregateFunction<?, ?> tableAggregateFunction = (TableAggregateFunction<?, ?>) udf;
			TypeInformation<?> returnTypeInfo = UserDefinedFunctionHelper
				.getReturnTypeOfAggregateFunction(tableAggregateFunction);
			TypeInformation<?> accTypeInfo = UserDefinedFunctionHelper
				.getAccumulatorTypeOfAggregateFunction(tableAggregateFunction);

			return new TableAggregateFunctionDefinition(
				name,
				tableAggregateFunction,
				returnTypeInfo,
				accTypeInfo
			);
		} else {
			throw new UnsupportedOperationException(
				String.format("Function %s should be of ScalarFunction, TableFunction, AggregateFunction, or TableAggregateFunction", className)
			);
		}
	}
}
