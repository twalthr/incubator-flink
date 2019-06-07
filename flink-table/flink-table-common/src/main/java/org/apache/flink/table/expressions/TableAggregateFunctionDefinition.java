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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * The function definition of an user-defined table aggregate function.
 */
@PublicEvolving
public final class TableAggregateFunctionDefinition implements FunctionDefinition {

	private final TableAggregateFunction<?, ?> tableAggregateFunction;
	private final TypeInformation<?> resultTypeInfo;
	private final TypeInformation<?> accumulatorTypeInfo;

	public TableAggregateFunctionDefinition(
			TableAggregateFunction<?, ?> tableAggregateFunction,
			TypeInformation<?> resultTypeInfo,
			TypeInformation<?> accTypeInfo) {
		this.tableAggregateFunction = Preconditions.checkNotNull(tableAggregateFunction);
		this.resultTypeInfo = Preconditions.checkNotNull(resultTypeInfo);
		this.accumulatorTypeInfo = Preconditions.checkNotNull(accTypeInfo);
	}

	public TableAggregateFunction<?, ?> getTableAggregateFunction() {
		return tableAggregateFunction;
	}

	public TypeInformation<?> getResultTypeInfo() {
		return resultTypeInfo;
	}

	public TypeInformation<?> getAccumulatorTypeInfo() {
		return accumulatorTypeInfo;
	}

	@Override
	public FunctionKind getKind() {
		return FunctionKind.TABLE_AGGREGATE_FUNCTION;
	}

	@Override
	public String toString() {
		return tableAggregateFunction.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TableAggregateFunctionDefinition that = (TableAggregateFunctionDefinition) o;
		return tableAggregateFunction.equals(that.tableAggregateFunction) &&
			resultTypeInfo.equals(that.resultTypeInfo) &&
			accumulatorTypeInfo.equals(that.accumulatorTypeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(tableAggregateFunction, resultTypeInfo, accumulatorTypeInfo);
	}
}
