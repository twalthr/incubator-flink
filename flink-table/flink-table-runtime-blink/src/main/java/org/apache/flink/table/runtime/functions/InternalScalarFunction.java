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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeInferenceUtil;

import java.util.Set;

/**
 * {@link ScalarFunction} for runtime implementation of {@link BuiltInFunctionDefinition}.
 */
@Internal
public class InternalScalarFunction extends ScalarFunction {

	protected BuiltInFunctionDefinition builtInDefinition;

	void setBuiltInDefinition(BuiltInFunctionDefinition builtInDefinition) {
		this.builtInDefinition = builtInDefinition;
	}

	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		final TypeInference originalInference = builtInDefinition.getTypeInference(typeFactory);
		return TypeInferenceUtil.createInternalTypeInference(originalInference);
	}

	@Override
	public Set<FunctionRequirement> getRequirements() {
		return builtInDefinition.getRequirements();
	}

	@Override
	public boolean isDeterministic() {
		return builtInDefinition.isDeterministic();
	}
}
