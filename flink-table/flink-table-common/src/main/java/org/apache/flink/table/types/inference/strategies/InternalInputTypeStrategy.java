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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link InputTypeStrategy} that always returns {@link DataType}s of internal data structures.
 */
@Internal
public final class InternalInputTypeStrategy implements InputTypeStrategy {

	private final InputTypeStrategy originalStrategy;

	public InternalInputTypeStrategy(InputTypeStrategy originalStrategy) {
		this.originalStrategy = originalStrategy;
	}

	@Override
	public ArgumentCount getArgumentCount() {
		return originalStrategy.getArgumentCount();
	}

	@Override
	public Optional<List<DataType>> inferInputTypes(
			CallContext callContext,
			boolean throwOnFailure) {
		return originalStrategy
			.inferInputTypes(callContext, throwOnFailure)
			.map(dataTypes ->
				dataTypes.stream()
					.map(dataType -> DataTypeUtils.transform(dataType, TypeTransformations.TO_INTERNAL_CLASS))
					.collect(Collectors.toList()));
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		return originalStrategy.getExpectedSignatures(definition);
	}
}
