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

package org.apache.flink.table.types.extraction.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.types.inference.ArgumentTypeValidator;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.InputTypeValidators;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

/**
 * Template of a {@link InputTypeValidator}.
 */
@Internal
public final class FunctionInputTemplate {

	public final List<DataTypeTemplate> argumentTemplates;

	public final boolean isVarArgs;

	// not part of equals/hashCode because it is irrelevant for overloading
	public final @Nullable String[] argumentNames;

	public FunctionInputTemplate(
			List<DataTypeTemplate> argumentTemplates,
			boolean isVarArgs,
			@Nullable String[] argumentNames) {
		if (argumentNames != null && argumentNames.length != argumentTemplates.size()) {
			throw ExtractionUtils.extractionError(
				"Mismatch between number of argument names '%s' and argument types '%s'.",
				argumentNames.length,
				argumentTemplates.size());
		}
		this.argumentTemplates = argumentTemplates;
		this.isVarArgs = isVarArgs;
		this.argumentNames = argumentNames;
	}

	public static FunctionInputTemplate extractFromMethod(DataTypeLookup lookup, Method method) {
		throw new UnsupportedOperationException(); // TODO
	}

	public InputTypeValidator toInputTypeValidator() {
		final ArgumentTypeValidator[] argumentValidators = argumentTemplates.stream()
			.map(DataTypeTemplate::toArgumentTypeValidator)
			.toArray(ArgumentTypeValidator[]::new);

		final InputTypeValidator validator;
		if (isVarArgs) {
			if (argumentNames == null) {
				validator = InputTypeValidators.varyingSequence(argumentValidators);
			} else {
				validator = InputTypeValidators.varyingSequence(argumentNames, argumentValidators);
			}
		} else {
			if (argumentNames == null) {
				validator = InputTypeValidators.sequence(argumentValidators);
			} else {
				validator = InputTypeValidators.sequence(argumentNames, argumentValidators);
			}
		}
		return validator;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		FunctionInputTemplate that = (FunctionInputTemplate) o;
		return isVarArgs == that.isVarArgs &&
			argumentTemplates.equals(that.argumentTemplates);
	}

	@Override
	public int hashCode() {
		return Objects.hash(argumentTemplates, isVarArgs);
	}
}
