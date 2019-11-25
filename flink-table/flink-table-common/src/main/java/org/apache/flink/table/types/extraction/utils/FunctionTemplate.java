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
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.HintFlag;
import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.InputTypeValidators;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.inference.validators.SingleInputTypeValidator;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Internal representation of a {@link FunctionHint} with more contextual information used to produce
 * the final {@link TypeInference}.
 */
@Internal
public final class FunctionTemplate {

	private static final FunctionHint DEFAULT_ANNOTATION = getDefaultAnnotation();

	public final @Nullable List<DataTypeTemplate> argumentTypes;

	public final @Nullable Boolean isVarArgs;

	public final @Nullable String[] argumentNames;

	public final @Nullable DataTypeTemplate accumulatorType;

	public final @Nullable DataTypeTemplate outputType;

	private FunctionTemplate(
			@Nullable List<DataTypeTemplate> argumentTypes,
			@Nullable Boolean isVarArgs,
			@Nullable String[] argumentNames,
			@Nullable DataTypeTemplate accumulatorType,
			@Nullable DataTypeTemplate outputType) {
		this.argumentTypes = argumentTypes;
		this.isVarArgs = isVarArgs;
		this.argumentNames = argumentNames;
		this.accumulatorType = accumulatorType;
		this.outputType = outputType;
	}

	public static FunctionTemplate fromAnnotation(FunctionHint hint) {
		return new FunctionTemplate(
			dataTypeHintsToTemplates(defaultAsNull(hint, FunctionHint::input)),
			hintFlagToBoolean(defaultAsNull(hint, FunctionHint::isVarArgs)),
			defaultAsNull(hint, FunctionHint::argumentNames),
			dataTypeHintToTemplate(defaultAsNull(hint, FunctionHint::accumulator)),
			dataTypeHintToTemplate(defaultAsNull(hint, FunctionHint::output))
		);
	}

	public boolean hasInputTypeValidator() {
		return argumentTypes != null;
	}

	public InputTypeValidator toInputTypeValidator(DataTypeLookup lookup) {
		Preconditions.checkState(argumentTypes != null);
		if (argumentNames != null && argumentNames.length != argumentTypes.size()) {
			throw ExtractionUtils.extractionError(
				"Mismatch between number of argument names '%s' and argument types '%s'.",
				argumentNames.length,
				argumentTypes.size());
		}

		final SingleInputTypeValidator[] argumentValidators = argumentTypes.stream()
			.map(template -> template.toSingleInputTypeValidator(lookup))
			.toArray(SingleInputTypeValidator[]::new);

		final InputTypeValidator validator;
		if (isVarArgs != null && isVarArgs) {
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

	public boolean hasOutputTypeStrategy() {
		return outputType != null;
	}

	public TypeStrategy toOutputTypeStrategy(DataTypeLookup lookup) {
		Preconditions.checkState(outputType != null);
		return outputType.toTypeStrategy(lookup);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		FunctionTemplate that = (FunctionTemplate) o;
		return Objects.equals(argumentTypes, that.argumentTypes) &&
			Objects.equals(isVarArgs, that.isVarArgs) &&
			Arrays.equals(argumentNames, that.argumentNames) &&
			Objects.equals(accumulatorType, that.accumulatorType) && Objects.equals(outputType, that.outputType);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(argumentTypes, isVarArgs, accumulatorType, outputType);
		result = 31 * result + Arrays.hashCode(argumentNames);
		return result;
	}

// --------------------------------------------------------------------------------------------

	@FunctionHint
	private static class DefaultAnnotationHelper {
		// no implementation
	}

	private static FunctionHint getDefaultAnnotation() {
		return DefaultAnnotationHelper.class.getAnnotation(FunctionHint.class);
	}

	private static <T> T defaultAsNull(FunctionHint hint, Function<FunctionHint, T> accessor) {
		final T defaultValue = accessor.apply(DEFAULT_ANNOTATION);
		final T actualValue = accessor.apply(hint);
		if (Objects.deepEquals(defaultValue, actualValue)) {
			return null;
		}
		return actualValue;
	}

	private static List<DataTypeTemplate> dataTypeHintsToTemplates(DataTypeHint[] hints) {
		if (hints == null) {
			return null;
		}
		return Stream.of(hints)
			.map(DataTypeTemplate::fromAnnotation)
			.collect(Collectors.toList());
	}

	private static DataTypeTemplate dataTypeHintToTemplate(DataTypeHint hint) {
		if (hint == null) {
			return null;
		}
		return DataTypeTemplate.fromAnnotation(hint);
	}

	private static Boolean hintFlagToBoolean(HintFlag flag) {
		if (flag == null) {
			return null;
		}
		return flag == HintFlag.TRUE;
	}
}
