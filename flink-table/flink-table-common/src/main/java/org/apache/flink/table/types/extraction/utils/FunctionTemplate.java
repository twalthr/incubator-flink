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
import org.apache.flink.table.catalog.DataTypeLookup;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Internal representation of a {@link FunctionHint}.
 */
@Internal
public final class FunctionTemplate {

	private static final FunctionHint DEFAULT_ANNOTATION = getDefaultAnnotation();

	public final @Nullable FunctionInputTemplate inputTemplate;

	public final @Nullable DataTypeTemplate accumulatorTemplate;

	public final @Nullable DataTypeTemplate outputTemplate;

	private FunctionTemplate(
			@Nullable FunctionInputTemplate inputTemplate,
			@Nullable DataTypeTemplate accumulatorTemplate,
			@Nullable DataTypeTemplate outputTemplate) {
		this.inputTemplate = inputTemplate;
		this.accumulatorTemplate = accumulatorTemplate;
		this.outputTemplate = outputTemplate;
	}

	public static FunctionTemplate fromAnnotation(DataTypeLookup lookup, FunctionHint hint) {
		return new FunctionTemplate(
			createFunctionInputTemplate(lookup, hint),
			dataTypeHintToTemplate(lookup, defaultAsNull(hint, FunctionHint::accumulator)),
			dataTypeHintToTemplate(lookup, defaultAsNull(hint, FunctionHint::output))
		);
	}

	public boolean hasInputDefinition() {
		return inputTemplate != null;
	}

	public boolean hasAccumulatorDefinition() {
		return accumulatorTemplate != null;
	}

	public boolean hasOutputDefinition() {
		return outputTemplate != null;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		FunctionTemplate template = (FunctionTemplate) o;
		return Objects.equals(inputTemplate, template.inputTemplate) &&
			Objects.equals(accumulatorTemplate, template.accumulatorTemplate) &&
			Objects.equals(outputTemplate, template.outputTemplate);
	}

	@Override
	public int hashCode() {
		return Objects.hash(inputTemplate, accumulatorTemplate, outputTemplate);
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

	private static @Nullable FunctionInputTemplate createFunctionInputTemplate(DataTypeLookup lookup, FunctionHint hint) {
		final DataTypeHint[] argumentHints = defaultAsNull(hint, FunctionHint::input);
		if (argumentHints == null) {
			return null;
		}
		return new FunctionInputTemplate(
			Arrays.stream(argumentHints)
				.map(dataTypeHint -> DataTypeTemplate.fromAnnotation(lookup, dataTypeHint))
				.collect(Collectors.toList()),
			hint.isVarArgs(),
			defaultAsNull(hint, FunctionHint::argumentNames));
	}

	private static DataTypeTemplate dataTypeHintToTemplate(DataTypeLookup lookup, DataTypeHint hint) {
		if (hint == null) {
			return null;
		}
		return DataTypeTemplate.fromAnnotation(lookup, hint);
	}
}
