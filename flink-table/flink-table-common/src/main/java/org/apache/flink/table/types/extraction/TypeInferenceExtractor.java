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

package org.apache.flink.table.types.extraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.utils.DataTypeTemplate;
import org.apache.flink.table.types.extraction.utils.ExtractionUtils;
import org.apache.flink.table.types.extraction.utils.FunctionInputTemplate;
import org.apache.flink.table.types.extraction.utils.FunctionTemplate;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.InputTypeValidators;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.collectAnnotationsOfClass;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.collectAnnotationsOfMethod;
import static org.apache.flink.table.types.extraction.utils.ReflectiveValidationUtils.validateInstantiation;
import static org.apache.flink.table.types.extraction.utils.ReflectiveValidationUtils.validateMethodInvocation;

/**
 * Reflection-based utility for extracting a {@link TypeInference} from a supported subclass of
 * {@link UserDefinedFunction}.
 *
 * <p>The behavior of this utility can be influenced by {@link DataTypeHint}s and {@link FunctionHint}s
 * which have higher precedence than the reflective information.
 */
@Internal
public final class TypeInferenceExtractor {

	private static final String SCALAR_FUNCTION_EVAL = "eval";
	private static final String TABLE_FUNCTION_EVAL = "eval";
	private static final String AGGREGATE_FUNCTION_ACCUMULATE = "accumulate";
	private static final String AGGREGATE_FUNCTION_RETRACT = "retract";
	private static final String AGGREGATE_FUNCTION_MERGE = "merge";
	private static final String AGGREGATE_FUNCTION_RESET = "resetAccumulator";
	private static final String TABLE_AGGREGATE_FUNCTION_ACCUMULATE = "accumulate";
	private static final String TABLE_AGGREGATE_FUNCTION_EMIT_VALUE = "emitValue";
	private static final String TABLE_AGGREGATE_FUNCTION_EMIT_VALUE_RETRACT = "emitUpdateWithRetract";
	private static final String TABLE_AGGREGATE_FUNCTION_RETRACT = "retract";
	private static final String ASYNC_TABLE_FUNCTION_EVAL = "eval";

	public static void validateScalarFunction(Class<? extends ScalarFunction> function) {
		validateInstantiation(function);
		validateMethodInvocation(function, SCALAR_FUNCTION_EVAL, false, false);
	}

	public static void validateTableFunction(Class<? extends TableFunction> function) {
		validateInstantiation(function);
		validateMethodInvocation(function, TABLE_FUNCTION_EVAL, true, false);
	}

	public static void validateAggregateFunction(Class<? extends AggregateFunction> function) {
		validateInstantiation(function);
		validateMethodInvocation(function, AGGREGATE_FUNCTION_ACCUMULATE, true, false);
		validateMethodInvocation(function, AGGREGATE_FUNCTION_RETRACT, true, true);
		validateMethodInvocation(function, AGGREGATE_FUNCTION_MERGE, true, true);
		validateMethodInvocation(function, AGGREGATE_FUNCTION_RESET, true, true);
	}

	public static void validateTableAggregateFunction(Class<? extends TableAggregateFunction> function) {
		validateInstantiation(function);
		validateMethodInvocation(function, TABLE_AGGREGATE_FUNCTION_ACCUMULATE, true, false);
		validateMethodInvocation(function, TABLE_AGGREGATE_FUNCTION_EMIT_VALUE, true, true);
		validateMethodInvocation(function, TABLE_AGGREGATE_FUNCTION_EMIT_VALUE_RETRACT, true, true);
		validateMethodInvocation(function, TABLE_AGGREGATE_FUNCTION_RETRACT, true, true);
	}

	public static void validateAsyncTableFunction(Class<? extends AsyncTableFunction> function) {
		validateInstantiation(function);
		validateMethodInvocation(function, ASYNC_TABLE_FUNCTION_EVAL, true, false);
	}

	public static TypeInference extractFromScalarFunction(DataTypeLookup lookup, Class<? extends ScalarFunction> function) {
		final Set<FunctionTemplate> globalTemplates =
			asFunctionTemplates(lookup, collectAnnotationsOfClass(FunctionHint.class, function));

		// collect all global templates that only specify an output
		final Set<DataTypeTemplate> globalOutputTemplates = globalTemplates.stream()
			.filter(t -> !t.hasInputDefinition() && t.hasOutputDefinition())
			.map(t -> t.outputTemplate)
			.collect(Collectors.toSet());

		// go through methods and collect output strategies
		final List<Method> methods = collectImplementationMethods(function, SCALAR_FUNCTION_EVAL);
		final Map<FunctionInputTemplate, DataTypeTemplate> outputMapping = new HashMap<>();
		for (Method method : methods) {
			try {
				final Set<FunctionTemplate> localTemplates =
					asFunctionTemplates(lookup, collectAnnotationsOfMethod(FunctionHint.class, method));

				// collect all templates where input should map to some output
				final Set<FunctionTemplate> mappingTemplates = Stream.concat(globalTemplates.stream(), localTemplates.stream())
					.filter(t -> t.hasInputDefinition() && t.hasOutputDefinition())
					.collect(Collectors.toSet());

				// collect all local templates that only specify an output
				final Set<DataTypeTemplate> localOutputTemplates = localTemplates.stream()
					.filter(t -> !t.hasInputDefinition() && t.hasOutputDefinition())
					.map(t -> t.outputTemplate)
					.collect(Collectors.toSet());

				final Set<DataTypeTemplate> outputTemplates = Stream.concat(globalOutputTemplates.stream(), localOutputTemplates.stream())
					.collect(Collectors.toSet());

				// output overloading is not supported
				if (outputTemplates.size() > 1 || (!outputTemplates.isEmpty() && !mappingTemplates.isEmpty())) {
					throw ExtractionUtils.extractionError(
						"Function hints with same input definition but different output types are not allowed.");
				}

				// collect all templates where only an input is defined
				final Set<FunctionInputTemplate> inputTemplates = Stream.concat(globalTemplates.stream(), localTemplates.stream())
					.filter(t -> t.hasInputDefinition() && !t.hasOutputDefinition())
					.map(t -> t.inputTemplate)
					.collect(Collectors.toSet());

				final Map<FunctionInputTemplate, DataTypeTemplate> perMethodOutputMapping = new HashMap<>();
				// add all mapping templates because they are complete signatures
				mappingTemplates.forEach(t -> {
					// input templates are valid everywhere + mapping validator
					Stream.concat(inputTemplates.stream(), Stream.of(t.inputTemplate))
						.forEach(v -> putMapping(perMethodOutputMapping, v, t.outputTemplate));
				});
				// handle the output only templates
				outputTemplates.forEach(t -> {
					// input templates are valid everywhere if they don't exist fallback to extraction
					if (!inputTemplates.isEmpty()) {
						inputTemplates.forEach(v -> putMapping(perMethodOutputMapping, v, t));
					} else {
						putMapping(perMethodOutputMapping, FunctionInputTemplate.extractFromMethod(lookup, method), t);
					}
				});
				// handle missing output strategy
				if (perMethodOutputMapping.isEmpty()) {
					// input only validators are valid everywhere if they don't exist fallback to extraction
					if (!inputTemplates.isEmpty()) {
						inputTemplates.forEach(v -> putMapping(perMethodOutputMapping, v, DataTypeTemplate.extractFromMethod(lookup, method)));
					} else {
						putMapping(
							perMethodOutputMapping,
							FunctionInputTemplate.extractFromMethod(lookup, method),
							DataTypeTemplate.extractFromMethod(lookup, method));
					}
				}

				// check if method strategies conflict with function strategies
				perMethodOutputMapping.forEach((key, value) -> putMapping(outputMapping, key, value));
			} catch (Exception e) {
				throw ExtractionUtils.extractionError(
					e,
					"Unable to extract a type inference from function class '%s'. Analyzed method:\n%s",
					function.getName(),
					method.toString());
			}
		}

		final TypeInference.Builder builder = TypeInference.newBuilder();

		// set argument name and type for the assignment operator "=>"
		// if the input signature is not ambiguous and not vararg
		if (outputMapping.size() == 1) {
			final FunctionInputTemplate inputTemplate = outputMapping.keySet().iterator().next();
			final List<DataType> dataTypes = inputTemplate.argumentTemplates.stream()
				.map(t -> t.dataType)
				.collect(Collectors.toList());
			if (!inputTemplate.isVarArgs && dataTypes.stream().allMatch(Objects::nonNull)) {
				builder.typedArguments(dataTypes);
				if (inputTemplate.argumentNames != null) {
					builder.namedArguments(Arrays.asList(inputTemplate.argumentNames));
				}
			}
		}

		try {
			final Map<InputTypeValidator, TypeStrategy> matchers = outputMapping.entrySet()
				.stream()
				.collect(
					Collectors.toMap(
						e -> e.getKey().toInputTypeValidator(),
						e -> e.getValue().toTypeStrategy()));

			final TypeStrategy finalOutputStrategy = TypeStrategies.matching(matchers);
			builder.outputTypeStrategy(finalOutputStrategy);

			final InputTypeValidator finalValidator = matchers.keySet()
				.stream()
				.reduce(InputTypeValidators::or)
				.orElseThrow(IllegalStateException::new);
			builder.inputTypeValidator(finalValidator);

		} catch (Exception e) {
			throw ExtractionUtils.extractionError(
				e,
				"Unable to extract a type inference from function class '%s'.",
				function.getName());
		}



		return builder.build();
	}

	public static TypeInference extractFromTableFunction(Class<? extends TableFunction> function) {
		return null;
	}

	public static TypeInference extractFromAggregateFunction(Class<? extends AggregateFunction> function) {
		return null;
	}

	public static TypeInference extractFromTableAggregateFunction(Class<? extends TableAggregateFunction> function) {
		return null;
	}

	public static TypeInference extractFromAsyncTableFunction(Class<? extends AsyncTableFunction> function) {
		return null;
	}

	// -------------------------------------------------------------------------------------------

	private static Set<FunctionTemplate> asFunctionTemplates(DataTypeLookup lookup, Set<FunctionHint> hints) {
		return hints.stream()
			.map(hint -> FunctionTemplate.fromAnnotation(lookup, hint))
			.collect(Collectors.toSet());
	}

	private static List<Method> collectImplementationMethods(Class<?> function, String methodName) {
		return Arrays.stream(function.getMethods())
			.filter(method -> method.getName().equals(methodName))
			.collect(Collectors.toList());
	}

	private static InputTypeValidator extractInputTypeValidator(DataTypeLookup lookup, Method method) {
		throw new IllegalStateException();
	}

	private static TypeStrategy extractOutputTypeStrategy(DataTypeLookup lookup, Method method) {
		throw new IllegalStateException();
	}

	private static void putMapping(
			Map<FunctionInputTemplate, DataTypeTemplate> mapping,
			FunctionInputTemplate inputTemplate,
			DataTypeTemplate template) {
		final DataTypeTemplate existingMapping = mapping.get(inputTemplate);
		if (existingMapping == null) {
			mapping.put(inputTemplate, template);
		}
		// template must not conflict with same input
		else if (!existingMapping.equals(template)) {
			throw ExtractionUtils.extractionError(
				"Function hints with same input definition but different output types are not allowed.");
		}
	}
}
