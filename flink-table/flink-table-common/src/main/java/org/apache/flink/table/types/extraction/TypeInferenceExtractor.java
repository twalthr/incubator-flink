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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.utils.ExtractionUtils;
import org.apache.flink.table.types.extraction.utils.FunctionTemplate;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.InputTypeValidators;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.inference.validators.CompositeTypeValidator;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
			asFunctionTemplates(collectAnnotationsOfClass(FunctionHint.class, function));

		// collect all global templates that only specify an output
		final Set<FunctionTemplate> globalOutputOnlyTemplates = globalTemplates.stream()
			.filter(t -> !t.hasInputTypeValidator() && t.hasOutputTypeStrategy())
			.collect(Collectors.toSet());

		// go through methods and collect output strategies
		final List<Method> methods =
			collectImplementationMethods(function, SCALAR_FUNCTION_EVAL);
		final Map<InputTypeValidator, TypeStrategy> outputStrategies = new HashMap<>();
		for (Method method : methods) {
			try {
				final Set<FunctionTemplate> localTemplates =
					asFunctionTemplates(collectAnnotationsOfMethod(FunctionHint.class, method));

				// collect all templates where input should map to some output
				final Set<FunctionTemplate> mappingTemplates = Stream.concat(globalTemplates.stream(), localTemplates.stream())
					.filter(t -> t.hasInputTypeValidator() && t.hasOutputTypeStrategy())
					.collect(Collectors.toSet());

				// collect all local templates that only specify an output
				final Set<FunctionTemplate> localOutputOnlyTemplates = localTemplates.stream()
					.filter(t -> !t.hasInputTypeValidator() && t.hasOutputTypeStrategy())
					.collect(Collectors.toSet());

				final Set<FunctionTemplate> outputOnlyTemplates = Stream.concat(globalOutputOnlyTemplates.stream(), localOutputOnlyTemplates.stream())
					.collect(Collectors.toSet());

				// output overloading is not supported
				if (outputOnlyTemplates.size() > 1 || (!outputOnlyTemplates.isEmpty() && !mappingTemplates.isEmpty())) {
					throw ExtractionUtils.extractionError(
						"Function hints with same input definition but different output types are not allowed.");
				}

				// collect all templates where only an input is defined
				final Set<InputTypeValidator> inputOnlyValidators = Stream.concat(globalTemplates.stream(), localTemplates.stream())
					.filter(t -> t.hasInputTypeValidator() && !t.hasOutputTypeStrategy())
					.map(t -> t.toInputTypeValidator(lookup))
					.collect(Collectors.toSet());

				final Map<InputTypeValidator, TypeStrategy> perMethodOutputStrategies = new HashMap<>();
				// add all mapping templates because they are complete signatures
				mappingTemplates.forEach(t -> {
					final InputTypeValidator mappingValidator = t.toInputTypeValidator(lookup);
					// input only validators are valid everywhere + mapping validator
					Stream.concat(inputOnlyValidators.stream(), Stream.of(mappingValidator))
						.forEach(v -> putStrategyMapping(perMethodOutputStrategies, v, t.toOutputTypeStrategy(lookup)));
				});
				// handle the output only templates
				outputOnlyTemplates.forEach(t -> {
					final TypeStrategy outputStrategy = t.toOutputTypeStrategy(lookup);
					// input only validators are valid everywhere if they don't exist fallback to extraction
					if (!inputOnlyValidators.isEmpty()) {
						inputOnlyValidators.forEach(v -> putStrategyMapping(perMethodOutputStrategies, v, outputStrategy));
					} else {
						putStrategyMapping(perMethodOutputStrategies, extractInputTypeValidator(lookup, method), outputStrategy);
					}
				});
				// handle missing output strategy
				if (perMethodOutputStrategies.isEmpty()) {
					// input only validators are valid everywhere if they don't exist fallback to extraction
					if (!inputOnlyValidators.isEmpty()) {
						inputOnlyValidators.forEach(v -> putStrategyMapping(perMethodOutputStrategies, v, extractOutputTypeStrategy(lookup, method)));
					} else {
						putStrategyMapping(perMethodOutputStrategies, extractInputTypeValidator(lookup, method), extractOutputTypeStrategy(lookup, method));
					}
				}

				// check if method strategies conflict with function strategies
				perMethodOutputStrategies.forEach((key, value) -> putStrategyMapping(outputStrategies, key, value));
			} catch (Exception e) {
				throw new ValidationException(
					String.format(
						"Unable to extract a type inference from function class '%s'. Analyzed method:\n%s",
						function.getName(),
						method.toString()
					),
					e
				);
			}
		}

		final TypeInference.Builder builder = TypeInference.newBuilder();

		// set argument name and type for the assignment operator "=>"
		// if the input signature is not ambiguous
		if (outputStrategies.size() == 1) {
			final InputTypeValidator validator = outputStrategies.keySet().iterator().next();
			if (validator instanceof CompositeTypeValidator) {
				final CompositeTypeValidator sequence = (CompositeTypeValidator) outputStrategies.keySet().iterator().next();
				final Optional<List<DataType>> explicitTypes = sequence.getExplicitArgumentTypes();
				final Optional<List<String>> explicitNames = sequence.getExplicitArgumentNames();
				if (explicitTypes.isPresent() && explicitNames.isPresent()) {
					builder.typedArguments(explicitTypes.get());
					builder.namedArguments(explicitNames.get());
				}
			}
		}

		final InputTypeValidator finalValidator = outputStrategies.keySet()
			.stream()
			.reduce(InputTypeValidators::or)
			.orElseThrow(IllegalStateException::new);
		builder.inputTypeValidator(finalValidator);

		final TypeStrategy finalOutputStrategy = TypeStrategies.matching(outputStrategies);
		builder.outputTypeStrategy(finalOutputStrategy);

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

	private static Set<FunctionTemplate> asFunctionTemplates(Set<FunctionHint> hints) {
		return hints.stream()
			.map(FunctionTemplate::fromAnnotation)
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

	private static void putStrategyMapping(
			Map<InputTypeValidator, TypeStrategy> mapping,
			InputTypeValidator validator,
			TypeStrategy strategy) {
		final TypeStrategy existingStrategy = mapping.get(validator);
		if (existingStrategy == null) {
			mapping.put(validator, strategy);
		}
		// strategies must not conflict with same input
		else if (!existingStrategy.equals(strategy)) {
			throw ExtractionUtils.extractionError(
				"Function hints with same input definition but different output types are not allowed.");
		}
	}
}
