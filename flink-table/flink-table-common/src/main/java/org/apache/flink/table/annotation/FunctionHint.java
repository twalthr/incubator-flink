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

package org.apache.flink.table.annotation;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.inference.TypeInference;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A hint that influences the reflection-based extraction of a {@link TypeInference} for a
 * {@link UserDefinedFunction}.
 *
 * <p>A function hint can parameterize or replace the default extraction logic for input types, accumulator
 * types, and output types. An implementer can choose to what extent the default extraction logic
 * should be modified.
 *
 * <p>One or more annotations can be declared on top of a {@link UserDefinedFunction} class or individually
 * for each {@code eval()/accumulate()} method for overloading function signatures. All hint parameters
 * are optional. If a parameter is not defined, the default reflection-based extraction is used. Hint
 * parameters defined on top of a {@link UserDefinedFunction} class are inherited by all {@code eval()/accumulate()}
 * methods.
 *
 * <p>The following examples show how to explicitly specify function signatures in whole or in part
 * and let the default extraction do the rest:
 *
 * <pre>
 * {@code
 *   // accepts (INT, STRING) and returns BOOLEAN
 *   @FunctionHint(
 *     input = [@DataTypeHint("INT"), @DataTypeHint("STRING")],
 *     output = @DataTypeHint("BOOLEAN")
 *   )
 *   class ScalarFunction { ... }
 *
 *   // accepts (INT, STRING) or (BOOLEAN) and returns BOOLEAN
 *   @FunctionHint(
 *     input = [@DataTypeHint("INT"), @DataTypeHint("STRING")],
 *     output = @DataTypeHint("BOOLEAN")
 *   )
 *   @FunctionHint(
 *     input = [@DataTypeHint("BOOLEAN")],
 *     output = @DataTypeHint("BOOLEAN")
 *   )
 *   class ScalarFunction { ... }
 *
 *   // accepts (INT, STRING) or (BOOLEAN) and returns BOOLEAN
 *   @FunctionHint(
 *     output = @DataTypeHint("BOOLEAN")
 *   )
 *   class ScalarFunction {
 *     @FunctionHint(
 *       input = [@DataTypeHint("INT"), @DataTypeHint("STRING")]
 *     )
 *     @FunctionHint(
 *       input = [@DataTypeHint("BOOLEAN")]
 *     )
 *     Object eval(Object... o) { ... }
 *   }
 *
 *   // accepts (INT) or (BOOLEAN) and returns ROW<f0 BOOLEAN, f1 INT>
 *   @FunctionHint(
 *     output = @DataTypeHint("ROW<f0 BOOLEAN, f1 INT>")
 *   )
 *   class ScalarFunction {
 *     Row eval(int i) { ... }
 *     Row eval(boolean b) { ... }
 *   }
 *
 *   // accepts (ROW<f BOOLEAN>...) or (BOOLEAN...) and returns INT
 *   class ScalarFunction {
 *     @FunctionHint(
 *       input = [@DataTypeHint("ROW<f BOOLEAN>")],
 *       isVarArgs = TRUE
 *     )
 *     int eval(Row... r) { ... }
 *
 *     int eval(boolean... b) { ... }
 *   }
 * }
 * </pre>
 *
 * @see DataTypeHint
 */
@PublicEvolving
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Repeatable(FunctionHints.class)
public @interface FunctionHint {

	// Note to implementers:
	// Because "null" is not supported as an annotation value. Every annotation parameter has
	// some representation for unknown values in order to merge multi-level annotations.

	/**
	 * Explicitly lists the argument types that a function takes as input.
	 *
	 * <p>By default, explicit input types are undefined and the reflection-based extraction is
	 * used.
	 */
	DataTypeHint[] input() default @DataTypeHint();

	/**
	 * Defines that the last argument type defined in {@link #input()} should be treated as a
	 * variable-length argument.
	 *
	 * <p>By default, if {@link #input()} is defined, the last argument type is not a var-arg. Otherwise
	 * the reflection-based extraction decides about var-arg parameters.
	 */
	boolean isVarArgs() default false;

	/**
	 * Explicitly lists the argument names that a function takes as input.
	 *
	 * <p>By default, explicit input names are undefined and the reflection-based extraction is used.
	 */
	String[] argumentNames() default {""};

	/**
	 * Explicitly defines the intermediate result type that a function uses as accumulator.
	 *
	 * <p>By default, an explicit accumulator type is undefined and the reflection-based extraction
	 * is used.
	 */
	DataTypeHint accumulator() default @DataTypeHint();

	/**
	 * Explicitly defines the result type that a function uses as output.
	 *
	 * <p>By default, an explicit output type is undefined and the reflection-based extraction
	 * is used.
	 */
	DataTypeHint output() default @DataTypeHint();
}
