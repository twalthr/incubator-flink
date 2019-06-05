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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * Definition of a function. Instances of this class enable unique identification across different
 * modules and provide all details necessary to validate a function call and perform planning.
 *
 * <p>A pure function definition must not contain a runtime implementation. This can be provided by
 * the planner at later stages.
 *
 * @see UserDefinedFunctionDefinition
 */
@PublicEvolving
public interface FunctionDefinition {

	/**
	 * Classifies the function definition.
	 */
	enum FunctionKind {
		AGGREGATE_FUNCTION,
		SCALAR_FUNCTION,
		TABLE_FUNCTION,
		TABLE_AGGREGATE_FUNCTION,
		OTHER_FUNCTION
	}

	/**
	 * Returns the name of function this definition describes. The name is usually equivalent to the
	 * name of the function in the catalog (if registered). However, this method serves mainly for
	 * printing to a console.
	 */
	String getName();

	/**
	 * Returns the kind of function this definition describes.
	 */
	FunctionKind getKind();

	/**
	 * Returns information about the determinism of the function's results.
	 *
	 * <p>It returns <code>true</code> if and only if a call to this function is guaranteed to
	 * always return the same result given the same parameters. <code>true</code> is
	 * assumed by default. If the function is not pure functional like <code>random(), date(), now(), ...</code>
	 * this method must return <code>false</code>.
	 */
	default boolean isDeterministic() {
		return true;
	}

	/**
	 * Returns logic for inferring and/or validating input and output types of functions.
	 */
	TypeInference getTypeInference();
}
