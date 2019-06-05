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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.inference.validators.NiladicTypeValidator;

/**
 * Validators for checking the input data types of a function call.
 *
 * <p>Please note that this class is a stub for now. In the future, it will be replaced by an advanced
 * type inference logic (see FLIP-37, part 2).
 *
 * @see InputTypeValidator
 */
@Internal
public final class InputTypeValidators {

	/**
	 * Validator for functions that don't take any parameters.
	 */
	public static final InputTypeValidator NILADIC = new NiladicTypeValidator();

	// --------------------------------------------------------------------------------------------

	private InputTypeValidators() {
		// no instantiation
	}
}
