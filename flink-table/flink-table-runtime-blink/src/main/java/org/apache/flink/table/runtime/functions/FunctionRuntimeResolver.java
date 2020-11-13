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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.utils.ThreadLocalCache;

/**
 * Creates a more specific {@link FunctionDefinition} that includes runtime implementation if possible.
 *
 * <p>{@link BuiltInFunctionDefinition} get resolved if they define a {@link BuiltInFunctionDefinition#getRuntimeClassName()}.
 *
 * <p>Other functions are not modified because they already contain runtime implementation (i.e. {@link UserDefinedFunction})
 * or will be code generated.
 */
@Internal
public final class FunctionRuntimeResolver {

	private static final ThreadLocalCache<String, UserDefinedFunction> FUNCTION_CACHE =
		new ThreadLocalCache<String, UserDefinedFunction>() {
			@Override
			public UserDefinedFunction getNewInstance(String className) {
				try {
					final Class<?> function = Class.forName(
						className,
						true,
						FunctionRuntimeResolver.class.getClassLoader());
					return (UserDefinedFunction) function.newInstance();
				} catch (Exception t) {
					throw new TableException(
						String.format("Unable to instantiate built-in function '%s'.", className),
						t);
				}
			}
		};

	public static FunctionDefinition resolveFunctionDefinition(FunctionDefinition definition) {
		if (definition instanceof BuiltInFunctionDefinition) {
			final BuiltInFunctionDefinition builtInDefinition = (BuiltInFunctionDefinition) definition;
			if (!builtInDefinition.getRuntimeClassName().isPresent()) {
				return definition;
			}
			final UserDefinedFunction function = FUNCTION_CACHE.get(builtInDefinition.getRuntimeClassName().get());
			if (function instanceof InternalScalarFunction) {
				((InternalScalarFunction) function).setBuiltInDefinition(builtInDefinition);
			}
		}
		return definition;
	}

	private FunctionRuntimeResolver() {
		// no in
	}
}
