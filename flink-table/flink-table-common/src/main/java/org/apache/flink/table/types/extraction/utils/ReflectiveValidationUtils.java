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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.util.InstantiationUtil;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Utilities for validating the integrity of a {@link UserDefinedFunction} and a {@link StructuredType} class.
 */
@Internal
public final class ReflectiveValidationUtils {

	/**
	 * Checks if a class can be easily instantiated.
	 */
	public static void validateInstantiation(Class<?> clazz) {
		if (!InstantiationUtil.isPublic(clazz)) {
			throw new ValidationException(
				String.format(
					"Class '%s' is not public.",
					clazz.getName()
				)
			);
		}
		else if (!InstantiationUtil.isProperClass(clazz)) {
			throw new ValidationException(
				String.format(
					"Class '%s' is not a proper class because it is either abstract, an interface, or a primitive type.",
					clazz.getName()
				)
			);
		}
		else if (InstantiationUtil.isNonStaticInnerClass(clazz)) {
			throw new ValidationException(
				String.format(
					"Class '%s' is an inner class but not statically accessible.",
					clazz.getName()
				)
			);
		}
	}

	/**
	 * Checks if a method can be easily invoked.
	 */
	public static void validateMethodInvocation(Class<?> clazz, String methodName, boolean rejectStatic, boolean isOptional) {
		boolean found = false;
		for (Method method : clazz.getMethods()) {
			if (!method.getName().equals(methodName)) {
				continue;
			}
			found = true;
			final int modifier = method.getModifiers();
			if (!Modifier.isPublic(modifier)) {
				throw new ValidationException(
					String.format(
						"Method '%s' of class '%s' is not public.",
						method.getName(),
						clazz.getName()
					)
				);
			}
			if (Modifier.isAbstract(modifier)) {
				throw new ValidationException(
					String.format(
						"Method '%s' of class '%s' must not be abstract.",
						method.getName(),
						clazz.getName()
					)
				);
			}
			if (rejectStatic && Modifier.isStatic(modifier)) {
				throw new ValidationException(
					String.format(
						"Method '%s' of class '%s' must not be static.",
						method.getName(),
						clazz.getName()
					)
				);
			}
		}
		if (!found && !isOptional) {
			throw new ValidationException(
				String.format(
					"Class '%s' does not implement a method named '%s'.",
					clazz.getName(),
					methodName
				)
			);
		}
	}

	private ReflectiveValidationUtils() {
		// no instantiation
	}
}
