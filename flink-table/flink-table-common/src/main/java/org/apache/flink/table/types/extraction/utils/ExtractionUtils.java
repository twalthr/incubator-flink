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

import org.apache.flink.table.api.ValidationException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Utilities for performing reflection tasks.
 */
public final class ExtractionUtils {

	/**
	 * Collects all annotations of the given type defined in the current class or superclasses. Duplicates
	 * are ignored.
	 */
	public static <T extends Annotation> Set<T> collectAnnotationsOfClass(
			Class<T> annotation,
			Class<?> annotatedClass) {
		final Set<T> collectedAnnotations = new HashSet<>();
		Class<?> currentClass = annotatedClass;
		while (currentClass != null) {
			collectedAnnotations.addAll(Arrays.asList(currentClass.getAnnotationsByType(annotation)));
			currentClass = currentClass.getSuperclass();
		}
		return collectedAnnotations;
	}

	/**
	 * Collects all annotations of the given type defined in the given method. Duplicates are ignored.
	 */
	public static <T extends Annotation> Set<T> collectAnnotationsOfMethod(
			Class<T> annotation,
			Method annotatedMethod) {
		return new HashSet<>(Arrays.asList(annotatedMethod.getAnnotationsByType(annotation)));
	}

	/**
	 * Helper method for creating consistent exceptions during extraction.
	 */
	public static ValidationException extractionError(String message, Object... args) {
		return extractionError(null, message, args);
	}

	/**
	 * Helper method for creating consistent exceptions during extraction.
	 */
	public static ValidationException extractionError(Throwable cause, String message, Object... args) {
		return new ValidationException(
			String.format(
				message,
				args
			),
			cause
		);
	}

	private ExtractionUtils() {
		// no instantiation
	}
}
