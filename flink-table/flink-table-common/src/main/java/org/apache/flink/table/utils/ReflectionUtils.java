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

package org.apache.flink.table.utils;

import org.apache.flink.table.api.TableException;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Reflections utilities.
 */
public class ReflectionUtils {

	public static boolean isComparable(Class<?> clazz) {
		return clazz.isPrimitive() || Comparable.class.isAssignableFrom(clazz);
	}

	public static boolean hasEquals(Class<?> clazz) {
		try {
			return clazz.isPrimitive() ||
				!clazz.getMethod("equals", Object.class).getDeclaringClass().equals(Object.class);
		} catch (NoSuchMethodException e) {
			throw new TableException("This should not happen.", e);
		}
	}

	public static @Nullable List<Class<?>> getAllInterfaces(final Class<?> clazz) {
		if (clazz == null) {
			return null;
		}

		final LinkedHashSet<Class<?>> interfacesFound = new LinkedHashSet<>();
		getAllInterfaces(clazz, interfacesFound);

		return new ArrayList<>(interfacesFound);
	}

	private static void getAllInterfaces(Class<?> clazz, final HashSet<Class<?>> interfacesFound) {
		while (clazz != null) {
			final Class<?>[] interfaces = clazz.getInterfaces();

			for (final Class<?> i : interfaces) {
				if (interfacesFound.add(i)) {
					getAllInterfaces(i, interfacesFound);
				}
			}

			clazz = clazz.getSuperclass();
		}
	}

	public static @Nullable Field getDeclaredField(Class<?> clazz, String name) {
		while (clazz != null) {
			for (Field f : clazz.getDeclaredFields()) {
				if (f.getName().equals(name)) {
					return f;
				}
			}
			clazz = clazz.getSuperclass();
		}
		return null;
	}

	private ReflectionUtils() {
		// do not instantiate
	}
}
