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

package org.apache.flink.table.types.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.asm6.org.objectweb.asm.ClassReader;
import org.apache.flink.shaded.asm6.org.objectweb.asm.ClassVisitor;
import org.apache.flink.shaded.asm6.org.objectweb.asm.Label;
import org.apache.flink.shaded.asm6.org.objectweb.asm.MethodVisitor;
import org.apache.flink.shaded.asm6.org.objectweb.asm.Opcodes;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.StructuredType;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.shaded.asm6.org.objectweb.asm.Type.getConstructorDescriptor;

/**
 * Reflection-based utility that analyzes a given {@link java.lang.reflect.Type} to extract a (possibly
 * nested) {@link DataType} from it.
 */
@Internal
public final class ReflectiveDataTypeConverter {

	private static final int CURRENT_VERSION = 1;

	private static final List<String> ALWAYS_ANY_TYPE_PREFIXES = Arrays.asList("java", "scala");

	private final int version;

	private final boolean allowAny;

	private ReflectiveDataTypeConverter(int version, boolean allowAny) {
		this.version = version;
		this.allowAny = allowAny;
	}

	public static Builder newInstance() {
		return new Builder();
	}

	public DataType extractDataType(Type type, int genericPos) {
		if (!(type instanceof ParameterizedType) ||
				((ParameterizedType) type).getActualTypeArguments().length <= genericPos) {
			throw extractionError(
				"Invalid parameterized type %s for extracting a data type at position %d.",
				type.toString(),
				genericPos);
		}
		return extractDataType(((ParameterizedType) type).getActualTypeArguments()[genericPos]);
	}

	public DataType extractDataType(Type type) {
		try {
			return extractDataTypeInternal(type);
		} catch (Throwable t) {
			throw extractionError(
				t,
				"Could not extract a data type from %s. Please pass the required data type manually.",
				type.toString());
		}
	}

	// --------------------------------------------------------------------------------------------

	private DataType extractDataTypeInternal(Type type) {
		// use annotation-based extraction

		// use class-based extraction
		if (type instanceof Class) {
			final Optional<DataType> foundType = ClassDataTypeConverter.extractDataType((Class) type);
			if (foundType.isPresent()) {
				return foundType.get();
			}
		}

		// TODO use reflection-based extraction for MAP and ARRAY and primitives

		return null;
	}

	private DataType extractStructuredType(Type type) {
		final List<Type> hierarchy = collectTypeHierarchy(type);

		extractStructuredTypeHierarchy(hierarchy);

		// TODO HIER WEITER MACHEN!!!

		return null;
	}

	/**
	 * Creates a {@link StructuredType} if all types in the hierarchy of types meet the
	 * requirements of a {@link StructuredType}.
	 */
	private static DataType extractStructuredTypeHierarchy(List<Type> hierarchy) {

//		final StructuredType.Builder builder = StructuredType.newInstance();

		// traverse hierarchy from supertype to subtype and collect fields while traversing
		final List<Field> superFields = new ArrayList<>();
		for (int i = hierarchy.size() - 1; i >= 0; i++) {
			final Type type = hierarchy.get(i);
			extractStructuredType(type, superFields);
		}

		// most concrete type must be instantiable
		final Class<?> concreteType = asClassType(hierarchy.get(0));
		if (Modifier.isAbstract(concreteType.getModifiers())) {
			throw extractionError(
				"Most concrete class in hierarchy %s must not be abstract.",
				concreteType.getName());
		}

		return null;
	}

	/**
	 * Validates if a type qualifies as a {@link StructuredType}.
	 *
	 * <p>Adds its own fields to the given list of fields.
	 */
	private static void extractStructuredType(Type type, List<Field> superFields) {
		final Class<?> clazz = asClassType(type);

		validateStructuredClass(clazz);

		final List<Field> fields = collectStructuredDeclaredFields(clazz);
		
		validateStructuredFields(clazz, superFields, fields);

		superFields.addAll(fields);
	}

	/**
	 * Checks whether field requirements for a {@link StructuredType} are met.
	 */
	private static void validateStructuredFields(Class<?> clazz, List<Field> superFields, List<Field> fields) {
		final List<Field> allFields = new ArrayList<>();
		allFields.addAll(superFields);
		allFields.addAll(fields);

		boolean requireAssigningConstructor = false;
		for (Field field : allFields) {

			validateStructuredFieldReadability(clazz, field);

			final boolean isMutable = checkStructuredFieldMutability(clazz, field);
			// not all fields are mutable, a default constructor is not enough
			if (!isMutable) {
				requireAssigningConstructor = true;
			}
		}

		validateStructuredConstructor(clazz, requireAssigningConstructor, allFields);
	}

	/**
	 * Checks whether constructor requirements for a {@link StructuredType} are met according to the
	 * given fields.
	 */
	private static void validateStructuredConstructor(
			Class<?> clazz,
			boolean requireAssigningConstructor,
			List<Field> allFields) {
		// check for assigning constructor
		if (requireAssigningConstructor && !hasAssigningConstructor(clazz, allFields)) {
			throw extractionError(
				"Class %s has immutable fields and thus requires a constructor that assigns all fields: %s",
				clazz.getName(),
				allFields.stream().map(Field::getName).collect(Collectors.joining(", ")));
		}
		// check for default constructor
		else if (!hasDefaultConstructor(clazz)) {
			throw extractionError(
				"Class %s does not specify a default constructor.",
				clazz.getName());
		}
	}

	/**
	 * Checks whether a default constructor exists.
	 */
	private static boolean hasDefaultConstructor(Class<?> clazz) {
		return Stream.of(clazz.getConstructors())
			.anyMatch((c) -> c.getParameterCount() == 0 && Modifier.isPublic(c.getModifiers()));
	}

	/**
	 * Checks whether a constructor exists that takes all of the given fields with matching (possibly
	 * primitive) type and name.
	 */
	private static boolean hasAssigningConstructor(Class<?> clazz, List<Field> fields) {
		for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
			if (Modifier.isPublic(constructor.getModifiers()) &&
					isAssigningConstructor(clazz, constructor, fields)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Checks whether the given constructor takes all of the given fields with matching (possibly
	 * primitive) type and name.
	 */
	private static boolean isAssigningConstructor(Class<?> clazz, Constructor<?> constructor, List<Field> fields) {
		final Type[] parameterTypes = constructor.getGenericParameterTypes();
		List<String> parameterNames = Stream.of(constructor.getParameters())
			.map(Parameter::getName)
			.collect(Collectors.toList());

		// by default parameter names are "arg0, arg1, arg2, ..." if compiler flag is not set
		// so we need to extract them manually if possible
		if (parameterNames.stream().allMatch(n -> n.startsWith("arg"))) {
			final ParameterExtractor extractor = new ParameterExtractor(constructor);
			getClassReader(clazz).accept(extractor, 0);

			final List<String> extractedNames = extractor.getParameterNames();
			if (extractedNames.size() == 0 || !extractedNames.get(0).equals("this")) {
				return false;
			}
			// remove "this" and additional local variables
			parameterNames = extractedNames.subList(1, Math.min(fields.size(), extractedNames.size()));
		}

		if (parameterNames.size() != fields.size()) {
			return false;
		}

		final Map<String, Type> fieldMap = fields.stream()
			.collect(Collectors.toMap(Field::getName, Field::getGenericType));

		// check that all fields are represented in the parameters of the constructor
		for (int i = 0; i < parameterNames.size(); i++) {
			final String parameterName = parameterNames.get(i);
			final Type fieldType = fieldMap.get(parameterName); // might be null
			final Type parameterType = parameterTypes[i];
			if (!boxPrimitives(parameterType).equals(fieldType)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Validates if a field is properly readable either directly or through a getter.
	 */
	private static void validateStructuredFieldReadability(Class<?> clazz, Field field) {
		final int m = field.getModifiers();

		// field is accessible
		if (Modifier.isPublic(m)) {
			return;
		}

		// field needs a getter
		if (!hasFieldGetter(clazz, field)) {
			throw extractionError(
				"Field %s of class %s is neither publicly accessible nor does it have " +
					"a corresponding getter method.",
				field.getName(),
				clazz.getName());
		}
	}

	/**
	 * Checks if a field is mutable or immutable. Returns {@code true} if the field is properly
	 * mutable. Returns {@code false} if it is properly immutable.
	 */
	private static boolean checkStructuredFieldMutability(Class<?> clazz, Field field) {
		final int m = field.getModifiers();

		// field is immutable
		if (Modifier.isFinal(m)) {
			return false;
		}
		// field is directly mutable
		if (Modifier.isPublic(m)) {
			return true;
		}
		// field has setters by which it is mutable
		if (hasFieldSetter(clazz, field)) {
			return true;
		}

		throw extractionError(
			"Field %s of class %s is mutable but is neither publicly accessible nor does it have " +
				"a corresponding setter method.",
			field.getName(),
			clazz.getName());
	}

	/**
	 * Checks for a field getters. The logic is as broad as possible to support both Java and Scala
	 * in different flavors.
	 */
	private static boolean hasFieldGetter(Class<?> clazz, Field field) {
		final String normalizedFieldName = field.getName().toUpperCase().replaceAll("_", "");

		for (Method method : clazz.getDeclaredMethods()) {

			// check visibility:
			// public implementation
			final int m = method.getModifiers();
			if (!Modifier.isPublic(m) || Modifier.isNative(m) || Modifier.isAbstract(m)) {
				continue;
			}

			// check name:
			// get<Name>()
			// is<Name>()
			// <Name>() for Scala
			final String normalizedMethodName = method.getName().toUpperCase().replaceAll("_", "");
			if (!normalizedMethodName.equals("GET" + normalizedFieldName) &&
					!normalizedMethodName.equals("IS" + normalizedFieldName) &&
					!normalizedMethodName.equals(normalizedFieldName)) {
				continue;
			}

			// check return type:
			// equal to field type
			final Type returnType = method.getGenericReturnType();
			if (returnType.equals(field.getGenericType())) {
				continue;
			}

			// check parameters:
			// no parameters
			if (method.getParameterCount() != 0) {
				continue;
			}

			// matching getter found
			return true;
		}

		// no getter found
		return false;
	}

	/**
	 * Checks for a field setters. The logic is as broad as possible to support both Java and Scala
	 * in different flavors.
	 */
	private static boolean hasFieldSetter(Class<?> clazz, Field field) {
		final String normalizedFieldName = field.getName().toUpperCase().replaceAll("_", "");

		for (Method method : clazz.getDeclaredMethods()) {

			// check visibility:
			// public implementation
			final int m = method.getModifiers();
			if (!Modifier.isPublic(m) || Modifier.isNative(m) || Modifier.isAbstract(m)) {
				continue;
			}

			// check name:
			// set<Name>(type)
			// <Name>(type)
			// <Name>_$eq(type) for Scala
			final String normalizedMethodName = method.getName().toUpperCase().replaceAll("_", "");
			if (!normalizedMethodName.equals("SET" + normalizedFieldName) &&
					!normalizedMethodName.equals(normalizedFieldName) &&
					!normalizedMethodName.equals(normalizedFieldName + "$EQ")) {
				continue;
			}

			// check return type:
			// void or the declaring class
			final Class<?> returnType = method.getReturnType();
			if (returnType != Void.TYPE && returnType != clazz) {
				continue;
			}

			// check parameters:
			// one parameter that has the same (or primitive) type of the field
			if (method.getParameterCount() != 1 ||
					!boxPrimitives(method.getGenericParameterTypes()[0]).equals(field.getGenericType())) {
				continue;
			}

			// matching setter found
			return true;
		}

		// no setter found
		return false;
	}

	/**
	 * Validates the characteristics of a class for a {@link StructuredType} such as accessibility.
	 */
	private static void validateStructuredClass(Class<?> clazz) {
		final int m = clazz.getModifiers();
		if (!Modifier.isPublic(m)) {
			throw extractionError("Class %s is not public.", clazz.getName());
		}
		if (clazz.getEnclosingClass() != null &&
				(clazz.getDeclaringClass() == null || !Modifier.isStatic(m))) {
			throw extractionError("Class %s is a not a static, globally accessible class.", clazz.getName());
		}
	}

	/**
	 * Collects the hierarchy of the given type up to {@link Object}.
	 */
	private static List<Type> collectTypeHierarchy(Type type) {
		final List<Type> hierarchy = new ArrayList<>();

		Type currentSuperType = type;
		Class<?> currentSuperClass = asClassType(type);
		while (currentSuperClass != Object.class) {
			hierarchy.add(currentSuperType);
			currentSuperType = currentSuperClass.getGenericSuperclass();
			currentSuperClass = asClassType(currentSuperType);
		}

		if (hierarchy.size() == 0) {
			throw extractionError("Only Object.class found in hierarchy.");
		}

		return hierarchy;
	}

	/**
	 * Collects field that need to serialized for a {@link StructuredType}.
	 */
	private static List<Field> collectStructuredDeclaredFields(Class<?> clazz) {
		final Field[] fields = clazz.getDeclaredFields();
		return Stream.of(fields)
			.filter(field -> {
				final int m = field.getModifiers();
				return !Modifier.isStatic(m) && !Modifier.isTransient(m);
			})
			.collect(Collectors.toList());
	}

	private static Class<?> asClassType(Type type) {
		if (type instanceof Class) {
			return (Class<?>) type;
		} else if (type instanceof ParameterizedType) {
			// this is always a class
			return (Class<?>) ((ParameterizedType) type).getRawType();
		}
		throw extractionError("Unsupported class type %s.", type);
	}

	private static IllegalStateException extractionError(Throwable t, String cause, Object... args) {
		return new IllegalStateException(
			String.format(cause, args),
			t
		);
	}

	private static IllegalStateException extractionError(String cause, Object... args) {
		return extractionError(null, cause, args);
	}

	private static ClassReader getClassReader(Class<?> cls) {
		final String className = cls.getName().replaceFirst("^.*\\.", "") + ".class";
		try {
			return new ClassReader(cls.getResourceAsStream(className));
		} catch (IOException e) {
			throw new IllegalStateException("Could not instantiate ClassReader.", e);
		}
	}

	private static final Map<Class<?>, Class<?>> primitiveWrapperMap = new HashMap<>();
	static {
		primitiveWrapperMap.put(Boolean.TYPE, Boolean.class);
		primitiveWrapperMap.put(Byte.TYPE, Byte.class);
		primitiveWrapperMap.put(Character.TYPE, Character.class);
		primitiveWrapperMap.put(Short.TYPE, Short.class);
		primitiveWrapperMap.put(Integer.TYPE, Integer.class);
		primitiveWrapperMap.put(Long.TYPE, Long.class);
		primitiveWrapperMap.put(Double.TYPE, Double.class);
		primitiveWrapperMap.put(Float.TYPE, Float.class);
	}

	private static Type boxPrimitives(Type type) {
		if (type instanceof Class && ((Class) type).isPrimitive()) {
			return primitiveWrapperMap.get(type);
		}
		return type;
	}

	/**
	 * Extracts the parameter names and descriptors from a constructor. Assuming the existence of a
	 * local variable table.
	 *
	 * <p>For example:
	 * <pre>
	 * {@code
	 * public WC(java.lang.String arg0, long arg1) { // <init> //(Ljava/lang/String;J)V
	 *   <localVar:index=0 , name=this , desc=Lorg/apache/flink/WC;, sig=null, start=L1, end=L2>
	 *   <localVar:index=1 , name=word , desc=Ljava/lang/String;, sig=null, start=L1, end=L2>
	 *   <localVar:index=2 , name=frequency , desc=J, sig=null, start=L1, end=L2>
	 *   <localVar:index=2 , name=otherLocal , desc=J, sig=null, start=L1, end=L2>
	 *   <localVar:index=2 , name=otherLocal2 , desc=J, sig=null, start=L1, end=L2>
	 * }
	 * </pre>
	 */
	private static class ParameterExtractor extends ClassVisitor {

		private final String constructorDescriptor;

		private final List<String> parameterNames = new ArrayList<>();

		public ParameterExtractor(Constructor constructor) {
			super(Opcodes.ASM6);
			constructorDescriptor = getConstructorDescriptor(constructor);
		}

		public List<String> getParameterNames() {
			return parameterNames;
		}

		@Override
		public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
			if (descriptor.equals(constructorDescriptor)) {
				return new MethodVisitor(Opcodes.ASM6) {
					@Override
					public void visitLocalVariable(String name, String descriptor, String signature, Label start, Label end, int index) {
						parameterNames.add(name);
					}
				};
			}
			return super.visitMethod(access, name, descriptor, signature, exceptions);
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Builder for creating an instance of {@link ReflectiveDataTypeConverter}.
	 */
	public static class Builder {

		private int version = ReflectiveDataTypeConverter.CURRENT_VERSION;

		private boolean allowAny = false;

		public Builder() {
			// default constructor for fluent definition
		}

		/**
		 * Logic version for future backwards compatibility. Current version by default.
		 */
		public Builder version(int version) {
			this.version = version;
			return this;
		}

		/**
		 * Defines whether {@link DataTypes#ANY(TypeInformation)} should be used for classes that
		 * cannot be mapped to any SQL-like type. Set to {@code false} by default, which means that
		 * an exception is thrown for unmapped types.
		 *
		 * <p>For example, {@link java.math.BigDecimal} cannot be mapped because the SQL standard
		 * defines that decimals have a fixed precision and scale.
		 */
		public Builder allowAny(boolean allowAny) {
			this.allowAny = allowAny;
			return this;
		}

		public ReflectiveDataTypeConverter build() {
			return new ReflectiveDataTypeConverter(version, allowAny);
		}
	}
}
