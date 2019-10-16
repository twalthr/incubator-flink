package org.apache.flink.table.types.annotations;

import org.apache.flink.table.functions.UserDefinedFunction;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A hint for the input, intermediate accumulator, and output types of
 * a {@link UserDefinedFunction}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Repeatable(FunctionHints.class)
public @interface FunctionHint {

	/**
	 * Adds a hint for the input types of this function.
	 */
	DataTypeHint[] input() default @DataTypeHint();

	/**
	 * Adds a hint for the accumulator type of this function.
	 */
	DataTypeHint accumulator() default @DataTypeHint();

	/**
	 * Adds a hint for the output type of this function.
	 */
	DataTypeHint output() default @DataTypeHint();
}
