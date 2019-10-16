package org.apache.flink.table.types.annotations;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A hint for extracting a {@link DataType}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
public @interface DataTypeHint {

	/**
	 * A string describing a {@link LogicalType}. No value by default.
	 *
	 * @see LogicalTypeParser
	 */
	String value() default "";

	/**
	 * Adds a hint that data should be represented using the given class when entering or leaving
	 * the table ecosystem.
	 *
	 * <p>A supported conversion class depends on the logical type and its nullability property.
	 *
	 * <p>Please see the implementation of {@link LogicalType#supportsInputConversion(Class)},
	 * {@link LogicalType#supportsOutputConversion(Class)}, or the documentation for more information
	 * about supported conversions.
	 */
	Class<?> bridgedTo() default void.class;

	// ....
}
