package org.apache.flink.table.types.annotations;

public @interface DataTypeHint {

	String value() default "";

	Class<?> bridgedTo() default void.class;

	boolean extract() default false;

	ExtractionContext context() default @ExtractionContext();
}
