package org.apache.flink.table.types.annotations;

public @interface ExtractionContext {

	int version() default -1;

	boolean allowAny() default false;

	String[] anyPatterns() default {};

	int decimalPrecision() default -1;

	int decimalScale() default -1;
}
