package org.apache.flink.table.types.annotations;

public @interface FunctionHint {

	DataTypeHint[] input() default @DataTypeHint();

	DataTypeHint accumulator() default @DataTypeHint();

	DataTypeHint output() default @DataTypeHint();
}
