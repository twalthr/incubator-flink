package org.apache.flink.table.types.annotations;

public @interface DataTypeHints {

	DataTypeHint[] value() default @DataTypeHint();
}
