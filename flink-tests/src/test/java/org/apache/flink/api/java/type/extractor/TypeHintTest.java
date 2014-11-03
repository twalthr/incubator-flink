package org.apache.flink.api.java.type.extractor;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class TypeHintTest {
	
	public static <T, O> MapFunction<T,O> hint(MapFunction<T,O> function, String information) {
		
		return function;
	}
	
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		env.fromElements(1, 2, 3)
		.map((i) -> new Tuple2<String,String>()).returns("Tuple2<String,String>")
		.print();
		
		env.execute();
	}

}
