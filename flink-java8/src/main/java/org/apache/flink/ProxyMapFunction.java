package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;

public class ProxyMapFunction<I, O> implements MapFunction<I, O> {

		private MapFunction<I, O> function;

		public ProxyMapFunction(MapFunction<I, O> function) {
			this.function = function;
		}

		@Override
		public O map(I value) throws Exception {
			return function.map(value);
		}
	}
