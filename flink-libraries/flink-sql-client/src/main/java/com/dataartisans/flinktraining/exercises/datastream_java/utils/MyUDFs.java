/*
 * Copyright 2017 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.exercises.datastream_java.utils;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

@SuppressWarnings("ALL")
public class MyUDFs {

	/**
	 * Accumulator for WeightedAvg.
	 */
	public static class WeightedAvgAccum {
		public long sum = 0;
		public int count = 0;
	}

	/**
	 * Weighted Average user-defined aggregate function.
	 */
	public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccum> {

		// create and init the accumulator
		@Override
		public WeightedAvgAccum createAccumulator() { // MANDATORY
			return new WeightedAvgAccum();
		}

		// process input values and update provided accumulator
		public void accumulate(WeightedAvgAccum acc, long iValue, int iWeight) { // MANDATORY
			acc.sum += iValue * iWeight;
			acc.count += iWeight;
		}

		// retract input values from accumulator e.g. for over aggregates
		public void retract(WeightedAvgAccum acc, long iValue, int iWeight) { // OPTIONAL
			acc.sum -= iValue * iWeight;
			acc.count -= iWeight;
		}

		// merge a group of accumulators
		// e.g. for session group windows and dataset grouping aggregates
		public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) { // OPTIONAL
			Iterator<WeightedAvgAccum> iter = it.iterator();
			while (iter.hasNext()) {
				WeightedAvgAccum a = iter.next();
				acc.count += a.count;
				acc.sum += a.sum;
			}
		}

		// resets an accumulator e.g. for dataset grouping aggregate
		public void resetAccumulator(WeightedAvgAccum acc) { // OPTIONAL
			acc.count = 0;
			acc.sum = 0L;
		}

		@Override
		public Long getValue(WeightedAvgAccum acc) { // MANDATORY
			if (acc.count == 0) {
				return null;
			} else {
				return acc.sum / acc.count;
			}
		}
	}

}
