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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.typeutils.DataViewUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.Map;

/**
 * built-in Max with retraction aggregate function.
 */
public abstract class MaxWithRetractAggFunction<T extends Comparable<T>>
		extends InternalAggregateFunction<T, MaxWithRetractAggFunction.MaxWithRetractAccumulator<T>> {

	private static final long serialVersionUID = -5860934997657147836L;

	/** The initial accumulator for Max with retraction aggregate function. */
	public static class MaxWithRetractAccumulator<T> {
		public T max;
		public Long mapSize;
		public MapView<T, Long> map;
	}

	protected abstract DataType getValueDataType();

	@Override
	public final DataType[] getInputDataTypes() {
		return new DataType[]{getValueDataType()};
	}

	@Override
	public final DataType getAccumulatorDataType() {
		final DataType valueDataType = getValueDataType();
		return DataTypeUtils.newStructuredDataType(
			MaxWithRetractAccumulator.class,
			DataTypes.FIELD("max", valueDataType),
			DataTypes.FIELD("mapSize", DataTypes.BIGINT()),
			DataTypes.FIELD("map", DataViewUtils.newMapView(valueDataType, DataTypes.BIGINT())));
	}

	@Override
	public final DataType getOutputDataType() {
		return getValueDataType();
	}

	@Override
	public MaxWithRetractAccumulator<T> createAccumulator() {
		MaxWithRetractAccumulator<T> acc = new MaxWithRetractAccumulator<>();
		acc.max = null; // max
		acc.mapSize = 0L;
		// store the count for each value
		acc.map = new MapView<>();
		return acc;
	}

	public void accumulate(MaxWithRetractAccumulator<T> acc, T v) throws Exception {
		if (v != null) {
			if (acc.mapSize == 0L || acc.max.compareTo(v) < 0) {
				acc.max = v;
			}

			Long count = acc.map.get(v);
			if (count == null) {
				count = 0L;
			}
			count += 1L;
			if (count == 0) {
				// remove it when count is increased from -1 to 0
				acc.map.remove(v);
			} else {
				// store it when count is NOT zero
				acc.map.put(v, count);
			}
			if (count == 1L) {
				// previous count is zero, this is the first time to see the key
				acc.mapSize += 1;
			}
		}
	}

	public void retract(MaxWithRetractAccumulator<T> acc, T v) throws Exception {
		if (v != null) {
			Long count = acc.map.get(v);
			if (count == null) {
				count = 0L;
			}
			count -= 1;
			if (count == 0) {
				// remove it when count is decreased from 1 to 0
				acc.map.remove(v);
				acc.mapSize -= 1L;

				//if the total count is 0, we could just simply set the f0(max) to the initial value
				if (acc.mapSize == 0) {
					acc.max = null;
					return;
				}
				//if v is the current max value, we have to iterate the map to find the 2nd biggest
				// value to replace v as the max value
				if (v.equals(acc.max)) {
					updateMax(acc);
				}
			} else {
				// store it when count is NOT zero
				acc.map.put(v, count);
				// we do not take negative number account into mapSize
			}
		}
	}

	private void updateMax(MaxWithRetractAccumulator<T> acc) throws Exception {
		boolean hasMax = false;
		for (T key : acc.map.keys()) {
			if (!hasMax || acc.max.compareTo(key) < 0) {
				acc.max = key;
				hasMax = true;
			}
		}
		// The behavior of deleting expired data in the state backend is uncertain.
		// so `mapSize` data may exist, while `map` data may have been deleted
		// when both of them are expired.
		if (!hasMax) {
			acc.mapSize = 0L;
			// we should also override max value, because it may have an old value.
			acc.max = null;
		}
	}

	public void merge(MaxWithRetractAccumulator<T> acc, Iterable<MaxWithRetractAccumulator<T>> its) throws Exception {
		boolean needUpdateMax = false;
		for (MaxWithRetractAccumulator<T> a : its) {
			// set max element
			if (acc.mapSize == 0 || (a.mapSize > 0 && a.max != null && acc.max.compareTo(a.max) < 0)) {
				acc.max = a.max;
			}
			// merge the count for each key
			for (Map.Entry<T, Long> entry : a.map.entries()) {
				T key = entry.getKey();
				Long otherCount = entry.getValue(); // non-null
				Long thisCount = acc.map.get(key);
				if (thisCount == null) {
					thisCount = 0L;
				}
				long mergedCount = otherCount + thisCount;
				if (mergedCount == 0) {
					// remove it when count is increased from -1 to 0
					acc.map.remove(key);
					if (thisCount > 0) {
						// origin is > 0, and retract to 0
						acc.mapSize -= 1;
						if (key.equals(acc.max)) {
							needUpdateMax = true;
						}
					}
				} else if (mergedCount < 0) {
					acc.map.put(key, mergedCount);
					if (thisCount > 0) {
						// origin is > 0, and retract to < 0
						acc.mapSize -= 1;
						if (key.equals(acc.max)) {
							needUpdateMax = true;
						}
					}
				} else { // mergedCount > 0
					acc.map.put(key, mergedCount);
					if (thisCount <= 0) {
						// origin is <= 0, and accumulate to > 0
						acc.mapSize += 1;
					}
				}
			}
		}
		if (needUpdateMax) {
			updateMax(acc);
		}
	}

	public void resetAccumulator(MaxWithRetractAccumulator<T> acc) {
		acc.max = null;
		acc.mapSize = 0L;
		acc.map.clear();
	}

	@Override
	public T getValue(MaxWithRetractAccumulator<T> acc) {
		if (acc.mapSize > 0) {
			return acc.max;
		} else {
			return null;
		}
	}

	/**
	 * Built-in Byte Max with retraction aggregate function.
	 */
	public static class ByteMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Byte> {

		private static final long serialVersionUID = 7383980948808353819L;

		@Override
		protected DataType getValueDataType() {
			return DataTypes.TINYINT();
		}
	}

	/**
	 * Built-in Short Max with retraction aggregate function.
	 */
	public static class ShortMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Short> {

		private static final long serialVersionUID = 7579072678911328694L;

		@Override
		protected DataType getValueDataType() {
			return DataTypes.SMALLINT();
		}
	}

	/**
	 * Built-in Int Max with retraction aggregate function.
	 */
	public static class IntMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Integer> {

		private static final long serialVersionUID = 3833976566544263072L;

		@Override
		protected DataType getValueDataType() {
			return DataTypes.INT();
		}
	}

	/**
	 * Built-in Long Max with retraction aggregate function.
	 */
	public static class LongMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Long> {

		private static final long serialVersionUID = 8585384188523017375L;

		@Override
		protected DataType getValueDataType() {
			return DataTypes.BIGINT();
		}
	}

	/**
	 * Built-in Float Max with retraction aggregate function.
	 */
	public static class FloatMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Float> {

		private static final long serialVersionUID = -1433882434794024584L;

		@Override
		protected DataType getValueDataType() {
			return DataTypes.FLOAT();
		}
	}

	/**
	 * Built-in Double Max with retraction aggregate function.
	 */
	public static class DoubleMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Double> {

		private static final long serialVersionUID = -1525221057708740308L;

		@Override
		protected DataType getValueDataType() {
			return DataTypes.DOUBLE();
		}
	}

	/**
	 * Built-in Boolean Max with retraction aggregate function.
	 */
	public static class BooleanMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Boolean> {

		private static final long serialVersionUID = -8408715018822625309L;

		@Override
		protected DataType getValueDataType() {
			return DataTypes.BOOLEAN();
		}
	}

	/**
	 * Built-in Big Decimal Max with retraction aggregate function.
	 */
	public static class DecimalMaxWithRetractAggFunction extends MaxWithRetractAggFunction<DecimalData> {

		private static final long serialVersionUID = 5301860581297042635L;

		private final int precision;

		private final int scale;

		public DecimalMaxWithRetractAggFunction(int precision, int scale) {
			this.precision = precision;
			this.scale = scale;
		}

		public void accumulate(MaxWithRetractAccumulator<DecimalData> acc, DecimalData value) throws Exception {
			super.accumulate(acc, value);
		}

		public void retract(MaxWithRetractAccumulator<DecimalData> acc, DecimalData value) throws Exception {
			super.retract(acc, value);
		}

		@Override
		protected DataType getValueDataType() {
			return DataTypes.DECIMAL(precision, scale).bridgedTo(DecimalData.class);
		}
	}

	/**
	 * Built-in String Max with retraction aggregate function.
	 */
	public static class StringMaxWithRetractAggFunction extends MaxWithRetractAggFunction<StringData> {

		private static final long serialVersionUID = 787528574867514796L;

		public void accumulate(MaxWithRetractAccumulator<StringData> acc, StringData value) throws Exception {
			super.accumulate(acc, value);
		}

		public void retract(MaxWithRetractAccumulator<StringData> acc, StringData value) throws Exception {
			super.retract(acc, value);
		}

		@Override
		protected DataType getValueDataType() {
			return DataTypes.STRING().bridgedTo(StringData.class);
		}
	}

	/**
	 * Built-in Timestamp Max with retraction aggregate function.
	 */
	public static class TimestampMaxWithRetractAggFunction extends MaxWithRetractAggFunction<TimestampData> {

		private static final long serialVersionUID = -7096481949093142944L;

		private final int precision;

		public TimestampMaxWithRetractAggFunction(int precision) {
			this.precision = precision;
		}

		public void accumulate(MaxWithRetractAccumulator<TimestampData> acc, TimestampData value) throws Exception {
			super.accumulate(acc, value);
		}

		public void retract(MaxWithRetractAccumulator<TimestampData> acc, TimestampData value) throws Exception {
			super.retract(acc, value);
		}

		@Override
		protected DataType getValueDataType() {
			return DataTypes.TIMESTAMP(precision).bridgedTo(TimestampData.class);
		}
	}

	/**
	 * Built-in Date Max with retraction aggregate function.
	 */
	public static class DateMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Integer> {

		private static final long serialVersionUID = 7452698503075473023L;

		@Override
		protected DataType getValueDataType() {
			return DataTypes.DATE().bridgedTo(Integer.class);
		}
	}

	/**
	 * Built-in Time Max with retraction aggregate function.
	 */
	public static class TimeMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Integer> {

		private static final long serialVersionUID = 3578216747876121493L;

		@Override
		protected DataType getValueDataType() {
			return DataTypes.TIME(3).bridgedTo(Integer.class);
		}
	}
}
