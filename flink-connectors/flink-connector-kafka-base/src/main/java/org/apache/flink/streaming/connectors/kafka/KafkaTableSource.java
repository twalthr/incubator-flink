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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Properties;

/**
 * A version-agnostic Kafka {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #getKafkaConsumer(String, Properties, DeserializationSchema)}}.
 */
public abstract class KafkaTableSource
	implements StreamTableSource<Row>, DefinedProctimeAttribute, DefinedRowtimeAttribute {

	/** The Kafka topic to consume. */
	private final String topic;

	/** Properties for the Kafka consumer. */
	private final Properties properties;

	/** Deserialization schema to use for Kafka records. */
	private final DeserializationSchema<Row> deserializationSchema;

	/** Type information describing the result type. */
	private TypeInformation<Row> typeInfo;

	/** Name of proctime attribute. Null if not set. */
	private String procTimeAttribute = null;

	/** Name of ingestion time attribute. Null if not set. */
	private String ingestionTimeAttribute = null;

	/** Name of rowtime attribute. Null if not set. */
	private String rowTimeAttribute = null;

	/** Position of an existing rowtime field. */
	private int rowtimeFieldPos = -1;

	private AssignerWithPeriodicWatermarks<Row> timestampAssigner = null;

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @param typeInfo              Type information describing the result type.
	 */
	KafkaTableSource(
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema,
			TypeInformation<Row> typeInfo) {

		this.topic = Preconditions.checkNotNull(topic, "Topic");
		this.properties = Preconditions.checkNotNull(properties, "Properties");
		this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema, "Deserialization schema");
		this.typeInfo = Preconditions.checkNotNull(typeInfo, "Type information");
	}

	public void withProcTimeAttribute(String proctime) {
		Preconditions.checkNotNull(proctime, "Processing time attribute");
		this.procTimeAttribute = proctime;
	}

	public void withIngestionTimeAttribute(String ingestionTime) {
		Preconditions.checkNotNull(ingestionTime, "Ingestion time attribute");
		if (this.rowTimeAttribute != null) {
			throw new IllegalArgumentException(
				"You cannot specify an ingestion time attribute and a rowtime attribute.");
		}
		this.rowTimeAttribute = ingestionTime;
		this.timestampAssigner = new IngestionTimeWatermarkAssigner();
	}

	public void withAscendingRowTimeAttribute(String rowtime) {
		configureRowTimeAttribute(rowtime);

		// create ascending timestamp assigner
		this.timestampAssigner = new RowFieldWatermarkAssigner(this.rowtimeFieldPos, 1L);
	}

	public void withBoundedOutOfOrderRowtimeAttribute(String rowtime, Time delay) {
		this.withBoundedOutOfOrderRowtimeAttribute(rowtime, delay.toMilliseconds());
	}

	public void withBoundedOutOfOrderRowtimeAttribute(String rowtime, long delayMs) {
		configureRowTimeAttribute(rowtime);

		// create bounded OOO timestamp assigner
		this.timestampAssigner = new RowFieldWatermarkAssigner(this.rowtimeFieldPos, delayMs);
	}

	private void configureRowTimeAttribute(String rowtime) {
		Preconditions.checkNotNull(rowtime, "Row time attribute");

		String[] fieldNames = ((RowTypeInfo) this.getReturnType()).getFieldNames();
		TypeInformation[] fieldTypes = ((RowTypeInfo) this.getReturnType()).getFieldTypes();

		// check if the rowtime field exists and remember position
		this.rowtimeFieldPos = -1;
		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(rowtime)) {
				if (fieldTypes[i] != Types.LONG) {
					throw new IllegalArgumentException("Specified rowtime field must be of type BIGINT. " +
						"Available fields: " + fieldInfo(fieldNames, fieldTypes));
				}
				this.rowtimeFieldPos = i;
				break;
			}
		}
		if (this.rowtimeFieldPos < 0) {
			throw new IllegalArgumentException("Specified rowtime field must be present in data. " +
				"Available fields: " + fieldInfo(fieldNames, fieldTypes));
		}
		this.rowTimeAttribute = rowtime;

		// adjust result type by removing rowtime field (will be added later)
		String[] newNames = new String[fieldNames.length - 1];
		TypeInformation[] newTypes = new TypeInformation[fieldTypes.length - 1];
		for (int i = 0; i < rowtimeFieldPos; i++) {
			newNames[i] = fieldNames[i];
			newTypes[i] = fieldTypes[i];
		}
		for (int i = rowtimeFieldPos + 1; i < fieldNames.length; i++) {
			newNames[i - 1] = fieldNames[i];
			newTypes[i - 1] = fieldTypes[i];
		}
		this.typeInfo = new RowTypeInfo(newTypes, newNames);
	}

	private String fieldInfo(String[] fieldNames, TypeInformation[] fieldTypes) {
		Preconditions.checkArgument(fieldNames.length == fieldTypes.length);

		StringBuilder sb = new StringBuilder("[");
		for (int i = 0; i < fieldNames.length - 1; i++) {
			sb.append(fieldNames[i]);
			sb.append(": ");
			if (fieldTypes[i] == Types.BOOLEAN) {
				sb.append("BOOLEAN");
			} else if (fieldTypes[i] == Types.BYTE) {
				sb.append("TINYINT");
			} else if (fieldTypes[i] == Types.SHORT) {
				sb.append("SMALLINT");
			} else if (fieldTypes[i] == Types.INT) {
				sb.append("INTEGER");
			} else if (fieldTypes[i] == Types.LONG) {
				sb.append("BIGINT");
			} else if (fieldTypes[i] == Types.FLOAT) {
				sb.append("FLOAT");
			} else if (fieldTypes[i] == Types.DOUBLE) {
				sb.append("DOUBLE");
			} else if (fieldTypes[i] == Types.STRING) {
				sb.append("VARCHAR");
			} else if (fieldTypes[i] == Types.DECIMAL) {
				sb.append("DECIMAL");
			} else if (fieldTypes[i] == Types.SQL_DATE) {
				sb.append("DATE");
			} else if (fieldTypes[i] == Types.SQL_TIME) {
				sb.append("TIME");
			} else if (fieldTypes[i] == Types.SQL_TIMESTAMP) {
				sb.append("TIMESTAMP");
			} else {
				sb.append(fieldTypes[i].getTypeClass().getSimpleName());
			}
			sb.append(", ");
		}
		sb.delete(sb.length() - 2, sb.length());
		sb.append("]");
		return sb.toString();
	}

	public String getProctimeAttribute() {
		return this.procTimeAttribute;
	}

	public String getRowtimeAttribute() {
		if (rowTimeAttribute != null) {
			return this.rowTimeAttribute;
		} else {
			return this.ingestionTimeAttribute;
		}
	}

	/**
	 * NOTE: This method is for internal use only for defining a TableSource.
	 *       Do not use it in Table API programs.
	 */
	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
		// Version-specific Kafka consumer
		FlinkKafkaConsumerBase<Row> kafkaConsumer = getKafkaConsumer(topic, properties, deserializationSchema);
		DataStream<Row> stream = env.addSource(kafkaConsumer);

		if (ingestionTimeAttribute != null) {
			return stream.assignTimestampsAndWatermarks(timestampAssigner).returns(getReturnType());
		} else if (rowTimeAttribute != null && rowtimeFieldPos < 0) {
			return stream.assignTimestampsAndWatermarks(timestampAssigner).returns(getReturnType());
		} else if (rowTimeAttribute != null) {
			DataStream<Row> withWatermarks = stream.assignTimestampsAndWatermarks(timestampAssigner);
			return withWatermarks.map(new FieldRemover(this.rowtimeFieldPos)).returns(getReturnType());
		} else {
			return stream;
		}
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return typeInfo;
	}

	/**
	 * Returns the version-specific Kafka consumer.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	abstract FlinkKafkaConsumerBase<Row> getKafkaConsumer(
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema);

	/**
	 * Returns the deserialization schema.
	 *
	 * @return The deserialization schema
	 */
	protected DeserializationSchema<Row> getDeserializationSchema() {
		return deserializationSchema;
	}

	/**
	 * Assigns ingestion time timestamps and watermarks.
	 */
	public static class IngestionTimeWatermarkAssigner implements AssignerWithPeriodicWatermarks<Row> {

		private long curTime = Long.MIN_VALUE;

		@Override
		public long extractTimestamp(Row element, long previousElementTimestamp) {
			long t = System.currentTimeMillis();
			if (t > curTime) {
				curTime = t;
			}
			return curTime;
		}

		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(curTime - 1);
		}
	}

	/**
	 * Assigns an existing field as timestamp and generates bounded out-of-order watermarks.
	 */
	public static class RowFieldWatermarkAssigner implements AssignerWithPeriodicWatermarks<Row> {

		private final int timeField;
		private final long delayMs;
		private long maxTime = Long.MIN_VALUE;

		private RowFieldWatermarkAssigner(int timeField, long delayMs) {
			this.timeField = timeField;
			this.delayMs = delayMs;
		}

		@Override
		public long extractTimestamp(Row row, long previousElementTimestamp) {
			long t = (long) row.getField(timeField);
			if (t > maxTime) {
				maxTime = t;
			}
			return t;
		}

		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(maxTime - delayMs);
		}
	}

	/**
	 * Removes a field from a Row.
	 */
	public static class FieldRemover implements MapFunction<Row, Row> {

		private int fieldToRemove;

		private FieldRemover(int fieldToRemove) {
			this.fieldToRemove = fieldToRemove;
		}

		@Override
		public Row map(Row value) throws Exception {

			Row out = new Row(value.getArity());
			for (int i = 0; i < fieldToRemove; i++) {
				out.setField(i, value.getField(i));
			}
			for (int i = fieldToRemove + 1; i < value.getArity(); i++) {
				out.setField(i - 1, value.getField(i));
			}

			return out;
		}
	}

}
