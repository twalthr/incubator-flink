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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A version-agnostic Kafka {@link ScanTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and override
 * {@link #createKafkaConsumer(List, Properties, KafkaDeserializationSchema)}} and
 * {@link #createKafkaConsumer(Pattern, Properties, KafkaDeserializationSchema)}}.
 */
@Internal
public abstract class KafkaDynamicSourceBase implements ScanTableSource, SupportsReadingMetadata {

	// --------------------------------------------------------------------------------------------
	// Mutable attributes
	// --------------------------------------------------------------------------------------------

	/** Data type that describes the final output of the source. */
	protected DataType producedDataType;

	/** Metadata that is appended at the end of a physical source row. */
	protected MetadataSupplier[] metadataSuppliers;

	// --------------------------------------------------------------------------------------------
	// Format attributes
	// --------------------------------------------------------------------------------------------

	/** Scan format for decoding records from Kafka. */
	protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

	/** Data type to configure the format. */
	protected final DataType physicalDataType;

	// --------------------------------------------------------------------------------------------
	// Kafka-specific attributes
	// --------------------------------------------------------------------------------------------

	/** The Kafka topics to consume. */
	protected final List<String> topics;

	/** The Kafka topic pattern to consume. */
	protected final Pattern topicPattern;

	/** Properties for the Kafka consumer. */
	protected final Properties properties;

	/** The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
	protected final StartupMode startupMode;

	/** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}. */
	protected final Map<KafkaTopicPartition, Long> specificStartupOffsets;

	/** The start timestamp to locate partition offsets; only relevant when startup mode is {@link StartupMode#TIMESTAMP}.*/
	protected final long startupTimestampMillis;

	protected KafkaDynamicSourceBase(
			DataType physicalDataType,
			@Nullable List<String> topics,
			@Nullable Pattern topicPattern,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis) {
		this.physicalDataType = Preconditions.checkNotNull(physicalDataType, "Physical data type must not be null.");
		this.producedDataType = physicalDataType;
		Preconditions.checkArgument((topics != null && topicPattern == null) ||
				(topics == null && topicPattern != null),
			"Either Topic or Topic Pattern must be set for source.");
		this.topics = topics;
		this.topicPattern = topicPattern;
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.decodingFormat = Preconditions.checkNotNull(
			decodingFormat, "Decoding format must not be null.");
		this.startupMode = Preconditions.checkNotNull(startupMode, "Startup mode must not be null.");
		this.specificStartupOffsets = Preconditions.checkNotNull(
			specificStartupOffsets, "Specific offsets must not be null.");
		this.startupTimestampMillis = startupTimestampMillis;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return this.decodingFormat.getChangelogMode();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		DeserializationSchema<RowData> deserializationSchema =
			decodingFormat.createRuntimeDecoder(runtimeProviderContext, physicalDataType);
		// Version-specific Kafka consumer
		FlinkKafkaConsumerBase<RowData> kafkaConsumer = getKafkaConsumer(deserializationSchema);
		return SourceFunctionProvider.of(kafkaConsumer, false);
	}

	@Override
	public Map<String, DataType> listReadableMetadata() {
		final Map<String, DataType> metadataMap = new LinkedHashMap<>();
		Stream.of(Metadata.values()).forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
		return metadataMap;
	}

	@Override
	public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
		this.metadataSuppliers = metadataKeys.stream()
			.map(Metadata::valueOf)
			.map(m -> m.supplier)
			.toArray(MetadataSupplier[]::new);
		this.producedDataType = producedDataType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final KafkaDynamicSourceBase that = (KafkaDynamicSourceBase) o;
		return Objects.equals(producedDataType, that.producedDataType) &&
			Arrays.equals(metadataSuppliers, that.metadataSuppliers) &&
			Objects.equals(physicalDataType, that.physicalDataType) &&
			Objects.equals(topics, that.topics) &&
			Objects.equals(topicPattern, that.topicPattern) &&
			Objects.equals(properties, that.properties) &&
			Objects.equals(decodingFormat, that.decodingFormat) &&
			startupMode == that.startupMode &&
			Objects.equals(specificStartupOffsets, that.specificStartupOffsets) &&
			startupTimestampMillis == that.startupTimestampMillis;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			producedDataType,
			metadataSuppliers,
			physicalDataType,
			topics,
			topicPattern,
			properties,
			decodingFormat,
			startupMode,
			specificStartupOffsets,
			startupTimestampMillis);
	}

	// --------------------------------------------------------------------------------------------
	// Abstract methods for subclasses
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a version-specific Kafka consumer.
	 *
	 * @param topics                Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected abstract FlinkKafkaConsumerBase<RowData> createKafkaConsumer(
			List<String> topics,
			Properties properties,
			KafkaDeserializationSchema<RowData> deserializationSchema);

	/**
	 * Creates a version-specific Kafka consumer.
	 *
	 * @param topicPattern          afka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected abstract FlinkKafkaConsumerBase<RowData> createKafkaConsumer(
			Pattern topicPattern,
			Properties properties,
			KafkaDeserializationSchema<RowData> deserializationSchema);

	// --------------------------------------------------------------------------------------------
	// Metadata attributes
	// --------------------------------------------------------------------------------------------

	public enum Metadata {
		TOPIC(
			"topic",
			DataTypes.STRING(),
			record -> StringData.fromString(record.topic())
		),

		PARTITION(
			"partition",
			DataTypes.INT(),
			ConsumerRecord::partition
		),

		HEADERS(
			"headers",
			DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()),
			record -> {
				final Map<StringData, byte[]> map = new HashMap<>();
				for (Header header : record.headers()) {
					map.put(StringData.fromString(header.key()), header.value());
				}
				return new GenericMapData(map);
			}
		),

		LEADER_EPOCH(
			"leader-epoch",
			DataTypes.INT(),
			ConsumerRecord::leaderEpoch
		),

		OFFSET(
			"offset",
			DataTypes.BIGINT(),
			ConsumerRecord::offset),


		TIMESTAMP(
			"timestamp",
			DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
			record -> TimestampData.fromEpochMillis(record.timestamp())),

		TIMESTAMP_TYPE(
			"timestamp-type",
			DataTypes.STRING(),
			record -> StringData.fromString(record.timestampType().toString())
		);

		public final String key;

		public final DataType dataType;

		public final MetadataSupplier supplier;

		Metadata(String key, DataType dataType, MetadataSupplier supplier) {
			this.key = key;
			this.dataType = dataType;
			this.supplier = supplier;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns a version-specific Kafka consumer with the start position configured.
	 * 
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected FlinkKafkaConsumerBase<RowData> getKafkaConsumer(DeserializationSchema<RowData> deserializationSchema) {
		final MetadataKafkaDeserializationSchema kafkaSchema = new MetadataKafkaDeserializationSchema(deserializationSchema, metadataSuppliers, )

		FlinkKafkaConsumerBase<RowData> kafkaConsumer = topics != null ?
			createKafkaConsumer(topics, properties, kafkaSchema) :
			createKafkaConsumer(topicPattern, properties, kafkaSchema);

		switch (startupMode) {
			case EARLIEST:
				kafkaConsumer.setStartFromEarliest();
				break;
			case LATEST:
				kafkaConsumer.setStartFromLatest();
				break;
			case GROUP_OFFSETS:
				kafkaConsumer.setStartFromGroupOffsets();
				break;
			case SPECIFIC_OFFSETS:
				kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);
				break;
			case TIMESTAMP:
				kafkaConsumer.setStartFromTimestamp(startupTimestampMillis);
				break;
			}
		kafkaConsumer.setCommitOffsetsOnCheckpoints(properties.getProperty("group.id") != null);
		return kafkaConsumer;
	}

	// --------------------------------------------------------------------------------------------

	private static class MetadataKafkaDeserializationSchema implements KafkaDeserializationSchema<RowData> {

		private final DeserializationSchema<RowData> valueDeserializer;

		private final MetadataAppendingCollector metadataAppendingCollector;

		private final TypeInformation<RowData> producedTypeInfo;

		MetadataKafkaDeserializationSchema(
				DeserializationSchema<RowData> valueDeserializer,
				MetadataSupplier[] suppliers,
				TypeInformation<RowData> producedTypeInfo) {
			this.valueDeserializer = valueDeserializer;
			this.metadataAppendingCollector = new MetadataAppendingCollector(suppliers);
			this.producedTypeInfo = producedTypeInfo;
		}

		@Override
		public boolean isEndOfStream(RowData nextElement) {
			return false;
		}

		@Override
		public RowData deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
			throw new IllegalStateException("A collector is required for deserializing.");
		}

		@Override
		public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector) throws Exception {
			metadataAppendingCollector.inputRecord = record;
			metadataAppendingCollector.outputCollector = collector;
			valueDeserializer.deserialize(record.value(), metadataAppendingCollector);
		}

		@Override
		public TypeInformation<RowData> getProducedType() {
			return producedTypeInfo;
		}
	}

	// --------------------------------------------------------------------------------------------

	private static final class MetadataAppendingCollector implements Collector<RowData>, Serializable {

		private final MetadataSupplier[] suppliers;

		private transient ConsumerRecord<?, ?> inputRecord;

		private transient Collector<RowData> outputCollector;

		MetadataAppendingCollector(MetadataSupplier[] suppliers) {
			this.suppliers = suppliers;
		}

		@Override
		public void collect(RowData physicalRow) {
			final int metadataArity = suppliers.length;
			// shortcut if no metadata is required
			if (metadataArity == 0) {
				outputCollector.collect(physicalRow);
			}

			final GenericRowData genericPhysicalRow = (GenericRowData) physicalRow;
			final int physicalArity = physicalRow.getArity();

			final GenericRowData outputRow = new GenericRowData(
				physicalRow.getRowKind(),
				physicalArity + metadataArity);

			for (int i = 0; i < physicalArity; i++) {
				outputRow.setField(i, genericPhysicalRow.getField(i));
			}

			for (int i = 0; i < metadataArity; i++) {
				outputRow.setField(i + physicalArity, suppliers[i].get(inputRecord));
			}

			outputCollector.collect(outputRow);
		}

		@Override
		public void close() {
			// nothing to do
		}
	}

	// --------------------------------------------------------------------------------------------

	interface MetadataSupplier extends Serializable {
		Object get(ConsumerRecord<?, ?> record);
	}
}
