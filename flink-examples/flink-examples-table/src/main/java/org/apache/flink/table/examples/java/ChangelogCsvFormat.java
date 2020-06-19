package org.apache.flink.table.examples.java;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

public class ChangelogCsvFormat implements DecodingFormat<DeserializationSchema<RowData>> {

	private final String columnDelimiter;

	public ChangelogCsvFormat(String columnDelimiter) {
		this.columnDelimiter = columnDelimiter;
	}

	@Override
	@SuppressWarnings("unchecked")
	public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
		// create type information for the DeserializationSchema
		final TypeInformation<RowData> producedTypeInfo = (TypeInformation<RowData>) context.createTypeInformation(producedDataType);

		// DeserializationSchema will not work on internal data structures
		// create converter for conversion at the end
		final DataStructureConverter converter = context.createDataStructureConverter(producedDataType);

		// use logical type during runtime for parsing
		final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();

		return new ChangelogCsvDeserializer(parsingTypes, converter, producedTypeInfo, columnDelimiter);
	}

	@Override
	public ChangelogMode getChangelogMode() {
		// define that this format can produce INSERT and DELETE rows
		return ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.DELETE)
			.build();
	}
}
