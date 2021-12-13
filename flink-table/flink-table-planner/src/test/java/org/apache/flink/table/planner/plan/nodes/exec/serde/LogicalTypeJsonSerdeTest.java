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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.typeutils.DataViewUtils;
import org.apache.flink.table.runtime.typeutils.ExternalSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.DataTypeJsonSerdeTest.configuredObjectMapper;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link LogicalType} serialization and deserialization. See also {@link
 * LogicalTypeCatalogJsonSerdeTest} for catalog-specific tests.
 */
@RunWith(Parameterized.class)
public class LogicalTypeJsonSerdeTest {

    @Parameter public LogicalType logicalType;

    @Test
    public void testLogicalTypeSerde() throws IOException {
        final ObjectMapper mapper = configuredObjectMapper();
        final String json = toJson(mapper, logicalType);
        final LogicalType actual = toLogicalType(mapper, json);

        assertThat(actual).isEqualTo(logicalType);
    }

    @Parameters(name = "{0}")
    public static List<LogicalType> testData() {
        final List<LogicalType> types =
                Arrays.asList(
                        new BooleanType(),
                        new TinyIntType(),
                        new SmallIntType(),
                        new IntType(),
                        new BigIntType(),
                        new FloatType(),
                        new DoubleType(),
                        new DecimalType(10),
                        new DecimalType(15, 5),
                        CharType.ofEmptyLiteral(),
                        new CharType(),
                        new CharType(5),
                        VarCharType.ofEmptyLiteral(),
                        new VarCharType(),
                        new VarCharType(5),
                        BinaryType.ofEmptyLiteral(),
                        new BinaryType(),
                        new BinaryType(100),
                        VarBinaryType.ofEmptyLiteral(),
                        new VarBinaryType(),
                        new VarBinaryType(100),
                        new DateType(),
                        new TimeType(),
                        new TimeType(3),
                        new TimestampType(),
                        new TimestampType(3),
                        new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                        new TimestampType(false, TimestampKind.ROWTIME, 3),
                        new ZonedTimestampType(),
                        new ZonedTimestampType(3),
                        new ZonedTimestampType(false, TimestampKind.ROWTIME, 3),
                        new LocalZonedTimestampType(),
                        new LocalZonedTimestampType(3),
                        new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                        new LocalZonedTimestampType(false, TimestampKind.ROWTIME, 3),
                        new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR),
                        new DayTimeIntervalType(
                                false, DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR, 3, 6),
                        new YearMonthIntervalType(
                                YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH),
                        new YearMonthIntervalType(
                                false, YearMonthIntervalType.YearMonthResolution.MONTH, 2),
                        new ZonedTimestampType(),
                        new LocalZonedTimestampType(),
                        new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                        new SymbolType<>(),
                        new ArrayType(new IntType(false)),
                        new ArrayType(new LocalZonedTimestampType(false, TimestampKind.ROWTIME, 3)),
                        new ArrayType(new ZonedTimestampType(false, TimestampKind.ROWTIME, 3)),
                        new ArrayType(new TimestampType()),
                        new ArrayType(CharType.ofEmptyLiteral()),
                        new ArrayType(VarCharType.ofEmptyLiteral()),
                        new ArrayType(BinaryType.ofEmptyLiteral()),
                        new ArrayType(VarBinaryType.ofEmptyLiteral()),
                        new MapType(new BigIntType(), new IntType(false)),
                        new MapType(
                                new TimestampType(false, TimestampKind.ROWTIME, 3),
                                new ZonedTimestampType()),
                        new MapType(CharType.ofEmptyLiteral(), CharType.ofEmptyLiteral()),
                        new MapType(VarCharType.ofEmptyLiteral(), VarCharType.ofEmptyLiteral()),
                        new MapType(BinaryType.ofEmptyLiteral(), BinaryType.ofEmptyLiteral()),
                        new MapType(VarBinaryType.ofEmptyLiteral(), VarBinaryType.ofEmptyLiteral()),
                        new MultisetType(new IntType(false)),
                        new MultisetType(new TimestampType()),
                        new MultisetType(new TimestampType(true, TimestampKind.ROWTIME, 3)),
                        new MultisetType(CharType.ofEmptyLiteral()),
                        new MultisetType(VarCharType.ofEmptyLiteral()),
                        new MultisetType(BinaryType.ofEmptyLiteral()),
                        new MultisetType(VarBinaryType.ofEmptyLiteral()),
                        RowType.of(new BigIntType(), new IntType(false), new VarCharType(200)),
                        RowType.of(
                                new LogicalType[] {
                                    new BigIntType(), new IntType(false), new VarCharType(200)
                                },
                                new String[] {"f1", "f2", "f3"}),
                        RowType.of(
                                new TimestampType(false, TimestampKind.ROWTIME, 3),
                                new TimestampType(false, TimestampKind.REGULAR, 3),
                                new ZonedTimestampType(false, TimestampKind.ROWTIME, 3),
                                new ZonedTimestampType(false, TimestampKind.REGULAR, 3),
                                new LocalZonedTimestampType(false, TimestampKind.ROWTIME, 3),
                                new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                                new LocalZonedTimestampType(false, TimestampKind.REGULAR, 3)),
                        RowType.of(
                                CharType.ofEmptyLiteral(),
                                VarCharType.ofEmptyLiteral(),
                                BinaryType.ofEmptyLiteral(),
                                VarBinaryType.ofEmptyLiteral()),
                        // registered structured type
                        StructuredType.newBuilder(
                                        ObjectIdentifier.of("cat", "db", "structuredType"),
                                        PojoClass.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredType.StructuredAttribute(
                                                        "f0", new IntType(true)),
                                                new StructuredType.StructuredAttribute(
                                                        "f1", new BigIntType(true)),
                                                new StructuredType.StructuredAttribute(
                                                        "f2", new VarCharType(200), "desc")))
                                .comparison(StructuredType.StructuredComparison.FULL)
                                .setFinal(false)
                                .setInstantiable(false)
                                .superType(
                                        StructuredType.newBuilder(
                                                        ObjectIdentifier.of(
                                                                "cat", "db", "structuredType2"))
                                                .attributes(
                                                        Collections.singletonList(
                                                                new StructuredType
                                                                        .StructuredAttribute(
                                                                        "f0",
                                                                        new BigIntType(false))))
                                                .build())
                                .description("description for StructuredType")
                                .build(),
                        // unregistered structured type
                        StructuredType.newBuilder(PojoClass.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredType.StructuredAttribute(
                                                        "f0", new IntType(true)),
                                                new StructuredType.StructuredAttribute(
                                                        "f1", new BigIntType(true)),
                                                new StructuredType.StructuredAttribute(
                                                        "f2", new VarCharType(200), "desc")))
                                .build(),
                        // registered distinct type
                        DistinctType.newBuilder(
                                        ObjectIdentifier.of("cat", "db", "distinctType"),
                                        new VarCharType(5))
                                .build(),
                        DistinctType.newBuilder(
                                        ObjectIdentifier.of("cat", "db", "distinctType"),
                                        new VarCharType(false, 5))
                                .build(),
                        // custom RawType
                        new RawType<>(Integer.class, IntSerializer.INSTANCE),
                        // external RawType
                        new RawType<>(
                                Row.class,
                                ExternalSerializer.of(
                                        DataTypes.ROW(DataTypes.INT(), DataTypes.STRING()))));

        final List<LogicalType> mutableTypes = new ArrayList<>(types);

        // RawType for MapView
        addRawTypesForMapView(mutableTypes, new VarCharType(100), new VarCharType(100));
        addRawTypesForMapView(mutableTypes, new VarCharType(100), new BigIntType());
        addRawTypesForMapView(mutableTypes, new BigIntType(), new VarCharType(100));
        addRawTypesForMapView(mutableTypes, new BigIntType(), new BigIntType());

        // RawType for ListView
        addRawTypesForListView(mutableTypes, new VarCharType(100));
        addRawTypesForListView(mutableTypes, new BigIntType());

        // RawType for custom MapView
        mutableTypes.add(
                DataViewUtils.adjustDataViews(
                                MapView.newMapViewDataType(
                                        DataTypes.STRING().toInternal(),
                                        DataTypes.STRING().bridgedTo(byte[].class)),
                                false)
                        .getLogicalType());

        final List<LogicalType> allTypes = new ArrayList<>();
        // consider nullable
        for (LogicalType type : mutableTypes) {
            allTypes.add(type.copy(true));
            allTypes.add(type.copy(false));
        }
        // ignore nullable for NullType
        allTypes.add(new NullType());
        return allTypes;
    }

    private static void addRawTypesForMapView(
            List<LogicalType> types, LogicalType keyType, LogicalType valueType) {
        for (boolean hasStateBackedDataViews : Arrays.asList(true, false)) {
            for (boolean keyNullable : Arrays.asList(true, false)) {
                for (boolean isInternalKeyType : Arrays.asList(true, false)) {
                    for (boolean valueNullable : Arrays.asList(true, false)) {
                        for (boolean isInternalValueType : Arrays.asList(true, false)) {
                            final DataType viewDataType =
                                    DataViewUtils.adjustDataViews(
                                            MapView.newMapViewDataType(
                                                    convertToInternalTypeIfNeeded(
                                                            DataTypes.of(keyType.copy(keyNullable)),
                                                            isInternalKeyType),
                                                    convertToInternalTypeIfNeeded(
                                                            DataTypes.of(
                                                                    valueType.copy(valueNullable)),
                                                            isInternalValueType)),
                                            hasStateBackedDataViews);
                            types.add(viewDataType.getLogicalType());
                        }
                    }
                }
            }
        }
    }

    private static void addRawTypesForListView(List<LogicalType> types, LogicalType elementType) {
        for (boolean hasStateBackedDataViews : Arrays.asList(true, false)) {
            for (boolean elementNullable : Arrays.asList(true, false)) {
                for (boolean isInternalType : Arrays.asList(true, false)) {
                    final DataType viewDataType =
                            DataViewUtils.adjustDataViews(
                                    ListView.newListViewDataType(
                                            convertToInternalTypeIfNeeded(
                                                    DataTypes.of(elementType.copy(elementNullable)),
                                                    isInternalType)),
                                    hasStateBackedDataViews);
                    types.add(viewDataType.getLogicalType());
                }
            }
        }
    }

    private static DataType convertToInternalTypeIfNeeded(
            DataType dataType, boolean isInternalType) {
        return isInternalType ? dataType.toInternal() : dataType;
    }

    /** Testing class. */
    public static class PojoClass {
        public int f0;
        public long f1;
        public String f2;
    }

    // --------------------------------------------------------------------------------------------
    // Shared utilities
    // --------------------------------------------------------------------------------------------

    static String toJson(ObjectMapper mapper, LogicalType logicalType) {
        final StringWriter writer = new StringWriter(100);
        try (JsonGenerator gen = mapper.getFactory().createGenerator(writer)) {
            gen.writeObject(logicalType);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return writer.toString();
    }

    static LogicalType toLogicalType(ObjectMapper mapper, String json) {
        try {
            return mapper.readValue(json, LogicalType.class);
        } catch (JsonProcessingException e) {
            throw new AssertionError(e);
        }
    }
}
