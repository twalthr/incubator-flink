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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataType} serialization and deserialization. */
@RunWith(Parameterized.class)
public class DataTypeJsonSerdeTest {

    @Parameter public DataType dataType;

    @Test
    public void testDataTypeSerde() throws IOException {
        final ObjectMapper mapper = configuredObjectMapper();
        final String json = toJson(mapper, dataType);
        final DataType actual = toDataType(mapper, json);

        assertThat(actual).isEqualTo(dataType);
    }

    @Parameters(name = "{0}")
    public static List<DataType> testData() {
        return Arrays.asList(
                DataTypes.INT(),
                DataTypes.INT().notNull().bridgedTo(int.class),
                DataTypes.TIMESTAMP_LTZ(3).toInternal(),
                DataTypes.TIMESTAMP_LTZ(9).bridgedTo(long.class));
    }

    // --------------------------------------------------------------------------------------------
    // Shared utilities
    // --------------------------------------------------------------------------------------------

    static ObjectMapper configuredObjectMapper() {
        return configuredObjectMapper(
                CatalogManagerMocks.createEmptyCatalogManager(), TableConfig.getDefault());
    }

    static ObjectMapper configuredObjectMapper(
            CatalogManager catalogManager, TableConfig tableConfig) {
        final SerdeContext serdeCtx =
                new SerdeContext(
                        new FlinkContextImpl(
                                false,
                                tableConfig,
                                new ModuleManager(),
                                null,
                                catalogManager,
                                null),
                        Thread.currentThread().getContextClassLoader(),
                        FlinkTypeFactory.INSTANCE(),
                        FlinkSqlOperatorTable.instance());
        final ObjectMapper mapper = JsonSerdeUtil.createObjectMapper(serdeCtx);

        final SimpleModule module = new SimpleModule();
        module.addSerializer(new DataTypeJsonSerializer());
        module.addSerializer(new LogicalTypeJsonSerializer());
        module.addSerializer(new ObjectIdentifierJsonSerializer());
        module.addDeserializer(DataType.class, new DataTypeJsonDeserializer());
        module.addDeserializer(LogicalType.class, new LogicalTypeJsonDeserializer());
        mapper.registerModule(module);

        return mapper;
    }

    static String toJson(ObjectMapper mapper, Object dataType) {
        final StringWriter writer = new StringWriter(100);
        try (JsonGenerator gen = mapper.getFactory().createGenerator(writer)) {
            gen.writeObject(dataType);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return writer.toString();
    }

    static DataType toDataType(ObjectMapper mapper, String json) {
        try {
            return mapper.readValue(json, DataType.class);
        } catch (JsonProcessingException e) {
            throw new AssertionError(e);
        }
    }
}
