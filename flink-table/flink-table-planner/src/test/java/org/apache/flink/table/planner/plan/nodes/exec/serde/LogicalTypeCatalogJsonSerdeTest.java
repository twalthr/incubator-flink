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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanRestore;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.UserDefinedType;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.util.Optional;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation.ALL;
import static org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation.IDENTIFIER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.DataTypeJsonSerdeTest.configuredObjectMapper;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerdeTest.toJson;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerdeTest.toLogicalType;
import static org.apache.flink.table.utils.CatalogManagerMocks.preparedCatalogManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for restoring instances of {@link UserDefinedType} from a catalog. */
public class LogicalTypeCatalogJsonSerdeTest {

    private static final StructuredType STRUCTURED_TYPE =
            StructuredType.newBuilder(
                            ObjectIdentifier.of(
                                    CatalogManagerMocks.DEFAULT_CATALOG,
                                    CatalogManagerMocks.DEFAULT_DATABASE,
                                    "MyType"))
                    .description("My original type.")
                    .build();

    private static final StructuredType UPDATED_STRUCTURED_TYPE =
            StructuredType.newBuilder(
                            ObjectIdentifier.of(
                                    CatalogManagerMocks.DEFAULT_CATALOG,
                                    CatalogManagerMocks.DEFAULT_DATABASE,
                                    "MyType"))
                    .description("My original type with update description.")
                    .build();

    @Test
    public void testIdentifierSerde() {
        final DataTypeFactoryMock dataTypeFactoryMock = new DataTypeFactoryMock();
        final TableConfig tableConfig = TableConfig.getDefault();
        final Configuration config = tableConfig.getConfiguration();
        final CatalogManager catalogManager =
                preparedCatalogManager().dataTypeFactory(dataTypeFactoryMock).build();
        final ObjectMapper mapper = configuredObjectMapper(catalogManager, tableConfig);

        // minimal plan content
        config.set(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS, IDENTIFIER);
        final String minimalJson = toJson(mapper, STRUCTURED_TYPE);
        assertThat(minimalJson).isEqualTo("\"`default_catalog`.`default_database`.`MyType`\"");

        // catalog lookup with miss
        config.set(TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS, CatalogPlanRestore.IDENTIFIER);
        dataTypeFactoryMock.logicalType = Optional.empty();
        assertThatThrownBy(() -> toLogicalType(mapper, minimalJson))
                .satisfies(anyCauseMatches(ValidationException.class, "No type found."));

        // catalog lookup
        config.set(TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS, CatalogPlanRestore.IDENTIFIER);
        dataTypeFactoryMock.logicalType = Optional.of(STRUCTURED_TYPE);
        assertThat(toLogicalType(mapper, minimalJson)).isEqualTo(STRUCTURED_TYPE);

        // maximum plan content
        config.set(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS, ALL);
        final String maximumJson = toJson(mapper, STRUCTURED_TYPE);
        assertThat(maximumJson)
                .isEqualTo(
                        "{\"type\":\"STRUCTURED_TYPE\","
                                + "\"objectIdentifier\":"
                                + "{\"catalogName\":\"default_catalog\","
                                + "\"databaseName\":\"default_database\","
                                + "\"tableName\":\"MyType\"},"
                                + "\"description\":\"My original type.\","
                                + "\"attributes\":[]}");

        // catalog lookup with miss
        config.set(TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS, CatalogPlanRestore.IDENTIFIER);
        dataTypeFactoryMock.logicalType = Optional.empty();
        assertThatThrownBy(() -> toLogicalType(mapper, maximumJson))
                .satisfies(anyCauseMatches(ValidationException.class, "No type found."));

        // catalog lookup
        config.set(TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS, CatalogPlanRestore.IDENTIFIER);
        dataTypeFactoryMock.logicalType = Optional.of(UPDATED_STRUCTURED_TYPE);
        assertThat(toLogicalType(mapper, maximumJson)).isEqualTo(UPDATED_STRUCTURED_TYPE);

        // no lookup
        config.set(TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS, CatalogPlanRestore.ALL);
        dataTypeFactoryMock.logicalType = Optional.of(UPDATED_STRUCTURED_TYPE);
        assertThat(toLogicalType(mapper, maximumJson)).isEqualTo(STRUCTURED_TYPE);
    }
}
