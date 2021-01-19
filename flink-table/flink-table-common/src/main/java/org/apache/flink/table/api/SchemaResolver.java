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

package org.apache.flink.table.api;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableColumn;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Internal
public final class SchemaResolver {

    private final SchemaResolverContext context;

    // TODO integrate MergeTableLikeUtil.mergeTables here

    // TODO integrate CatalogTableSchemaResolver here

    // TODO integrate validation logic of TableSchema.Builder here

    public SchemaResolver(SchemaResolverContext context) {
        this.context = context;
    }

    public ResolvedSchema resolve(Schema schema) {
        final List<TableColumn> columns = resolveColumns(schema.columns);
        final List<WatermarkSpec> watermarkSpecs = resolveWatermarkSpecs(schema.watermarkSpecs);
    }

    private List<TableColumn> resolveColumns(List<Schema.UnresolvedColumn> unresolvedColumns) {
        final List<TableColumn> resolvedColumns = new ArrayList<>();
        for (Schema.UnresolvedColumn unresolvedColumn : unresolvedColumns) {
            final TableColumn column;
            if (unresolvedColumn instanceof Schema.UnresolvedPhysicalColumn) {
                column = resolvePhysicalColumn((Schema.UnresolvedPhysicalColumn) unresolvedColumn);
            } else if (unresolvedColumn instanceof Schema.UnresolvedMetadataColumn) {
                column = resolveMetadataColumn((Schema.UnresolvedMetadataColumn) unresolvedColumn);
            } else if (unresolvedColumn instanceof Schema.UnresolvedComputedColumn) {
                column =
                        resolveComputedColumn(
                                (Schema.UnresolvedComputedColumn) unresolvedColumn,
                                resolvedColumns);
            } else {
                throw new IllegalArgumentException("Unknown unresolved column type.");
            }
            resolvedColumns.add(column);
        }

        validateDuplicateColumns(resolvedColumns);

        return resolvedColumns;
    }

    private TableColumn.PhysicalColumn resolvePhysicalColumn(
            Schema.UnresolvedPhysicalColumn column) {
        return TableColumn.physical(column.columnName, context.resolveDataType(column.dataType));
    }

    private TableColumn.MetadataColumn resolveMetadataColumn(
            Schema.UnresolvedMetadataColumn column) {
        if (!context.supportsMetadata()) {
            throw new ValidationException(
                    "Metadata columns are not supported in a schema at the current location.");
        }
        return TableColumn.metadata(
                column.columnName,
                context.resolveDataType(column.dataType),
                column.metadataKey,
                column.isVirtual);
    }

    private TableColumn.ComputedColumn resolveComputedColumn(
            Schema.UnresolvedComputedColumn column, List<TableColumn> inputColumns) {
        final ResolvedExpression resolvedExpression =
                context.resolveExpression(inputColumns, column.expression);
        return TableColumn.computed(column.columnName, resolvedExpression);
    }

    private void validateDuplicateColumns(List<TableColumn> columns) {
        final List<String> names =
                columns.stream().map(TableColumn::getName).collect(Collectors.toList());
        final List<String> duplicates =
                names.stream()
                        .filter(name -> Collections.frequency(names, name) > 1)
                        .distinct()
                        .collect(Collectors.toList());
        if (duplicates.size() > 0) {
            throw new ValidationException(
                    String.format(
                            "Table schema must not contain duplicate column names. Found duplicates: %s",
                            duplicates));
        }
    }

    private List<WatermarkSpec> resolveWatermarkSpecs(List<Schema.UnresolvedWatermarkSpec> watermarkSpecs) {
        if (watermarkSpecs.size() == 0) {
            return Collections.emptyList();
        }
        if (watermarkSpecs.size() > 1) {
            throw new ValidationException("Multiple watermark definitions are not supported yet.");
        }
        watermarkSpecs.stream().map(spec -> spec.)
    }

    private void validateWatermarkSpecs(List<WatermarkSpec> watermarkSpecs) {
        if (watermarkSpecs.size() > 1) {
            throw new ValidationException("Multiple watermark definitions are not supported yet.");
        }
    }

    // --------------------------------------------------------------------------------------------

    private interface SchemaResolverContext {
        boolean supportsMetadata();

        ResolvedExpression resolveExpression(List<TableColumn> inputColumns, Expression expression);

        DataType resolveDataType(AbstractDataType<?> dataType);
    }
}
