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
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.List;

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
    }

    private List<TableColumn> resolveColumns(List<Schema.UnresolvedColumn> unresolvedColumns) {
        final List<TableColumn> resolvedColumns = new ArrayList<>();
        for (Schema.UnresolvedColumn unresolvedColumn : unresolvedColumns) {}
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

    private interface SchemaResolverContext {
        boolean supportsMetadata();

        ResolvedExpression resolveExpression(List<TableColumn> inputColumns, Expression expression);

        DataType resolveDataType(AbstractDataType<?> dataType);
    }
}
