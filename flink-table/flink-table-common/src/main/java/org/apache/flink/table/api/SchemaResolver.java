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
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute;
import static org.apache.flink.table.types.utils.DataTypeUtils.replaceLogicalType;

/**
 * Resolves {@link Schema} (coming either from API or catalog) into a {@link ResolvedSchema}.
 *
 * <p>Currently, we only support a single {@link WatermarkSpec} and don't support nested rowtime
 * attributes. However, both class and method signatures are already prepared for the future.
 */
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

        final List<WatermarkSpec> watermarkSpecs =
                resolveWatermarkSpecs(schema.watermarkSpecs, columns);

        final List<TableColumn> columnsWithRowtime = setRowtimeAttributes(watermarkSpecs, columns);

        final UniqueConstraint primaryKey =
                resolvePrimaryKey(schema.primaryKey, columnsWithRowtime);

        return new ResolvedSchema(columnsWithRowtime, watermarkSpecs, primaryKey);
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
            Schema.UnresolvedPhysicalColumn unresolvedColumn) {
        return TableColumn.physical(
                unresolvedColumn.columnName, context.resolveDataType(unresolvedColumn.dataType));
    }

    private TableColumn.MetadataColumn resolveMetadataColumn(
            Schema.UnresolvedMetadataColumn unresolvedColumn) {
        if (!context.supportsMetadata()) {
            throw new ValidationException(
                    "Metadata columns are not supported in a schema at the current location.");
        }
        return TableColumn.metadata(
                unresolvedColumn.columnName,
                context.resolveDataType(unresolvedColumn.dataType),
                unresolvedColumn.metadataKey,
                unresolvedColumn.isVirtual);
    }

    private TableColumn.ComputedColumn resolveComputedColumn(
            Schema.UnresolvedComputedColumn unresolvedColumn, List<TableColumn> inputColumns) {
        final ResolvedExpression resolvedExpression =
                context.resolveExpression(inputColumns, unresolvedColumn.expression);
        return TableColumn.computed(unresolvedColumn.columnName, resolvedExpression);
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

    private List<WatermarkSpec> resolveWatermarkSpecs(
            List<Schema.UnresolvedWatermarkSpec> unresolvedWatermarkSpecs,
            List<TableColumn> inputColumns) {
        if (unresolvedWatermarkSpecs.size() == 0) {
            return Collections.emptyList();
        }
        if (unresolvedWatermarkSpecs.size() > 1) {
            throw new ValidationException("Multiple watermark definitions are not supported yet.");
        }
        final Schema.UnresolvedWatermarkSpec watermarkSpec = unresolvedWatermarkSpecs.get(0);

        // resolve time field
        final ResolvedExpression resolvedTimeField =
                context.resolveExpression(inputColumns, watermarkSpec.timeField);
        if (!(resolvedTimeField instanceof FieldReferenceExpression)) {
            throw new ValidationException(
                    "Invalid time field for watermark definition. "
                            + "A watermark definition must be defined on a top-level column. "
                            + "Nested fields are not supported yet.");
        }
        final FieldReferenceExpression timeField = (FieldReferenceExpression) resolvedTimeField;
        validateTimeField(timeField.getOutputDataType().getLogicalType());

        // resolve watermark expression
        final ResolvedExpression watermarkExpression =
                context.resolveExpression(inputColumns, watermarkSpec.watermarkExpression);
        validateWatermarkExpression(watermarkExpression.getOutputDataType().getLogicalType());

        return Collections.singletonList(
                new WatermarkSpec(new String[] {timeField.getName()}, watermarkExpression));
    }

    private void validateTimeField(LogicalType timeFieldType) {
        if (!hasRoot(timeFieldType, LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                || getPrecision(timeFieldType) != 3) {
            throw new ValidationException(
                    "Invalid data type of time field for watermark definition. "
                            + "The field must be of type TIMESTAMP(3) WITHOUT TIME ZONE.");
        }
        if (isProctimeAttribute(timeFieldType)) {
            throw new ValidationException(
                    "A watermark can not be defined for a processing-time attribute.");
        }
    }

    private void validateWatermarkExpression(LogicalType watermarkType) {
        if (!hasRoot(watermarkType, LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                || getPrecision(watermarkType) != 3) {
            throw new ValidationException(
                    "Invalid data type of expression for watermark definition. "
                            + "The field must be of type TIMESTAMP(3) WITHOUT TIME ZONE.");
        }
    }

    /** Updates the data type of columns that are referenced by {@link WatermarkSpec}. */
    private List<TableColumn> setRowtimeAttributes(
            List<WatermarkSpec> watermarkSpecs, List<TableColumn> columns) {
        return columns.stream()
                .map(
                        column -> {
                            final String name = column.getName();
                            final DataType dataType = column.getType();
                            final boolean hasWatermarkSpec =
                                    watermarkSpecs.stream()
                                            .anyMatch(s -> s.getRowtimeAttribute()[0].equals(name));
                            if (hasWatermarkSpec && context.isStreamingMode()) {
                                final TimestampType originalType =
                                        (TimestampType) dataType.getLogicalType();
                                final LogicalType rowtimeType =
                                        new TimestampType(
                                                originalType.isNullable(),
                                                TimestampKind.ROWTIME,
                                                originalType.getPrecision());
                                return column.copy(replaceLogicalType(dataType, rowtimeType));
                            }
                            return column;
                        })
                .collect(Collectors.toList());
    }

    private @Nullable UniqueConstraint resolvePrimaryKey(
            @Nullable Schema.UnresolvedPrimaryKey unresolvedPrimaryKey, List<TableColumn> columns) {
        if (unresolvedPrimaryKey == null) {
            return null;
        }

        final String constraintName;
        if (unresolvedPrimaryKey.constraintName != null) {
            constraintName = unresolvedPrimaryKey.constraintName;
        } else {
            constraintName = UUID.randomUUID().toString();
        }

        final UniqueConstraint primaryKey =
                UniqueConstraint.primaryKey(
                        constraintName, Arrays.asList(unresolvedPrimaryKey.columnNames));

        validatePrimaryKey(primaryKey, columns);

        return primaryKey;
    }

    private static void validatePrimaryKey(UniqueConstraint primaryKey, List<TableColumn> columns) {
        final Map<String, TableColumn> columnsByNameLookup =
                columns.stream()
                        .collect(Collectors.toMap(TableColumn::getName, Function.identity()));

        for (String columnName : primaryKey.getColumns()) {
            TableColumn column = columnsByNameLookup.get(columnName);
            if (column == null) {
                throw new ValidationException(
                        String.format(
                                "Invalid primary key '%s'. Column '%s' does not exist.",
                                primaryKey.getName(), columnName));
            }

            if (!column.isPhysical()) {
                throw new ValidationException(
                        String.format(
                                "Invalid primary key '%s'. Column '%s' is not a physical column.",
                                primaryKey.getName(), columnName));
            }

            final LogicalType columnType = column.getType().getLogicalType();
            if (columnType.isNullable()) {
                throw new ValidationException(
                        String.format(
                                "Invalid primary key '%s'. Column '%s' is nullable.",
                                primaryKey.getName(), columnName));
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    private interface SchemaResolverContext {
        boolean supportsMetadata();

        boolean isStreamingMode();

        ResolvedExpression resolveExpression(List<TableColumn> inputColumns, Expression expression);

        DataType resolveDataType(AbstractDataType<?> dataType);
    }
}
