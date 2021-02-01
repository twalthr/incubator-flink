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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableColumn;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import javax.annotation.Nullable;

import java.util.ArrayList;
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

    private final boolean isStreamingMode;

    private final DataTypeFactory dataTypeFactory;

    private final ExpressionResolverFactory resolverFactory;

    // TODO integrate MergeTableLikeUtil.mergeTables here

    public SchemaResolver(
            boolean isStreamingMode,
            DataTypeFactory dataTypeFactory,
            ExpressionResolverFactory resolverFactory) {
        this.isStreamingMode = isStreamingMode;
        this.dataTypeFactory = dataTypeFactory;
        this.resolverFactory = resolverFactory;
    }

    public ResolvedSchema resolve(Schema schema, boolean supportMetadata) {
        final List<TableColumn> columns = resolveColumns(schema.columns, supportMetadata);

        final List<WatermarkSpec> watermarkSpecs =
                resolveWatermarkSpecs(schema.watermarkSpecs, columns);

        final List<TableColumn> columnsWithRowtime = setRowtimeAttributes(watermarkSpecs, columns);

        final UniqueConstraint primaryKey =
                resolvePrimaryKey(schema.primaryKey, columnsWithRowtime);

        return new ResolvedSchema(columnsWithRowtime, watermarkSpecs, primaryKey);
    }

    private List<TableColumn> resolveColumns(
            List<Schema.UnresolvedColumn> unresolvedColumns, boolean supportMetadata) {
        final List<TableColumn> resolvedColumns = new ArrayList<>();
        for (Schema.UnresolvedColumn unresolvedColumn : unresolvedColumns) {
            final TableColumn column;
            if (unresolvedColumn instanceof Schema.UnresolvedPhysicalColumn) {
                column = resolvePhysicalColumn((Schema.UnresolvedPhysicalColumn) unresolvedColumn);
            } else if (unresolvedColumn instanceof Schema.UnresolvedMetadataColumn) {
                column =
                        resolveMetadataColumn(
                                (Schema.UnresolvedMetadataColumn) unresolvedColumn,
                                supportMetadata);
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
                unresolvedColumn.columnName,
                dataTypeFactory.createDataType(unresolvedColumn.dataType));
    }

    private TableColumn.MetadataColumn resolveMetadataColumn(
            Schema.UnresolvedMetadataColumn unresolvedColumn, boolean supportMetadata) {
        if (!supportMetadata) {
            throw new ValidationException(
                    "Metadata columns are not supported in a schema at the current location.");
        }
        return TableColumn.metadata(
                unresolvedColumn.columnName,
                dataTypeFactory.createDataType(unresolvedColumn.dataType),
                unresolvedColumn.metadataKey,
                unresolvedColumn.isVirtual);
    }

    private TableColumn.ComputedColumn resolveComputedColumn(
            Schema.UnresolvedComputedColumn unresolvedColumn, List<TableColumn> inputColumns) {
        final ResolvedExpression resolvedExpression =
                resolveExpression(inputColumns, unresolvedColumn.expression);
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
        final String[] timeField;
        if (watermarkSpec.timeFieldString != null) {
            timeField = resolveTimeField(watermarkSpec.timeFieldString, inputColumns);
        } else {
            timeField = resolveTimeField(watermarkSpec.timeFieldExpression, inputColumns);
        }

        // resolve watermark expression
        final ResolvedExpression watermarkExpression =
                resolveExpression(inputColumns, watermarkSpec.watermarkExpression);
        validateWatermarkExpression(watermarkExpression.getOutputDataType().getLogicalType());

        return Collections.singletonList(new WatermarkSpec(timeField, watermarkExpression));
    }

    private String[] resolveTimeField(Expression timeField, List<TableColumn> columns) {
        final ResolvedExpression resolvedTimeField = resolveExpression(columns, timeField);
        if (!(resolvedTimeField instanceof FieldReferenceExpression)) {
            throw new ValidationException(
                    "Invalid time field for watermark definition. "
                            + "A watermark definition must be defined on a top-level column. "
                            + "Nested fields are not supported yet.");
        }
        final FieldReferenceExpression fieldRef = (FieldReferenceExpression) resolvedTimeField;
        validateTimeField(fieldRef.getOutputDataType().getLogicalType());
        return new String[] {fieldRef.getName()};
    }

    private String[] resolveTimeField(String[] timeField, List<TableColumn> columns) {
        if (timeField.length > 1) {
            throw new ValidationException(
                    "Invalid time field for watermark definition. "
                            + "A watermark definition must be defined on a top-level column. "
                            + "Nested fields are not supported yet.");
        }
        final String timeFieldName = timeField[0];
        final TableColumn column =
                columns.stream()
                        .filter(c -> c.getName().equals(timeFieldName))
                        .findAny()
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Invalid time field '%s' for watermark definition. Available columns are: %s",
                                                        timeFieldName, columns)));
        validateTimeField(column.getType().getLogicalType());
        return timeField;
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
                            if (hasWatermarkSpec && isStreamingMode) {
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
                UniqueConstraint.primaryKey(constraintName, unresolvedPrimaryKey.columnNames);

        validatePrimaryKey(primaryKey, columns);

        return primaryKey;
    }

    private void validatePrimaryKey(UniqueConstraint primaryKey, List<TableColumn> columns) {
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

    private ResolvedExpression resolveExpression(List<TableColumn> columns, Expression expression) {
        return resolverFactory
                .createResolver(columns)
                .resolve(Collections.singletonList(expression))
                .get(0);
    }

    // -------------------------------------------------------

    public interface ExpressionResolverFactory {
        ExpressionResolver createResolver(List<TableColumn> columns);
    }
}
