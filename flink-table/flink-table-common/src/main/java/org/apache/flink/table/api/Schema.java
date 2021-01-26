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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.Constraint;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableColumn;
import org.apache.flink.table.catalog.TableColumn.ComputedColumn;
import org.apache.flink.table.catalog.TableColumn.MetadataColumn;
import org.apache.flink.table.catalog.TableColumn.PhysicalColumn;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Schema of a table or view.
 *
 * <p>This class is used in the API and catalogs to define an unresolved schema that will be
 * translated to {@link ResolvedSchema}.
 *
 * <p>Some methods of this class perform basic validation, however, the main validation happens
 * during the resolution.
 */
@PublicEvolving
public final class Schema {

    final List<UnresolvedColumn> columns;

    final List<UnresolvedWatermarkSpec> watermarkSpecs;

    final @Nullable UnresolvedPrimaryKey primaryKey;

    private Schema(
            List<UnresolvedColumn> columns,
            List<UnresolvedWatermarkSpec> watermarkSpecs,
            @Nullable UnresolvedPrimaryKey primaryKey) {
        this.columns = columns;
        this.watermarkSpecs = watermarkSpecs;
        this.primaryKey = primaryKey;
    }

    public static Schema.Builder newBuilder() {
        return new Builder();
    }

    // --------------------------------------------------------------------------------------------

    public static final class Builder {

        private final List<UnresolvedColumn> columns;

        private final List<UnresolvedWatermarkSpec> watermarkSpecs;

        private @Nullable UnresolvedPrimaryKey primaryKey;

        private Builder() {
            columns = new ArrayList<>();
            watermarkSpecs = new ArrayList<>();
        }

        public Builder fromSchema(Schema unresolvedSchema) {
            columns.addAll(unresolvedSchema.columns);
            watermarkSpecs.addAll(unresolvedSchema.watermarkSpecs);
            if (unresolvedSchema.primaryKey != null) {
                primaryKeyNamed(
                        unresolvedSchema.primaryKey.constraintName,
                        unresolvedSchema.primaryKey.columnNames);
            }
            return this;
        }

        public Builder fromSchema(ResolvedSchema resolvedSchema) {
            addResolvedColumns(resolvedSchema.getTableColumns());
            addResolvedWatermarkSpec(resolvedSchema.getWatermarkSpecs());
            resolvedSchema.getPrimaryKey().ifPresent(this::addResolvedConstraint);
            return this;
        }

        private void addResolvedColumns(List<TableColumn> columns) {
            columns.forEach(
                    c -> {
                        if (c instanceof PhysicalColumn) {
                            final PhysicalColumn physicalColumn = (PhysicalColumn) c;
                            column(physicalColumn.getName(), physicalColumn.getType());
                        } else if (c instanceof ComputedColumn) {
                            final ComputedColumn computedColumn = (ComputedColumn) c;
                            columnByExpression(
                                    computedColumn.getName(), computedColumn.getExpression());
                        } else if (c instanceof MetadataColumn) {
                            final MetadataColumn metadataColumn = (MetadataColumn) c;
                            columnByMetadata(
                                    metadataColumn.getName(),
                                    metadataColumn.getType(),
                                    metadataColumn.getMetadataAlias().orElse(null),
                                    metadataColumn.isVirtual());
                        }
                    });
        }

        private void addResolvedWatermarkSpec(List<WatermarkSpec> specs) {
            specs.forEach(
                    s ->
                            watermarkSpecs.add(
                                    new UnresolvedWatermarkSpec(
                                            Arrays.asList(s.getRowtimeAttribute()),
                                            s.getWatermarkExpression())));
        }

        private void addResolvedConstraint(UniqueConstraint constraint) {
            if (constraint.getType() == Constraint.ConstraintType.PRIMARY_KEY) {
                primaryKeyNamed(constraint.getName(), constraint.getColumns());
            } else {
                throw new UnsupportedOperationException("Unsupported constraint type.");
            }
        }

        public Builder column(String columnName, AbstractDataType<?> dataType) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(dataType, "Data type must not be null.");
            columns.add(new UnresolvedPhysicalColumn(columnName, dataType));
            return this;
        }

        public Builder columnByExpression(String columnName, Expression expression) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(expression, "Expression must not be null.");
            columns.add(new UnresolvedComputedColumn(columnName, expression));
            return this;
        }

        public Builder columnByMetadata(
                String columnName, AbstractDataType<?> dataType, boolean isVirtual) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(dataType, "Data type must not be null.");
            columns.add(new UnresolvedMetadataColumn(columnName, dataType, null, isVirtual));
            return this;
        }

        public Builder columnByMetadata(
                String columnName, AbstractDataType<?> dataType, @Nullable String metadataKey) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(dataType, "Data type must not be null.");
            columns.add(new UnresolvedMetadataColumn(columnName, dataType, metadataKey, false));
            return this;
        }

        public Builder columnByMetadata(
                String columnName,
                AbstractDataType<?> dataType,
                @Nullable String metadataKey,
                boolean isVirtual) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            columns.add(new UnresolvedMetadataColumn(columnName, dataType, metadataKey, isVirtual));
            return this;
        }

        public Builder watermark(Expression timeField, Expression watermarkExpression) {
            Preconditions.checkNotNull(timeField, "Time field must not be null.");
            Preconditions.checkNotNull(
                    watermarkExpression, "Watermark expression must not be null.");
            this.watermarkSpecs.add(new UnresolvedWatermarkSpec(timeField, watermarkExpression));
            return this;
        }

        public Builder primaryKey(String... columnNames) {
            Preconditions.checkNotNull(columnNames, "Primary key column names must not be null.");
            return primaryKey(Arrays.asList(columnNames));
        }

        public Builder primaryKey(List<String> columnNames) {
            return primaryKeyNamed(UUID.randomUUID().toString(), columnNames);
        }

        public Builder primaryKeyNamed(String constraintName, String... columnNames) {
            Preconditions.checkNotNull(columnNames, "Primary key column names must not be null.");
            return primaryKeyNamed(constraintName, Arrays.asList(columnNames));
        }

        public Builder primaryKeyNamed(String constraintName, List<String> columnNames) {
            Preconditions.checkState(
                    primaryKey != null, "Multiple primary keys are not supported.");
            Preconditions.checkNotNull(
                    constraintName, "Primary key constraint name must not be null.");
            Preconditions.checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(constraintName),
                    "Primary key constraint name must not be empty.");
            Preconditions.checkArgument(
                    columnNames != null && columnNames.size() > 0,
                    "Primary key constraint must be defined for at least a single column.");
            primaryKey = new UnresolvedPrimaryKey(constraintName, columnNames);
            return this;
        }

        public Schema build() {
            return new Schema(columns, watermarkSpecs, primaryKey);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Internal helper classes for representing the schema
    // --------------------------------------------------------------------------------------------

    abstract static class UnresolvedColumn {
        final String columnName;

        UnresolvedColumn(String columnName) {
            this.columnName = columnName;
        }
    }

    static class UnresolvedPhysicalColumn extends UnresolvedColumn {
        final AbstractDataType<?> dataType;

        UnresolvedPhysicalColumn(String columnName, AbstractDataType<?> dataType) {
            super(columnName);
            this.dataType = dataType;
        }
    }

    static class UnresolvedComputedColumn extends UnresolvedColumn {
        final Expression expression;

        UnresolvedComputedColumn(String columnName, Expression expression) {
            super(columnName);
            this.expression = expression;
        }
    }

    static class UnresolvedMetadataColumn extends UnresolvedColumn {
        final AbstractDataType<?> dataType;
        final @Nullable String metadataKey;
        final boolean isVirtual;

        UnresolvedMetadataColumn(
                String columnName,
                AbstractDataType<?> dataType,
                @Nullable String metadataKey,
                boolean isVirtual) {
            super(columnName);
            this.dataType = dataType;
            this.metadataKey = metadataKey;
            this.isVirtual = isVirtual;
        }
    }

    static class UnresolvedWatermarkSpec {
        // either as $("field").get("otherField")
        final @Nullable Expression timeFieldExpression;
        // or as ["field", "otherField"]
        final @Nullable List<String> timeFieldString;

        final Expression watermarkExpression;

        UnresolvedWatermarkSpec(Expression timeFieldExpression, Expression watermarkExpression) {
            this.timeFieldExpression = timeFieldExpression;
            this.timeFieldString = null;
            this.watermarkExpression = watermarkExpression;
        }

        UnresolvedWatermarkSpec(List<String> timeFieldString, Expression watermarkExpression) {
            this.timeFieldExpression = null;
            this.timeFieldString = timeFieldString;
            this.watermarkExpression = watermarkExpression;
        }
    }

    abstract static class UnresolvedConstraint {
        final String constraintName;

        UnresolvedConstraint(String constraintName) {
            this.constraintName = constraintName;
        }
    }

    static class UnresolvedPrimaryKey extends UnresolvedConstraint {
        final List<String> columnNames;

        UnresolvedPrimaryKey(String constraintName, List<String> columnNames) {
            super(constraintName);
            this.columnNames = columnNames;
        }
    }
}
