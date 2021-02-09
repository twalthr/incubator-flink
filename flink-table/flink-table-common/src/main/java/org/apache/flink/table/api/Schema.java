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
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.Column.PhysicalColumn;
import org.apache.flink.table.catalog.Constraint;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.SchemaResolver;
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
import java.util.Optional;
import java.util.UUID;

/**
 * Schema of a table or view.
 *
 * <p>A schema represents the schema part of a {@code CREATE TABLE (schema) WITH (options)} DDL
 * statement in SQL. It defines columns of different kind, constraints, time attributes, and
 * watermark strategies. It is possible to reference objects (such as functions or types) across
 * different catalogs.
 *
 * <p>This class is used in the API and catalogs to define an unresolved schema that will be
 * translated to {@link ResolvedSchema}. Some methods of this class perform basic validation,
 * however, the main validation happens during the resolution.
 */
@PublicEvolving
public final class Schema {

    private final List<UnresolvedColumn> columns;

    private final List<UnresolvedWatermarkSpec> watermarkSpecs;

    private final @Nullable UnresolvedPrimaryKey primaryKey;

    private Schema(
            List<UnresolvedColumn> columns,
            List<UnresolvedWatermarkSpec> watermarkSpecs,
            @Nullable UnresolvedPrimaryKey primaryKey) {
        this.columns = columns;
        this.watermarkSpecs = watermarkSpecs;
        this.primaryKey = primaryKey;
    }

    /** Builder for configuring and creating instances of {@link Schema}. */
    public static Schema.Builder newBuilder() {
        return new Builder();
    }

    /** Resolves the given {@link Schema} to a validated {@link ResolvedSchema}. */
    public ResolvedSchema resolve(SchemaResolver resolver) {
        return resolver.resolve(this);
    }

    // --------------------------------------------------------------------------------------------

    /** A builder for constructing an immutable but still unresolved {@link Schema}. */
    public static final class Builder {

        private final List<UnresolvedColumn> columns;

        private final List<UnresolvedWatermarkSpec> watermarkSpecs;

        private @Nullable UnresolvedPrimaryKey primaryKey;

        private Builder() {
            columns = new ArrayList<>();
            watermarkSpecs = new ArrayList<>();
        }

        /** Adopts all members from the given unresolved schema. */
        public Builder fromSchema(Schema unresolvedSchema) {
            columns.addAll(unresolvedSchema.columns);
            watermarkSpecs.addAll(unresolvedSchema.watermarkSpecs);
            if (unresolvedSchema.primaryKey != null) {
                primaryKeyNamed(
                        unresolvedSchema.primaryKey.getConstraintName(),
                        unresolvedSchema.primaryKey.getColumnNames());
            }
            return this;
        }

        /** Adopts all members from the given resolved schema. */
        public Builder fromResolvedSchema(ResolvedSchema resolvedSchema) {
            addResolvedColumns(resolvedSchema.getColumns());
            addResolvedWatermarkSpec(resolvedSchema.getWatermarkSpecs());
            resolvedSchema.getPrimaryKey().ifPresent(this::addResolvedConstraint);
            return this;
        }

        /**
         * Declares a physical column that is appended to this schema.
         *
         * <p>Physical columns are regular columns known from databases. They define the names, the
         * types, and the order of fields in the physical data. Thus, physical columns represent the
         * payload that is read from and written to an external system. Connectors and formats use
         * these columns (in the defined order) to configure themselves. Other kinds of columns can
         * be declared between physical columns but will not influence the final physical schema.
         *
         * @param columnName column name
         * @param dataType data type of the column
         */
        public Builder column(String columnName, AbstractDataType<?> dataType) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(dataType, "Data type must not be null.");
            columns.add(new UnresolvedPhysicalColumn(columnName, dataType));
            return this;
        }

        /**
         * Declares a computed column that is appended to this schema.
         *
         * <p>Computed columns are virtual columns that are generated by evaluating an expression
         * that can reference other columns declared in the same table. Both physical columns and
         * metadata columns can be accessed if they precede the computed column in the schema
         * declaration. The column itself is not physically stored within the table. The column’s
         * data type is derived automatically from the given expression and does not have to be
         * declared manually.
         *
         * <p>Computed columns are commonly used for defining time attributes. For example, the
         * computed column can be used if the original field is not TIMESTAMP(3) type or is nested
         * in a JSON string.
         *
         * <p>Any scalar expression can be used for in-memory/temporary tables. However, currently,
         * only SQL expressions can be persisted in a catalog. User-defined functions (also defined
         * in different catalogs) are supported.
         *
         * <p>Example: {@code .columnByExpression("ts", callSql("CAST(json_obj.ts AS TIMESTAMP(3)")}
         *
         * @param columnName column name
         * @param expression computation of the column
         */
        public Builder columnByExpression(String columnName, Expression expression) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(expression, "Expression must not be null.");
            columns.add(new UnresolvedComputedColumn(columnName, expression));
            return this;
        }

        /**
         * Declares a metadata column that is appended to this schema.
         *
         * <p>Metadata columns allow to access connector and/or format specific fields for every row
         * of a table. For example, a metadata column can be used to read and write the timestamp
         * from and to Kafka records for time-based operations. The connector and format
         * documentation lists the available metadata fields for every component.
         *
         * <p>Every metadata field is identified by a string-based key and has a documented data
         * type. For convenience, the runtime will perform an explicit cast if the data type of the
         * column differs from the data type of the metadata field. Of course, this requires that
         * the two data types are compatible.
         *
         * <p>By default, a metadata column can be used for both reading and writing. However, in
         * many cases an external system provides more read-only metadata fields than writable
         * fields. Therefore, it is possible to exclude metadata columns from persisting by setting
         * the {@code isVirtual} flag to {@code true}.
         *
         * <p>Note: This method assumes that the metadata key is equal to the column name.
         *
         * @param columnName column name
         * @param dataType data type of the column
         * @param isVirtual whether the column should be persisted or not
         */
        public Builder columnByMetadata(
                String columnName, AbstractDataType<?> dataType, boolean isVirtual) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(dataType, "Data type must not be null.");
            columns.add(new UnresolvedMetadataColumn(columnName, dataType, null, isVirtual));
            return this;
        }

        /**
         * Declares a metadata column that is appended to this schema.
         *
         * <p>Metadata columns allow to access connector and/or format specific fields for every row
         * of a table. For example, a metadata column can be used to read and write the timestamp
         * from and to Kafka records for time-based operations. The connector and format
         * documentation lists the available metadata fields for every component.
         *
         * <p>Every metadata field is identified by a string-based key and has a documented data
         * type. The metadata key can be omitted if the column name should be used as the
         * identifying metadata key. For convenience, the runtime will perform an explicit cast if
         * the data type of the column differs from the data type of the metadata field. Of course,
         * this requires that the two data types are compatible.
         *
         * <p>Note: This method assumes that a metadata column can be used for both reading and
         * writing.
         *
         * @param columnName column name
         * @param dataType data type of the column
         * @param metadataKey identifying metadata key, if null the column name will be used as
         *     metadata key
         */
        public Builder columnByMetadata(
                String columnName, AbstractDataType<?> dataType, @Nullable String metadataKey) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(dataType, "Data type must not be null.");
            columns.add(new UnresolvedMetadataColumn(columnName, dataType, metadataKey, false));
            return this;
        }

        /**
         * Declares a metadata column that is appended to this schema.
         *
         * <p>Metadata columns allow to access connector and/or format specific fields for every row
         * of a table. For example, a metadata column can be used to read and write the timestamp
         * from and to Kafka records for time-based operations. The connector and format
         * documentation lists the available metadata fields for every component.
         *
         * <p>Every metadata field is identified by a string-based key and has a documented data
         * type. The metadata key can be omitted if the column name should be used as the
         * identifying metadata key. For convenience, the runtime will perform an explicit cast if
         * the data type of the column differs from the data type of the metadata field. Of course,
         * this requires that the two data types are compatible.
         *
         * <p>By default, a metadata column can be used for both reading and writing. However, in
         * many cases an external system provides more read-only metadata fields than writable
         * fields. Therefore, it is possible to exclude metadata columns from persisting by setting
         * the {@code isVirtual} flag to {@code true}.
         *
         * @param columnName column name
         * @param dataType data type of the column
         * @param metadataKey identifying metadata key, if null the column name will be used as
         *     metadata key
         * @param isVirtual whether the column should be persisted or not
         */
        public Builder columnByMetadata(
                String columnName,
                AbstractDataType<?> dataType,
                @Nullable String metadataKey,
                boolean isVirtual) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            columns.add(new UnresolvedMetadataColumn(columnName, dataType, metadataKey, isVirtual));
            return this;
        }

        /**
         * Declares that the given column should serve as an event-time (i.e. rowtime) attribute and
         * specifies a corresponding watermark strategy as an expression.
         *
         * <p>The column must be of type {@code TIMESTAMP(3)} and be a top-level column in the
         * schema. It may be a computed column.
         *
         * <p>The watermark generation expression is evaluated by the framework for every record
         * during runtime. The framework will periodically emit the largest generated watermark. If
         * the current watermark is still identical to the previous one, or is null, or the value of
         * the returned watermark is smaller than that of the last emitted one, then no new
         * watermark will be emitted. A watermark is emitted in an interval defined by the
         * configuration.
         *
         * <p>Any scalar expression can be used for declaring a watermark strategy for
         * in-memory/temporary tables. However, currently, only SQL expressions can be persisted in
         * a catalog. The expression's return data type must be {@code TIMESTAMP(3)}. User-defined
         * functions (also defined in different catalogs) are supported.
         *
         * <p>Example: {@code .watermark("ts", callSql("ts - INTERVAL'5' SECOND")}
         *
         * @param columnName the column name used as a rowtime attribute
         * @param watermarkExpression the expression used for watermark generation
         */
        public Builder watermark(String columnName, Expression watermarkExpression) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(
                    watermarkExpression, "Watermark expression must not be null.");
            this.watermarkSpecs.add(new UnresolvedWatermarkSpec(columnName, watermarkExpression));
            return this;
        }

        /**
         * Declares a primary key constraint for a set of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable. The primary
         * key is informational only. It will not be enforced. It can be used for optimizations. It
         * is the data owner's responsibility to ensure uniqueness of the data.
         *
         * <p>The primary key will be assigned a random name.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKey(String... columnNames) {
            Preconditions.checkNotNull(columnNames, "Primary key column names must not be null.");
            return primaryKey(Arrays.asList(columnNames));
        }

        /**
         * Declares a primary key constraint for a set of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable. The primary
         * key is informational only. It will not be enforced. It can be used for optimizations. It
         * is the data owner's responsibility to ensure uniqueness of the data.
         *
         * <p>The primary key will be assigned a random name.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKey(List<String> columnNames) {
            return primaryKeyNamed(UUID.randomUUID().toString(), columnNames);
        }

        /**
         * Declares a primary key constraint for a set of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable. The primary
         * key is informational only. It will not be enforced. It can be used for optimizations. It
         * is the data owner's responsibility to ensure uniqueness of the data.
         *
         * @param constraintName name for the primary key, can be used to reference the constraint
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKeyNamed(String constraintName, String... columnNames) {
            Preconditions.checkNotNull(columnNames, "Primary key column names must not be null.");
            return primaryKeyNamed(constraintName, Arrays.asList(columnNames));
        }

        /**
         * Declares a primary key constraint for a set of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable. The primary
         * key is informational only. It will not be enforced. It can be used for optimizations. It
         * is the data owner's responsibility to ensure uniqueness of the data.
         *
         * @param constraintName name for the primary key, can be used to reference the constraint
         * @param columnNames columns that form a unique primary key
         */
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

        /** Returns an instance of an unresolved {@link Schema}. */
        public Schema build() {
            return new Schema(columns, watermarkSpecs, primaryKey);
        }

        // ----------------------------------------------------------------------------------------

        private void addResolvedColumns(List<Column> columns) {
            columns.forEach(
                    c -> {
                        if (c instanceof PhysicalColumn) {
                            final PhysicalColumn physicalColumn = (PhysicalColumn) c;
                            column(physicalColumn.getName(), physicalColumn.getDataType());
                        } else if (c instanceof ComputedColumn) {
                            final ComputedColumn computedColumn = (ComputedColumn) c;
                            columnByExpression(
                                    computedColumn.getName(), computedColumn.getExpression());
                        } else if (c instanceof MetadataColumn) {
                            final MetadataColumn metadataColumn = (MetadataColumn) c;
                            columnByMetadata(
                                    metadataColumn.getName(),
                                    metadataColumn.getDataType(),
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
                                            s.getRowtimeAttribute(), s.getWatermarkExpression())));
        }

        private void addResolvedConstraint(UniqueConstraint constraint) {
            if (constraint.getType() == Constraint.ConstraintType.PRIMARY_KEY) {
                primaryKeyNamed(constraint.getName(), constraint.getColumns());
            } else {
                throw new IllegalArgumentException("Unsupported constraint type.");
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Internal helper classes for representing the schema
    // --------------------------------------------------------------------------------------------

    /** Super class for all kinds of columns in an unresolved schema. */
    public abstract static class UnresolvedColumn {
        final String columnName;

        UnresolvedColumn(String columnName) {
            this.columnName = columnName;
        }
    }

    /**
     * Declaration of a physical column that will be resolved to {@link PhysicalColumn} during
     * schema resolution.
     */
    public static final class UnresolvedPhysicalColumn extends UnresolvedColumn {

        private final AbstractDataType<?> dataType;

        UnresolvedPhysicalColumn(String columnName, AbstractDataType<?> dataType) {
            super(columnName);
            this.dataType = dataType;
        }

        public AbstractDataType<?> getDataType() {
            return dataType;
        }
    }

    /**
     * Declaration of a computed column that will be resolved to {@link ComputedColumn} during
     * schema resolution.
     */
    public static final class UnresolvedComputedColumn extends UnresolvedColumn {

        private final Expression expression;

        UnresolvedComputedColumn(String columnName, Expression expression) {
            super(columnName);
            this.expression = expression;
        }

        public Expression getExpression() {
            return expression;
        }
    }

    /**
     * Declaration of a metadata column that will be resolved to {@link MetadataColumn} during
     * schema resolution.
     */
    public static final class UnresolvedMetadataColumn extends UnresolvedColumn {

        private final AbstractDataType<?> dataType;
        private final @Nullable String metadataKey;
        private final boolean isVirtual;

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

        public AbstractDataType<?> getDataType() {
            return dataType;
        }

        public @Nullable String getMetadataKey() {
            return metadataKey;
        }

        public boolean isVirtual() {
            return isVirtual;
        }
    }

    /**
     * Declaration of a watermark strategy that will be resolved to {@link WatermarkSpec} during
     * schema resolution.
     */
    public static final class UnresolvedWatermarkSpec {

        private final String columnName;
        private final Expression watermarkExpression;

        UnresolvedWatermarkSpec(String columnName, Expression watermarkExpression) {
            this.columnName = columnName;
            this.watermarkExpression = watermarkExpression;
        }

        public String getColumnName() {
            return columnName;
        }

        public Optional<Expression> getWatermarkExpression() {
            return Optional.ofNullable(watermarkExpression);
        }
    }

    /** Super class for all kinds of constraints in an unresolved schema. */
    public abstract static class UnresolvedConstraint {

        private final String constraintName;

        UnresolvedConstraint(String constraintName) {
            this.constraintName = constraintName;
        }

        public String getConstraintName() {
            return constraintName;
        }
    }

    /**
     * Declaration of a primary key that will be resolved to {@link UniqueConstraint} during schema
     * resolution.
     */
    public static final class UnresolvedPrimaryKey extends UnresolvedConstraint {

        private final List<String> columnNames;

        UnresolvedPrimaryKey(String constraintName, List<String> columnNames) {
            super(constraintName);
            this.columnNames = columnNames;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }
    }
}
