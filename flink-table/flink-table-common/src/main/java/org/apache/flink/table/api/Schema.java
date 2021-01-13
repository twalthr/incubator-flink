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
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

@PublicEvolving
public final class Schema {

    final List<UnresolvedColumn> columns;

    final @Nullable List<UnresolvedWatermarkSpec> watermarkSpecs;

    final @Nullable UnresolvedPrimaryKey primaryKey;

    private Schema(
            List<UnresolvedColumn> columns,
            @Nullable List<UnresolvedWatermarkSpec> watermarkSpecs,
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

        public Builder column(String columnName, AbstractDataType<?> dataType) {
            columns.add(new UnresolvedPhysicalColumn(columnName, dataType));
            return this;
        }

        public Builder columnByExpression(String columnName, Expression expression) {
            columns.add(new UnresolvedComputedColumn(columnName, expression));
            return this;
        }

        public Builder columnByMetadata(
                String columnName, AbstractDataType<?> dataType, boolean isVirtual) {
            columns.add(new UnresolvedMetadataColumn(columnName, dataType, null, isVirtual));
            return this;
        }

        public Builder columnByMetadata(
                String columnName, AbstractDataType<?> dataType, @Nullable String metadataKey) {
            columns.add(new UnresolvedMetadataColumn(columnName, dataType, metadataKey, false));
            return this;
        }

        public Builder columnByMetadata(
                String columnName,
                AbstractDataType<?> dataType,
                @Nullable String metadataKey,
                boolean isVirtual) {
            columns.add(new UnresolvedMetadataColumn(columnName, dataType, metadataKey, isVirtual));
            return this;
        }

        public Builder watermark(String timeColumnName, Expression expression) {
            this.watermarkSpecs.add(new UnresolvedWatermarkSpec(timeColumnName, expression));
            return this;
        }

        public Builder primaryKey(String... columnNames) {
            this.primaryKey = new UnresolvedPrimaryKey(null, columnNames);
            return this;
        }

        public Builder primaryKeyNamed(@Nullable String constraintName, String... columnNames) {
            this.primaryKey = new UnresolvedPrimaryKey(constraintName, columnNames);
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
            this.columnName =
                    Preconditions.checkNotNull(columnName, "Column name must not be null.");
        }
    }

    static class UnresolvedPhysicalColumn extends UnresolvedColumn {
        final AbstractDataType<?> dataType;

        UnresolvedPhysicalColumn(String columnName, AbstractDataType<?> dataType) {
            super(columnName);
            this.dataType = Preconditions.checkNotNull(dataType, "Data type must not be null.");
        }
    }

    static class UnresolvedComputedColumn extends UnresolvedColumn {
        final Expression expression;

        UnresolvedComputedColumn(String columnName, Expression expression) {
            super(columnName);
            this.expression =
                    Preconditions.checkNotNull(expression, "Expression must not be null.");
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
            this.dataType = Preconditions.checkNotNull(dataType, "Data type must not be null.");
            this.metadataKey = metadataKey;
            this.isVirtual = isVirtual;
        }
    }

    static class UnresolvedWatermarkSpec {
        final String timeColumnName;
        final Expression expression;

        UnresolvedWatermarkSpec(String timeColumnName, Expression expression) {
            this.timeColumnName =
                    Preconditions.checkNotNull(
                            timeColumnName, "Time column name must not be null.");
            this.expression =
                    Preconditions.checkNotNull(expression, "Expression must not be null.");
        }
    }

    abstract static class UnresolvedConstraint {
        final @Nullable String constraintName;

        UnresolvedConstraint(@Nullable String constraintName) {
            this.constraintName = constraintName;
        }
    }

    static class UnresolvedPrimaryKey extends UnresolvedConstraint {
        final String[] columnNames;

        UnresolvedPrimaryKey(@Nullable String constraintName, String[] columnNames) {
            super(constraintName);
            this.columnNames =
                    Preconditions.checkNotNull(columnNames, "Column names must not be null.");
        }
    }
}
