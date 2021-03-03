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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A validated {@link CatalogTable} that is backed by the original metadata coming from the {@link
 * Catalog} but resolved by the framework.
 *
 * <p>Note: Compared to {@link CatalogTable}, instances of this class are serializable into a map of
 * string properties.
 */
@PublicEvolving
public final class ResolvedCatalogTable implements CatalogTable {

    private final CatalogTable origin;

    private final ResolvedSchema resolvedSchema;

    public ResolvedCatalogTable(CatalogTable origin, ResolvedSchema resolvedSchema) {
        this.origin =
                Preconditions.checkNotNull(origin, "Original catalog table must not be null.");
        this.resolvedSchema =
                Preconditions.checkNotNull(resolvedSchema, "Resolved schema must not be null.");
    }

    /**
     * Returns the original, unresolved {@link CatalogTable} from the {@link Catalog}.
     *
     * <p>This method might be useful if catalog-specific object instances should be directly
     * forwarded from the catalog to a {@link DynamicTableFactory}.
     */
    CatalogTable getOrigin() {
        return origin;
    }

    /**
     * Returns a fully resolved and validated {@link ResolvedSchema}.
     *
     * <p>Connectors can configure themselves by accessing {@link ResolvedSchema#getPrimaryKey()}
     * and {@link ResolvedSchema#toPhysicalRowDataType()}.
     */
    ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public Map<String, String> toProperties() {
        return CatalogPropertiesUtil.serializeCatalogTable(this);
    }

    /**
     * @deprecated This method returns the deprecated {@link TableSchema} class. The old class was a
     *     hybrid of resolved and unresolved schema information. It has been replaced by the new
     *     {@link ResolvedSchema} which is resolved by the framework and accessible via {@link
     *     #getResolvedSchema()}.
     */
    @Deprecated
    @Override
    public TableSchema getSchema() {
        return TableSchema.fromResolvedSchema(getResolvedSchema());
    }

    // --------------------------------------------------------------------------------------------
    // Delegations to original CatalogTable
    // --------------------------------------------------------------------------------------------

    @Override
    public Map<String, String> getOptions() {
        return origin.getOptions();
    }

    @Override
    public Schema getUnresolvedSchema() {
        return origin.getUnresolvedSchema();
    }

    @Override
    public String getComment() {
        return origin.getComment();
    }

    @Override
    public CatalogBaseTable copy() {
        return new ResolvedCatalogTable((CatalogTable) origin.copy(), resolvedSchema);
    }

    @Override
    public Optional<String> getDescription() {
        return origin.getDescription();
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return origin.getDetailedDescription();
    }

    @Override
    public boolean isPartitioned() {
        return origin.isPartitioned();
    }

    @Override
    public List<String> getPartitionKeys() {
        return origin.getPartitionKeys();
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        return new ResolvedCatalogTable(origin.copy(options), resolvedSchema);
    }
}
