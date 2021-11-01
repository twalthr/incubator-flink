package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.StringUtils;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;

public class TestClass {

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Test
    public void testClass() {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        final SharedReference<List<String>> appliedKeys = sharedObjects.add(new ArrayList<>());
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(NoPushDownSource.SCHEMA)
                        .partitionKeys(Collections.singletonList("xxxx"))
                        .source(new NoPushDownSource(true, appliedKeys))
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);

        System.out.println(
                tableEnv.explainSql("SELECT d FROM T1 WHERE c = 100 AND xxxx = 'my-partition'"));
    }

    /** Source which supports metadata but not {@link SupportsProjectionPushDown}. */
    private static class NoPushDownSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsProjectionPushDown,
                    SupportsFilterPushDown,
                    SupportsPartitionPushDown {

        public static final Schema SCHEMA =
                Schema.newBuilder()
                        .column("c", INT())
                        .column("f", INT())
                        .column("d", STRING())
                        .build();

        private final boolean supportsMetadataProjection;
        private final SharedReference<List<String>> appliedMetadataKeys;

        public NoPushDownSource(
                boolean supportsMetadataProjection,
                SharedReference<List<String>> appliedMetadataKeys) {
            this.supportsMetadataProjection = supportsMetadataProjection;
            this.appliedMetadataKeys = appliedMetadataKeys;
        }

        @Override
        public boolean supportsNestedProjection() {
            return false;
        }

        @Override
        public void applyProjection(int[][] projectedFields) {
            System.out.println("Projections: " + StringUtils.arrayAwareToString(projectedFields));
        }

        @Override
        public Result applyFilters(List<ResolvedExpression> filters) {
            System.out.println("Filters: " + filters);
            return Result.of(filters, filters);
        }

        @Override
        public Optional<List<Map<String, String>>> listPartitions() {
            Map<String, String> map = new HashMap<>();
            map.put("d", "my-partition");
            return Optional.of(Collections.singletonList(map));
        }

        @Override
        public void applyPartitions(List<Map<String, String>> remainingPartitions) {
            System.out.println("Apply Partition:" + remainingPartitions);
        }
    }
}
