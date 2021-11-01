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
import org.apache.flink.util.StringUtils;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;

public class TestClass {

    @Test
    public void testClass() {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(TestPushDownSource.SCHEMA)
                        .partitionKeys(Collections.singletonList("d"))
                        .source(new TestPushDownSource())
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);

        System.out.println(tableEnv.explainSql("SELECT d FROM T1 WHERE c = 100 AND d > '2012'"));
    }

    private static class TestPushDownSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsProjectionPushDown,
                    SupportsFilterPushDown,
                    SupportsPartitionPushDown {

        public static final Schema SCHEMA =
                Schema.newBuilder()
                        .column("c", INT())
                        .column("f", INT())
                        .column("d", STRING())
                        .build();

        @Override
        public boolean supportsNestedProjection() {
            return false;
        }

        @Override
        public void applyProjection(int[][] projectedFields) {
            System.out.println("Projections: " + StringUtils.arrayAwareToString(projectedFields));
        }

        @Override
        public Optional<List<Map<String, String>>> listPartitions() {
            Map<String, String> map = new HashMap<>();
            map.put("d", "2012");
            return Optional.of(Collections.singletonList(map));
        }

        @Override
        public void applyPartitions(List<Map<String, String>> remainingPartitions) {
            System.out.println("Apply Partition:" + remainingPartitions);
        }

        @Override
        public Result applyFilters(List<ResolvedExpression> filters) {
            return Result.of(Collections.emptyList(), filters);
        }
    }
}
