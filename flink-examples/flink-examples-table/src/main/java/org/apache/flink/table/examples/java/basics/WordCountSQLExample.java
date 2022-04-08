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

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.callSql;
import static org.apache.flink.table.api.Expressions.row;

/** The famous word count example that shows a minimal Flink SQL job in batch execution mode. */
public final class WordCountSQLExample {

    public static void main(String[] args) throws Exception {

        // set up the Table API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporarySystemFunction("MySpecificFun", MySpecificFun.class);

        Table t = tableEnv.fromDataStream(env.fromElements(1, 2, 3));

        t.select(call(MySpecificFun.class, $("f0"), "Hello")).execute().print();
    }

    public static class MySpecificFun extends ScalarFunction implements SpecializedFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .typedArguments(DataTypes.INT(), DataTypes.STRING())
                    .outputTypeStrategy(callContext -> Optional.of(DataTypes.STRING()))
                    .build();
        }

        public Object eval(Object... o) {
            return null;
        }

        @Override
        public UserDefinedFunction specialize(SpecializedContext context) {
            final ExpressionEvaluator evaluator =
                    context.createEvaluator(
                            row(
                                    callSql("b + 12 + CAST(MySpecificFun(b, 'hello') AS INT)"),
                                    call(MyScalar.class, $("b"))),
                            DataTypes.ROW(
                                    DataTypes.FIELD("a", DataTypes.INT()),
                                    DataTypes.FIELD("b", DataTypes.INT())),
                            DataTypes.FIELD("a", DataTypes.INT()),
                            DataTypes.FIELD("b", DataTypes.INT()));
            return new SpecialScalar(evaluator);
        }

        public static class SpecialScalar extends ScalarFunction {
            private final ExpressionEvaluator rowEvaluator;
            private transient MethodHandle rowHandle;

            public SpecialScalar(ExpressionEvaluator rowEvaluator) {
                this.rowEvaluator = rowEvaluator;
            }

            @Override
            public void open(FunctionContext context) {
                rowHandle = rowEvaluator.openHandle(context);
            }

            public String eval(Integer i, String s) {
                try {
                    return ((Row) rowHandle.invokeExact(i, i)).toString();
                } catch (Throwable t) {
                    throw new FlinkRuntimeException(t);
                }
            }
        }
    }

    public static class MyScalar extends ScalarFunction {
        public Integer eval(Integer b) {
            return b + 1;
        }
    }
}
