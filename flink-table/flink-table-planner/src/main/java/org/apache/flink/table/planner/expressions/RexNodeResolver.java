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

package org.apache.flink.table.planner.expressions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver.ExpressionResolverBuilder;
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

/** Utilities to resolve {@link Expression} to {@link RexNode}. */
@Internal
public class RexNodeResolver {

    private final ExpressionResolverBuilder resolverBuilder;

    private final ExpressionConverter converter;

    public RexNodeResolver(
            ExpressionResolverBuilder expressionResolverBuilder, RelBuilder relBuilder) {
        this.resolverBuilder = expressionResolverBuilder;
        this.converter = new ExpressionConverter(relBuilder);
    }
}
