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

package org.apache.flink.table.plan.util

import org.apache.calcite.rel.{RelNode, RelShuttle}
import org.apache.calcite.rel.core.{TableFunctionScan, TableScan}
import org.apache.calcite.rel.logical._

/**
  * Implementation of [[org.apache.calcite.rel.RelShuttle]] that redirects all calls into generic
  * [[RexDefaultVisitor#visitNode(org.apache.calcite.rex.RexNode)]] method.
  */
abstract class RelDefaultShuttle extends RelShuttle {

  override def visit(intersect: LogicalIntersect): RelNode = visitNode(intersect)

  override def visit(union: LogicalUnion): RelNode = visitNode(union)

  override def visit(aggregate: LogicalAggregate): RelNode = visitNode(aggregate)

  override def visit(minus: LogicalMinus): RelNode = visitNode(minus)

  override def visit(sort: LogicalSort): RelNode = visitNode(sort)

  override def visit(matchNode: LogicalMatch): RelNode = visitNode(matchNode)

  override def visit(other: RelNode): RelNode = visitNode(other)

  override def visit(exchange: LogicalExchange): RelNode = visitNode(exchange)

  override def visit(scan: TableScan): RelNode = visitNode(scan)

  override def visit(scan: TableFunctionScan): RelNode = visitNode(scan)

  override def visit(values: LogicalValues): RelNode = visitNode(values)

  override def visit(filter: LogicalFilter): RelNode = visitNode(filter)

  override def visit(project: LogicalProject): RelNode = visitNode(project)

  override def visit(join: LogicalJoin): RelNode = visitNode(join)

  override def visit(correlate: LogicalCorrelate): RelNode = visitNode(correlate)

  def visitNode(node: RelNode): RelNode
}
