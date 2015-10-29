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

package org.apache.flink.api.table.sql

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._

class SqlTestUtil {
  private val tableRegistry = new TableRegistry
  private val env = ExecutionEnvironment.getExecutionEnvironment

  val table1 = env.fromElements((1, "A"), (2, "B"), (3, "C"), (4, "D")).as('FIELD1, 'FIELD2)
  tableRegistry.registerTable("TABLE1", table1)
  val table2 = env.fromElements((1, "A"), (2, "B"), (3, "C"), (4, "D")).as('FIELD1, 'FIELD2)
  tableRegistry.registerTable("TABLE2", table2)

  val translator = new SqlTranslator(tableRegistry)
}
