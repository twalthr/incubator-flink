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

package org.apache.flink.table.sources.generator

import org.apache.flink.api.common.functions.RuntimeContext

class DataGeneratorContext(private val maxCount: Long, private val context: RuntimeContext) {

  val localMaxCount: Long = {
    var localMaxCount = Long.MaxValue
    // split count by subtask
    if (maxCount < Long.MaxValue) {
      localMaxCount = maxCount / context.getNumberOfParallelSubtasks
      // the first subtask takes the remaining
      if (context.getIndexOfThisSubtask == 0) {
        localMaxCount += maxCount % context.getNumberOfParallelSubtasks
      }
    }
    localMaxCount
  }

  var localCount: Long = 0L
}
