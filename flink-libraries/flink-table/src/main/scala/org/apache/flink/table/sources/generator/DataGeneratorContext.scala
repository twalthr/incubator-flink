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

import java.util.Random

import org.apache.flink.api.common.functions.RuntimeContext

/**
  * Context during data generation for keeping track of global attributes such as
  * the blank ratio, counts etc.
  */
class DataGeneratorContext(
    private val seed: Option[Long],
    private val maxCount: Long,
    private val fieldBlankProbability: Array[Double],
    private val generatorBlankProbability: Array[Double],
    private val context: RuntimeContext) {

  val random: Random = seed match {
    case Some(s) => new Random(s)
    case None => new Random()
  }

  val subtaskIndex: Int = context.getIndexOfThisSubtask
  val subtaskCount: Int = context.getNumberOfParallelSubtasks

  val localMaxCount: Long = {
    var localMaxCount = Long.MaxValue
    // split count by subtask
    if (maxCount < Long.MaxValue) {
      localMaxCount = maxCount / subtaskCount
      // the first subtask takes the remainder
      if (subtaskIndex == 0) {
        localMaxCount += maxCount % subtaskCount
      }
    }
    localMaxCount
  }

  var localCount: Long = 0L
  val localGeneratorBlankCounts: Array[Long] = new Array(generatorBlankProbability.length)
  val localFieldBlankCounts: Array[Long] = new Array(fieldBlankProbability.length)

  def uniqueIndex: Long = {
    // data is bounded so we can use a consecutive index
    if (localMaxCount < Long.MaxValue) {
      // the first subtask takes the remainder
      val remainder = if (subtaskIndex > 0) { maxCount % subtaskCount } else { 0 }
      val startIndex = (maxCount / subtaskCount) * subtaskIndex + remainder
      startIndex + localCount
    }
    // use an index with gaps
    else {
      // build an index [2 byte subtask, 6 bytes local count]
      // we only support clusters up to parallelism 32767
      subtaskIndex.toLong << 48 | localCount & 0x00FFFFFFL
    }
  }

  def isGeneratorBlank(generator: Int): Boolean = {
    isAchievingRatio(generator, generatorBlankProbability, localGeneratorBlankCounts)
  }

  def isFieldBlank(field: Int): Boolean = {
    isAchievingRatio(field, fieldBlankProbability, localFieldBlankCounts)
  }

  private def isAchievingRatio(
      idx: Int,
      probabilities: Array[Double],
      counts: Array[Long])
    : Boolean = {

    val probability = probabilities(idx)
    if (probability == 0) {
      return false // nothing to achieve
    }

    val isAchieving = random.nextDouble() <= probability
    // maximum count is not set, always use probability
    if (maxCount == Long.MaxValue) {
      if (isAchieving) {
        counts(idx) += 1
      }
      isAchieving
    }
    // use probability as long as it guaranteed that the ratio is achieved
    else {
      val expectedCounts = localMaxCount * probabilities(idx)
      val actualCounts = counts(idx)
      val remaining = localMaxCount - localCount
      // count reached
      if (expectedCounts == actualCounts) {
        false
      }
      // enough records remaining to achieve ratio
      else if (actualCounts + (remaining * probability) > expectedCounts) {
        if (isAchieving) {
          counts(idx) += 1
        }
        isAchieving
      }
      // need to achieve ratio
      else {
        counts(idx) += 1
        true
      }
    }
  }
}
