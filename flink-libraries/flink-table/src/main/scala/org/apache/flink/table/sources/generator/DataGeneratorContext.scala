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
    private val blankSeed: Option[Long],
    private val maxCount: Long,
    private val fieldBlankProbability: Array[Double],
    private val generatorBlankProbability: Array[Double],
    private val context: RuntimeContext) {

  val blankRandom: Random = blankSeed match {
    case Some(s) => new Random(s)
    case None => new Random()
  }

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
  val localGeneratorBlankCounts: Array[Long] = new Array(generatorBlankProbability.length)
  val localFieldBlankCounts: Array[Long] = new Array(fieldBlankProbability.length)

  def isGeneratorBlank(generator: Int): Boolean = {
    val probability = generatorBlankProbability(generator)
    if (probability == 0) {
      return false // no blanks at all
    }

    val isBlank = blankRandom.nextDouble() <= probability
    // maximum count is not set, always use probability
    if (maxCount == Long.MaxValue) {
      if (isBlank) {
        localGeneratorBlankCounts(generator) += 1
      }
      isBlank
    }
    // use probability as long as it guaranteed that the ratio is achieved
    else {
      val expectedBlanks = localMaxCount * generatorBlankProbability(generator)
      val actualBlanks = localGeneratorBlankCounts(generator)
      val remaining = localMaxCount - localCount
      // blank count reached
      if (expectedBlanks == actualBlanks) {
        false
      }
      // enough records remaining to achieve ratio
      else if (actualBlanks + (remaining * probability) > expectedBlanks) {
        if (isBlank) {
          localGeneratorBlankCounts(generator) += 1
        }
        isBlank
      }
      // blanks needed to achieve ratio
      else {
        localGeneratorBlankCounts(generator) += 1
        true
      }
    }
  }

  def isFieldBlank(field: Int): Boolean = {
  }

  private def maintainBlank(blankProbabilities:): Unit = {
    val probability = generatorBlankProbability(generator)
    if (probability == 0) {
      return false // no blanks at all
    }

    val isBlank = blankRandom.nextDouble() <= probability
    // maximum count is not set, always use probability
    if (maxCount == Long.MaxValue) {
      if (isBlank) {
        localGeneratorBlankCounts(generator) += 1
      }
      isBlank
    }
    // use probability as long as it guaranteed that the ratio is achieved
    else {
      val expectedBlanks = localMaxCount * generatorBlankProbability(generator)
      val actualBlanks = localGeneratorBlankCounts(generator)
      val remaining = localMaxCount - localCount
      // blank count reached
      if (expectedBlanks == actualBlanks) {
        false
      }
      // enough records remaining to achieve ratio
      else if (actualBlanks + (remaining * probability) > expectedBlanks) {
        if (isBlank) {
          localGeneratorBlankCounts(generator) += 1
        }
        isBlank
      }
      // blanks needed to achieve ratio
      else {
        localGeneratorBlankCounts(generator) += 1
        true
      }
    }
  }
}
