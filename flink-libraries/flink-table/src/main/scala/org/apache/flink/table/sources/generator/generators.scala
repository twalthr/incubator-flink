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

import org.apache.flink.table.descriptors.DescriptorProperties

class IntGenerator extends DataGenerator[Integer] {

  private val seed: Option[Long] = _
  private val min: Int = Int.MinValue
  private val max: Int = Int.MaxValue

  private var random: Random = _

  override def configure(properties: DescriptorProperties): Unit = {
    properties.getDouble()
  }

  override def open(): Unit = {
    random = seed match {
      case Some(s) => new Random(s)
      case None => new Random()
    }
  }

  override def generate(context: DataGeneratorContext): Integer = {
    if () {

    }
  }
}
