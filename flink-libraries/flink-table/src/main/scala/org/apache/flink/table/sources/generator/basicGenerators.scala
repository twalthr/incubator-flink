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

import org.apache.flink.table.descriptors.DescriptorProperties
import org.apache.flink.table.sources.generator.DataGeneratorValidator._

class BooleanGenerator extends DataGenerator[Boolean] {

  override def generate(context: DataGeneratorContext): Boolean = {
    context.random.nextBoolean()
  }
}

class IntGenerator extends DataGenerator[Int] {

  var min: Int = Int.MinValue
  var max: Int = Int.MaxValue

  override def configure(properties: DescriptorProperties): Unit = {
    properties.getInt(MIN_VALUE).foreach(min = _)
    properties.getInt(MAX_VALUE).foreach(max = _)
  }

  override def generate(context: DataGeneratorContext): Int = {
    context.random.nextInt((max - min) + 1) + min
  }
}

class LongGenerator extends DataGenerator[Long] {

  var min: Long = Long.MinValue
  var max: Long = Long.MaxValue

  override def configure(properties: DescriptorProperties): Unit = {
    properties.getLong(MIN_VALUE).foreach(min = _)
    properties.getLong(MAX_VALUE).foreach(max = _)
  }

  override def generate(context: DataGeneratorContext): Long = {
    min + (context.random.nextDouble() * (max - min + 1L)).toLong
  }
}

class ByteGenerator extends DataGenerator[Byte] {

  var min: Byte = Byte.MinValue
  var max: Byte = Byte.MaxValue

  override def configure(properties: DescriptorProperties): Unit = {
    properties.getByte(MIN_VALUE).foreach(min = _)
    properties.getByte(MAX_VALUE).foreach(max = _)
  }

  override def generate(context: DataGeneratorContext): Byte = {
    val r: Byte = (min + context.random.nextInt().toByte * (max - min + 1.toByte)).toByte
    r
  }
}

class ShortGenerator extends DataGenerator[Short] {

  var min: Short = Short.MinValue
  var max: Short = Short.MaxValue

  override def configure(properties: DescriptorProperties): Unit = {
    properties.getByte(MIN_VALUE).foreach(min = _)
    properties.getByte(MAX_VALUE).foreach(max = _)
  }

  override def generate(context: DataGeneratorContext): Short = {
    val r: Short = (min + context.random.nextInt().toShort * (max - min + 1.toShort)).toShort
    r
  }
}

class DoubleGenerator extends DataGenerator[Double] {

  var min: Double = Double.MinValue
  var max: Double = Double.MaxValue

  override def configure(properties: DescriptorProperties): Unit = {
    properties.getDouble(MIN_VALUE).foreach(min = _)
    properties.getDouble(MAX_VALUE).foreach(max = _)
  }

  override def generate(context: DataGeneratorContext): Double = {
    min + (context.random.nextDouble() * (max - min + 1.0d))
  }
}

class FloatGenerator extends DataGenerator[Float] {

  var min: Float = Float.MinValue
  var max: Float = Float.MaxValue

  override def configure(properties: DescriptorProperties): Unit = {
    properties.getFloat(MIN_VALUE).foreach(min = _)
    properties.getFloat(MAX_VALUE).foreach(max = _)
  }

  override def generate(context: DataGeneratorContext): Float = {
    min + (context.random.nextFloat() * (max - min + 1.0f))
  }
}

// --------------------------------------------------------------------------

class EnumGenerator[T] extends DataGenerator[T] {

  var array: Array[T] = Array()

  val intGenerator: DataGenerator[Int] = {
    val gen = new IntGenerator
    gen.min = 0
    gen.max = array.length
    gen
  }

  override def configure(properties: DescriptorProperties): Unit = {

  }

  override def generate(context: DataGeneratorContext): T = {
    if (array.length == 0) {
      null.asInstanceOf[T]
    } else {
      array(intGenerator.generate(context))
    }
  }
}

//class ObjectArrayGenerator[E] extends DataGenerator[Array[E]] {
//
//  private var minLen: Int = 0
//  private var maxLen: Int = 10 // int max value would be too large
//
//  private var fieldGenerator: DataGenerator[E] = _
//
//  override def configure(properties: DescriptorProperties): Unit = {
//    properties.getFloat(MIN_LENGTH).foreach(minLen = _)
//    properties.getFloat(MAX_LENGTH).foreach(maxLen = _)
//    DataGeneratorHub.createDataGenerator(properties.getPrefix())
//  }
//
//  override def generate(context: DataGeneratorContext): Array[E] = {
//    val len = if (minLen == maxLen) {
//      minLen
//    } else {
//      context.random.nextInt((maxLen - minLen) + 1) + minLen
//    }
//
//    java.lang.reflect.Array.newInstance()
//  }
//}

