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

package org.apache.flink.table.client.demoSource

import org.apache.commons.lang3.RandomStringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, RowTypeInfo}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.Types
import org.apache.flink.table.sources.{DefinedProctimeAttribute, DefinedRowtimeAttribute, StreamTableSource}
import org.apache.flink.types.Row
import org.apache.flink.util.XORShiftRandom

import scala.collection.mutable.ListBuffer
import scala.io.Source

class ClickStreamTableSource
  extends StreamTableSource[Row]
    with DefinedRowtimeAttribute
    with DefinedProctimeAttribute {

  private def clickStream = new ClickStreamGenerator()

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.addSource(clickStream)
  }

  override def getRowtimeAttribute: String = "rowtime"

  override def getProctimeAttribute: String = "proctime"

  override def getReturnType: TypeInformation[Row] = {
    clickStream.getProducedType
  }

}


case class User(id: Long, ip: String)

class ClickStreamGenerator extends RichParallelSourceFunction[Row] with ResultTypeQueryable[Row] {

  private var running = true
  private var eventTimeMillis = 0L

  @transient
  private var categories: Seq[String] = _
  @transient
  private var users: ListBuffer[User] = _

  @throws[Exception]
  override def open(parameters: Configuration) {

    // event time starts now
    this.eventTimeMillis = System.currentTimeMillis

    // initialize catagories and users
    val in = classOf[ClickStreamGenerator].getClassLoader.getResourceAsStream("categories.txt")
    this.categories = Source.fromInputStream(in).getLines()
      .drop(18) // drop header
      .toSeq
    this.users = ListBuffer() ++ (0 until 50000).map(i => User(i.toLong, getNextIP))
  }

  @throws[Exception]
  def run(sourceContext: SourceFunction.SourceContext[Row]) {

    val numTasks = getRuntimeContext.getNumberOfParallelSubtasks
    val taskNo = getRuntimeContext.getIndexOfThisSubtask

    var sequenceNo = 0L
    while (running) {

      val u = getNextUser
      val ts = getNextTimestamp
      val url = getNextUrl
      val host = s"lb-brl-$numTasks-$taskNo.globalcorp.com"

      val click = Row.of(
        u.ip, // IP
        u.id.asInstanceOf[java.lang.Long], // Account
        url, // URL
        host, // Host
        sequenceNo.asInstanceOf[java.lang.Long]) // SequenceNo

      sourceContext.collectWithTimestamp(click, ts)
      sequenceNo += 1
      if (sequenceNo % 10000 == 0) {
        sourceContext.emitWatermark(new Watermark(ts - 60000)) // 1 min max delay
      }
    }
  }

  private def getNextIP: String = {
    val ip = ClickStreamGenerator.RND.nextInt

    val b1 = (ip >>> 24) & 0xff
    val b2 = (ip >>> 16) & 0xff
    val b3 = (ip >>> 8) & 0xff
    val b4 = ip & 0xff

    new StringBuilder()
      .append(b1).append('.')
      .append(b2).append('.')
      .append(b3).append('.')
      .append(b4).toString
  }

  private def getNextUser: User = {

    if (ClickStreamGenerator.RND.nextBoolean) {
      return User(-1L, getNextIP)
    }

    // replace an existing user by a new one
    users.insert(
      ClickStreamGenerator.RND.nextInt(users.size),
      User(ClickStreamGenerator.RND.nextInt(1000000), getNextIP)
    )
    // return a random user
    users(ClickStreamGenerator.RND.nextInt(users.size))
  }

  private def getNextUrl = {
    val sb = new StringBuffer
    val id = ClickStreamGenerator.RND.nextInt(3000000)
    id % 7 match {
      case 0 =>
      case 1 =>
        sb.append("/categories/")
        sb.append(categories(id % categories.size))
      case 2 =>
      case 3 =>
      case 4 =>
        sb.append("/productDetails/")
        sb.append(id / 10) // strip last digit
      case 5 =>
        sb.append("/blog/")
        sb.append(id / 10)
      case 6 =>
        sb.append("/search?q=")
        sb.append(RandomStringUtils.randomAlphabetic(id % 5))
    }
    sb.toString
  }

  private def getNextTimestamp = {
    this.eventTimeMillis += 10
    this.eventTimeMillis - ClickStreamGenerator.RND.nextInt(60 * 1000) // max delay of one minute
  }

  def cancel() {
    running = false
  }

  def getProducedType: TypeInformation[Row] = {
    val types = Array[TypeInformation[_]](
      Types.STRING,
      Types.LONG,
      Types.STRING,
      Types.STRING,
      Types.LONG)
    val names = Array[String]("ip", "userId", "url", "host", "seqNo")
    new RowTypeInfo(types, names)
  }
}

object ClickStreamGenerator {
  private val RND = new XORShiftRandom(1337)
}


