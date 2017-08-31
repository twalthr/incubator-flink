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

package org.apache.flink.table.client

import java.io.File
import java.util.{Collections, Properties}

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010, Kafka010JsonTableSource}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.{Table, TableEnvironment, Types, ValidationException}
import org.apache.flink.table.client.config.{CatalogParser, ConfigNode, ConfigParser}
import org.apache.flink.table.client.demoSource.ClickStreamTableSource
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConverters._
import scala.io.StdIn
import scala.util.Random

object SqlClient {

  def main(args: Array[String]): Unit = {

    println(
      """
        |####################
        |# FLINK SQL CLIENT #
        |####################
        |""".stripMargin)

    val catalogPath: String = "/Users/fhueske/testCatalog.json"
    val configPath: String = "/Users/fhueske/testConfig.json"

    println("> Loading configuration from: " + configPath)

    // read config
    val config: ConfigNode = ConfigParser.parseConfig(new File(configPath))
    // configure Kafka output
    val outputTopicPrefix = config.kafka.outputTopicPrefix
    val outputKafkaProps = new Properties()
    outputKafkaProps.putAll(config.kafka.properties)
    val watermarkInterval: Long = config.watermarkInterval

    println("> Loading catalog from: " + catalogPath)

    // read catalog
    val tables: Seq[(String, StreamTableSource[_])] = readCatalog(catalogPath)

    // create and configure stream exec environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(watermarkInterval)

    // register tables
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tables.foreach(t => tEnv.registerTableSource(t._1, t._2))

    // Loop doesn't work yet. We have no way to gracefully stop the query.
    var continue: Boolean = true
    while (continue) {

      println("> Please enter SQL query or Q to exit.")
      println("> Terminate a running query by pressing ENTER")

      val query = StdIn.readLine()

      if (query == "Q") {
        continue = false
      } else {

        // parse query
        val result: Option[Table] = try {
          Some(tEnv.sql(query))
        } catch {
          case e: Exception => {
            // TODO: we need better error messages than Calcite's exceptions.
            e.printStackTrace()
            None
          }
        }

        if (result.isDefined) {
          // create output topic with 1 partition and replication 1
          val outputTopicName = getRandomOutputTopic(outputTopicPrefix)
          createKafkaTopic(outputTopicName, outputKafkaProps)

          // create Kafka producer
          val producer = new FlinkKafkaProducer010(
            outputTopicName,
            new SimpleStringSchema(),
            outputKafkaProps)

          try {
            // try to create append stream
            tEnv.toAppendStream[Row](result.get)
              .map(r => r.toString)
              .addSink(producer)
          } catch {
            case _: Exception =>
              tEnv.toRetractStream[Row](result.get)
                .map(r => r.toString())
                .addSink(producer)
          }

          // start a thread to print query results
          val topicPrinter = new TopicPrinter(outputTopicName, outputKafkaProps)
          new Thread(topicPrinter, "Topic Printer").start()

          // register shutdown hook to stop output printer and delete output topic
          val mainThread = Thread.currentThread()
          Runtime.getRuntime.addShutdownHook(
            new Thread() {
              override def run(): Unit = {
                // stop printing of query results
                topicPrinter.stopPrinting()
                // delete Kakfa output topic
                deleteKafkaTopic(outputTopicName, outputKafkaProps)
                mainThread.join()
              }
            }
          )

          // create execution thread
          val execThread = new Thread(
            new Runnable {
              override def run(): Unit = env.execute()
            }, "Query Execution")

          // start query execution
          println("> Query execution starts.")
          execThread.start()

          // wait for <ENTER> to terminate query
          StdIn.readLine()

          // stop execution thread hard (there is no way to stop it gracefully).
          execThread.interrupt()
          execThread.join()
          println("> Query execution terminated.")

          // stop printing of query results
          topicPrinter.stopPrinting()
          // delete Kakfa output topic
          deleteKafkaTopic(outputTopicName, outputKafkaProps)

        }
      }
    }
  }

  def readCatalog(catalogPath: String): Seq[(String, StreamTableSource[_])] = {

    def tpeToTpeInfo(tpe: String): TypeInformation[_] = {
      tpe.toUpperCase match {
        case "BOOLEAN" => Types.BOOLEAN
        case "TINYINT" => Types.BYTE
        case "SMALLINT" => Types.SHORT
        case "INTEGER" => Types.INT
        case "BIGINT" => Types.LONG
        case "FLOAT" => Types.FLOAT
        case "DOUBLE" => Types.DOUBLE
        case "DECIMAL" => Types.DECIMAL
        case "TIMESTAMP" => Types.SQL_TIMESTAMP
        case "TIME" => Types.SQL_TIME
        case "DATE" => Types.SQL_DATE
        case "VARCHAR" => Types.STRING
        case _ => throw new ValidationException("Unknown type: " + tpe)
      }
    }

    val catalogNode = CatalogParser.parseCatalog(new File(catalogPath))

    catalogNode.tables.asScala.map { t =>
      val tableSource: StreamTableSource[_] = t.source.tpe match {
        case "kafka" =>

          val kafkaTableSource = t.encoding.tpe match {
            case "avro" => ??? // TODO: AVRO
            case "json" =>
              val kafkaProps = new Properties()
              kafkaProps.putAll(t.source.properties)

              val names = t.encoding.schema.map(_.name)
              val types = t.encoding.schema.map(f => tpeToTpeInfo(f.tpe))
              val schema = new RowTypeInfo(types, names)

              new Kafka010JsonTableSource(
                t.source.properties.get("topic"),
                kafkaProps,
                schema
              )
          }

          // configure time attributes
          if (t.proctime != null) kafkaTableSource.withProcTimeAttribute(t.proctime)
          if (t.rowtime != null) {
            t.rowtime.tpe match {
              case "ingestion-time" =>
                kafkaTableSource.withIngestionTimeAttribute(t.rowtime.field)
              case "event-time" =>
                t.rowtime.watermark.tpe match {
                  case "ascending" =>
                    kafkaTableSource.withAscendingRowTimeAttribute(t.rowtime.field)
                  case "bounded" =>
                    kafkaTableSource.withBoundedOutOfOrderRowtimeAttribute(
                      t.rowtime.field,
                      t.rowtime.watermark.lag)
                }
            }
          }
          kafkaTableSource
        case "demo" => t.source.properties.get("table") match {
          case "clickstream" => new ClickStreamTableSource()
        }

      }
      (t.name, tableSource)
    }
  }

  def getRandomOutputTopic(prefix: String): String = {
    prefix + "_" + Random.nextLong().toHexString
  }

  def createKafkaTopic(topicName: String, kafkaProps: Properties): Unit = {

    val topicConfig = new Properties()

    val zookeeperConnect = kafkaProps.getProperty("zookeeper.connect")
    val sessionTimeout = 10000
    val connTimeout = 8000

    val zkClient = new ZkClient(
      zookeeperConnect,
      sessionTimeout,
      connTimeout,
      ZKStringSerializer)

    val zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), false)
    AdminUtils.createTopic(zkUtils, topicName, 1, 1, topicConfig, RackAwareMode.Enforced)
    zkClient.close()

  }

  def deleteKafkaTopic(topicName: String, kafkaProps: Properties): Unit = {

    val zookeeperConnect = kafkaProps.getProperty("zookeeper.connect")
    val sessionTimeout = 10000
    val connTimeout = 8000

    val zkClient = new ZkClient(
      zookeeperConnect,
      sessionTimeout,
      connTimeout,
      ZKStringSerializer)

    val zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), false)
    AdminUtils.deleteTopic(zkUtils, topicName)
    zkClient.close()

  }

  object ZKStringSerializer extends ZkSerializer {

    @throws(classOf[ZkMarshallingError])
    def serialize(data : Object): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

    @throws(classOf[ZkMarshallingError])
    def deserialize(bytes : Array[Byte]): Object = {
      if (bytes == null) {
        null
      } else {
        new String(bytes, "UTF-8")
      }
    }
  }

  class TopicPrinter(topic: String, kafkaProps: Properties) extends Runnable {

    var printing: Boolean = true

    override def run(): Unit = {

      val consumerProps = new Properties()
      consumerProps.putAll(kafkaProps)
      consumerProps.put("group.id", "resultPrinter")
      consumerProps.put("enable.auto.commit", "true")
      consumerProps.put("auto.commit.interval.ms", "1000")
      consumerProps.put("session.timeout.ms", "30000")
      consumerProps.put(
        "key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")
      consumerProps.put(
        "value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")

      val consumer: KafkaConsumer[String, String] = new KafkaConsumer(consumerProps)
      consumer.subscribe(Collections.singleton(topic))

      while (printing) {
        val records: ConsumerRecords[String, String] = consumer.poll(100)

        records.iterator().asScala.foreach(r => println(r.value()))
        consumer.commitAsync()
      }

      consumer.close()
    }

    def stopPrinting(): Unit = {
      printing = false
    }

  }

}
