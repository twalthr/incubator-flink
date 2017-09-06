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
import java.lang.Thread.UncaughtExceptionHandler
import java.util.{Collections, Properties}

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010, Kafka010JsonTableSource}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.{Table, TableEnvironment, ValidationException}
import org.apache.flink.table.client.config.{CatalogParser, ConfigNode, ConfigParser}
import org.apache.flink.table.client.demoSource.ClickStreamTableSource
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.StdIn
import scala.util.Random

object SqlClient {

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val configPath: String = params.getRequired("config")
    val catalogPath: String = params.getRequired("catalog")

    printWelcome()

    println("> Loading configuration from: " + configPath)

    // read config
    val config: ConfigNode = ConfigParser.parseConfig(new File(configPath))

    val watermarkInterval: Long = config.watermarkInterval

    println("> Loading catalog from: " + catalogPath)

    // read catalog
    val tables: Seq[(String, StreamTableSource[_])] = readCatalog(catalogPath)

    println()
    println("> Welcome!")
    println()
    println("> Please enter SQL query or Q to exit.")
    println("> Terminate a running query by pressing ENTER")
    println("> Use 'SHOW TABLES' to print available tables.")

    // create and configure stream exec environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(watermarkInterval)

    // register tables
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tables.foreach(t => tEnv.registerTableSource(t._1, t._2))

    // we have no way to gracefully stop the query.
    var continue: Boolean = true
    while (continue) {

      println()
      print("> ")

      val query = StdIn.readLine().trim

      if (query == "Q") {
        continue = false
      } else if (query.toUpperCase == "SHOW TABLES") {
        tEnv.listTables().asScala.foreach { t =>
          println(t)
          println(tEnv.scan(t).getSchema)
        }
      } else if (query.toUpperCase.startsWith("EXPLAIN ")) {
        try {
          val tableOrStmt = query.substring(8)
          val table = if (!query.contains("SELECT")) {
            tEnv.scan(tableOrStmt)
          } else {
            tEnv.sql(tableOrStmt)
          }
          println(tEnv.explain(table))
        } catch {
          case e: Exception =>
            println("> Query could not be executed:")
            println(e.getMessage)
        }
      } else {

        // parse query
        val result: Option[Table] = try {
          Some(tEnv.sql(query))
        } catch {
          case e: Exception =>
            println("> Query could not be parsed:")
            println(e.getMessage)
            None
        }

        if (result.isDefined) {

          // prepare output
          val (sink, outputThread, cleanUp) = config.output match {
            case "kafka" =>
              createKafkaOutput(config)
            case o@_ =>
              throw new ValidationException(s"Unsupported output '$o'.")
          }

          try {
            // try to create append stream
            tEnv.toAppendStream[Row](result.get)
              .map(r => r.toString)
              .addSink(sink)
          } catch {
            case _: Exception =>
              tEnv.toRetractStream[Row](result.get)
                .map(r => r.toString())
                .addSink(sink)
          }

          outputThread.start()

          // create execution thread
          val exceptionHandler = new ExceptionHandler
          val execThread = new Thread(
            new Runnable {
              override def run(): Unit = env.execute()
            }, "Query Execution")
          execThread.setUncaughtExceptionHandler(exceptionHandler)

          // start query execution
          println("> Query execution starts.")
          execThread.start()

          // wait for <ENTER> to terminate query
          StdIn.readLine()

          // stop execution thread hard (there is no way to stop it gracefully).
          execThread.interrupt()
          execThread.join()
          exceptionHandler.collected.foreach { t =>
            println("> Terminated with exception:")
            t.printStackTrace(System.out)
          }

          // clean up
          cleanUp()
        }

        println("> Query execution terminated.")
      }
    }
  }

  def readCatalog(catalogPath: String): Seq[(String, StreamTableSource[_])] = {

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
              val types = t.encoding.schema.map(_.convertType())
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

  def createKafkaOutput(config: ConfigNode): (SinkFunction[String], Thread, () => Unit) = {
    val outputTopicPrefix = config.defaults.kafka.outputTopicPrefix
    val outputKafkaProps = new Properties()
    outputKafkaProps.putAll(config.defaults.kafka.properties)

    // create output topic with 1 partition and replication 1
    val outputTopicName = getRandomOutputTopic(outputTopicPrefix)
    createKafkaTopic(outputTopicName, outputKafkaProps)

    // create Kafka producer
    val producer = new FlinkKafkaProducer010(
      outputTopicName,
      new SimpleStringSchema(),
      outputKafkaProps)

    // create a thread to print query results
    val topicPrinter = new TopicPrinter(outputTopicName, outputKafkaProps)
    val outputThread = new Thread(topicPrinter, "Topic Printer")

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

    // clean up procedure
    val cleanUp = () => {
      // stop printing of query results
      topicPrinter.stopPrinting()
      // delete Kafka output topic
      deleteKafkaTopic(outputTopicName, outputKafkaProps)
    }

    (producer, outputThread, cleanUp)
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

  class ExceptionHandler extends UncaughtExceptionHandler {
    val collected = new mutable.ArrayBuffer[Throwable]()
    override def uncaughtException(t: Thread, e: Throwable): Unit = {
      collected += e
    }
  }

  def printWelcome() {
    println(
      // scalastyle:off
      """
                         \u2592\u2593\u2588\u2588\u2593\u2588\u2588\u2592
                     \u2593\u2588\u2588\u2588\u2588\u2592\u2592\u2588\u2593\u2592\u2593\u2588\u2588\u2588\u2593\u2592
                  \u2593\u2588\u2588\u2588\u2593\u2591\u2591        \u2592\u2592\u2592\u2593\u2588\u2588\u2592  \u2592
                \u2591\u2588\u2588\u2592   \u2592\u2592\u2593\u2593\u2588\u2593\u2593\u2592\u2591      \u2592\u2588\u2588\u2588\u2588
                \u2588\u2588\u2592         \u2591\u2592\u2593\u2588\u2588\u2588\u2592    \u2592\u2588\u2592\u2588\u2592
                  \u2591\u2593\u2588            \u2588\u2588\u2588   \u2593\u2591\u2592\u2588\u2588
                    \u2593\u2588       \u2592\u2592\u2592\u2592\u2592\u2593\u2588\u2588\u2593\u2591\u2592\u2591\u2593\u2593\u2588
                  \u2588\u2591 \u2588   \u2592\u2592\u2591       \u2588\u2588\u2588\u2593\u2593\u2588 \u2592\u2588\u2592\u2592\u2592
                  \u2588\u2588\u2588\u2588\u2591   \u2592\u2593\u2588\u2593      \u2588\u2588\u2592\u2592\u2592 \u2593\u2588\u2588\u2588\u2592
               \u2591\u2592\u2588\u2593\u2593\u2588\u2588       \u2593\u2588\u2592    \u2593\u2588\u2592\u2593\u2588\u2588\u2593 \u2591\u2588\u2591
         \u2593\u2591\u2592\u2593\u2588\u2588\u2588\u2588\u2592 \u2588\u2588         \u2592\u2588    \u2588\u2593\u2591\u2592\u2588\u2592\u2591\u2592\u2588\u2592
        \u2588\u2588\u2588\u2593\u2591\u2588\u2588\u2593  \u2593\u2588           \u2588   \u2588\u2593 \u2592\u2593\u2588\u2593\u2593\u2588\u2592
      \u2591\u2588\u2588\u2593  \u2591\u2588\u2591            \u2588  \u2588\u2592 \u2592\u2588\u2588\u2588\u2588\u2588\u2593\u2592 \u2588\u2588\u2593\u2591\u2592
     \u2588\u2588\u2588\u2591 \u2591 \u2588\u2591          \u2593 \u2591\u2588 \u2588\u2588\u2588\u2588\u2588\u2592\u2591\u2591    \u2591\u2588\u2591\u2593  \u2593\u2591
    \u2588\u2588\u2593\u2588 \u2592\u2592\u2593\u2592          \u2593\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2593\u2591       \u2592\u2588\u2592 \u2592\u2593 \u2593\u2588\u2588\u2593
 \u2592\u2588\u2588\u2593 \u2593\u2588 \u2588\u2593\u2588       \u2591\u2592\u2588\u2588\u2588\u2588\u2588\u2593\u2593\u2592\u2591         \u2588\u2588\u2592\u2592  \u2588 \u2592  \u2593\u2588\u2592
 \u2593\u2588\u2593  \u2593\u2588 \u2588\u2588\u2593 \u2591\u2593\u2593\u2593\u2593\u2593\u2593\u2593\u2592              \u2592\u2588\u2588\u2593           \u2591\u2588\u2592
 \u2593\u2588    \u2588 \u2593\u2588\u2588\u2588\u2593\u2592\u2591              \u2591\u2593\u2593\u2593\u2588\u2588\u2588\u2593          \u2591\u2592\u2591 \u2593\u2588
 \u2588\u2588\u2593    \u2588\u2588\u2592    \u2591\u2592\u2593\u2593\u2588\u2588\u2588\u2593\u2593\u2593\u2593\u2593\u2588\u2588\u2588\u2588\u2588\u2588\u2593\u2592            \u2593\u2588\u2588\u2588  \u2588
\u2593\u2588\u2588\u2588\u2592 \u2588\u2588\u2588   \u2591\u2593\u2593\u2592\u2591\u2591   \u2591\u2593\u2588\u2588\u2588\u2588\u2593\u2591                  \u2591\u2592\u2593\u2592  \u2588\u2593
\u2588\u2593\u2592\u2592\u2593\u2593\u2588\u2588  \u2591\u2592\u2592\u2591\u2591\u2591\u2592\u2592\u2592\u2592\u2593\u2588\u2588\u2593\u2591                            \u2588\u2593
\u2588\u2588 \u2593\u2591\u2592\u2588   \u2593\u2593\u2593\u2593\u2592\u2591\u2591  \u2592\u2588\u2593       \u2592\u2593\u2593\u2588\u2588\u2593    \u2593\u2592          \u2592\u2592\u2593
\u2593\u2588\u2593 \u2593\u2592\u2588  \u2588\u2593\u2591  \u2591\u2592\u2593\u2593\u2588\u2588\u2592            \u2591\u2593\u2588\u2592   \u2592\u2592\u2592\u2591\u2592\u2592\u2593\u2588\u2588\u2588\u2588\u2588\u2592
 \u2588\u2588\u2591 \u2593\u2588\u2592\u2588\u2592  \u2592\u2593\u2593\u2592  \u2593\u2588                \u2588\u2591      \u2591\u2591\u2591\u2591   \u2591\u2588\u2592
 \u2593\u2588   \u2592\u2588\u2593   \u2591     \u2588\u2591                \u2592\u2588              \u2588\u2593
  \u2588\u2593   \u2588\u2588         \u2588\u2591                 \u2593\u2593        \u2592\u2588\u2593\u2593\u2593\u2592\u2588\u2591
   \u2588\u2593 \u2591\u2593\u2588\u2588\u2591       \u2593\u2592                  \u2593\u2588\u2593\u2592\u2591\u2591\u2591\u2592\u2593\u2588\u2591    \u2592\u2588
    \u2588\u2588   \u2593\u2588\u2593\u2591      \u2592                    \u2591\u2592\u2588\u2592\u2588\u2588\u2592      \u2593\u2593
     \u2593\u2588\u2592   \u2592\u2588\u2593\u2592\u2591                         \u2592\u2592 \u2588\u2592\u2588\u2593\u2592\u2592\u2591\u2591\u2592\u2588\u2588
      \u2591\u2588\u2588\u2592    \u2592\u2593\u2593\u2592                     \u2593\u2588\u2588\u2593\u2592\u2588\u2592 \u2591\u2593\u2593\u2593\u2593\u2592\u2588\u2593
        \u2591\u2593\u2588\u2588\u2592                          \u2593\u2591  \u2592\u2588\u2593\u2588  \u2591\u2591\u2592\u2592\u2592
            \u2592\u2593\u2593\u2593\u2593\u2593\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2591\u2591\u2593\u2593  \u2593\u2591\u2592\u2588\u2591

             F L I N K - S Q L - C L I E N T

      """
    // scalastyle:on
    )

  }

}
