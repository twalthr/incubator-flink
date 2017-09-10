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
import java.lang.{Boolean => JBoolean}
import java.net.URLClassLoader
import java.util.{Collections, Properties}

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.client.CliFrontend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010, Kafka010JsonTableSource}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.{Table, TableEnvironment, TableException, ValidationException}
import org.apache.flink.table.client.config.{CatalogParser, ConfigNode, ConfigParser}
import org.apache.flink.table.client.demo.ClickStreamTableSource
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.StdIn
import scala.util.Random

object SqlClient {

  def main(args: Array[String]): Unit = {
    new SqlClient().start(args)
  }

}

class SqlClient {

  def start(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val isEmbeddedMode: Boolean = params.getBoolean("embedded", false)
    val configPath: String = params.getRequired("config")
    val catalogPath: String = params.getRequired("catalog")
    val query: String = params.get("query")

    printWelcome()

    if (isEmbeddedMode) {
      println("> Executing client in EMBEDDED mode.")
    } else {
      println("> Executing client in CLUSTER mode.")
    }

    println("> Loading configuration from: " + configPath)

    // read config
    val config = ConfigParser.parseConfig(new File(configPath))

    println("> Loading catalog from: " + catalogPath)

    // initialize classloader

    val jarPath = if (config.jar != null && !config.jar.isEmpty) {
      config.jar
    } else {
      "DummyJar.jar"
    }
    val classLoader = new URLClassLoader(
      Array(new File(jarPath).toURI.toURL),
      this.getClass.getClassLoader)

    // read catalog
    val (tables, functions) = readCatalog(catalogPath, classLoader)

    println()
    println("> Welcome!")
    println()
    println("> Please enter SQL query or Q to exit.")
    println("> Terminate a running query by pressing ENTER")
    println("> Commands: 'SHOW TABLES', 'SHOW FUNCTIONS', 'SELECT ...', 'EXPLAIN ...'")

    // create and configure stream exec environment
    // we are using the Java environment in order to support dynamically registration of functions
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    if (config.watermarkInterval != null) {
      env.getConfig.setAutoWatermarkInterval(config.watermarkInterval)
    }
    env.getConfig.disableSysoutLogging()

    // register tables
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tables.foreach(t => tEnv.registerTableSource(t._1, t._2))

    // register functions
    functions.foreach { case (name, function) =>
        function match {
          case sf: ScalarFunction =>
            tEnv.registerFunction(name, sf)
          case tf: TableFunction[_] =>
            tEnv.registerFunction(name, tf)
          case af: AggregateFunction[_, _] =>
            tEnv.registerFunction(name, af)
          case c@_ =>
            throw new ValidationException(s"Unsupported function type: ${c.getClass}")
        }
    }

    // we have no way to gracefully stop the query.
    var continue: Boolean = true
    while (continue) {

      println()
      print("> ")

      val action = if (query == null || query.isEmpty) {
        // read action from CLI
        StdIn.readLine().trim
      } else {
        // execute pre-defined action and exit
        continue = false
        query
      }

      if (action == "Q") {
        continue = false
      } else if (action.toUpperCase == "SHOW TABLES") {
        tEnv.listTables().foreach { t =>
          println(t)
          println(tEnv.scan(t).getSchema)
        }
      } else if (action.toUpperCase == "SHOW FUNCTIONS") {
        tEnv.listUserDefinedFunctions().foreach { t =>
          println(t)
        }
      } else if (action.toUpperCase.startsWith("EXPLAIN ")) {
        try {
          val tableOrStmt = action.substring(8)
          val explain = if (!action.contains("SELECT")) {
            tEnv.scan(tableOrStmt).getSchema
          } else {
            tEnv.explain(tEnv.sql(tableOrStmt))
          }
          println(explain)
        } catch {
          case e: Exception =>
            println("> Query could not be executed:")
            println(e.getMessage)
        }
      } else if (action != "") {

        // parse query
        val result: Option[Table] = try {
          Some(tEnv.sql(action))
        } catch {
          case e: Exception =>
            println("> Query could not be parsed:")
            println(e.getMessage)
            None
        }

        // ready to be executed or submitted
        if (result.isDefined) {

          // prepare output
          val (sink, outputThread, cleanUp) = config.output match {
            case "kafka" =>
              createKafkaOutput(config)
            case o@_ =>
              throw ValidationException(s"Unsupported output '$o'.")
          }

          try {
            // try to create append stream
            tEnv.toAppendStream(result.get, classOf[Row])
              .map(new MapFunction[Row, String] {
                override def map(value: Row): String = value.toString
              })
              .addSink(sink)
          } catch {
            case _: Exception =>
              tEnv.toRetractStream(result.get, classOf[Row])
                .map(new MapFunction[JTuple2[JBoolean, Row], String] {

                  def map(value: JTuple2[JBoolean, Row]): String = value.toString
                })
                .addSink(sink)
          }

          outputThread.start()

          // create execution thread
          val exceptionHandler = new ExceptionHandler
          val execThread = new Thread(
            new Runnable {
              override def run(): Unit = {
                if (isEmbeddedMode) {
                  env.execute()
                } else {
                  CliFrontend.main(Array(
                    "run", // run Flink job
                    "-c", classOf[SqlClient].getName, // use this client for submission
                    jarPath, // ship jar
                    "-embedded", // call execute in cluster configured environment
                    "-config", configPath,
                    "-catalog", catalogPath,
                    "-query", action)) // execute query immediately
                }
              }
            }, "Query Execution")
          execThread.setUncaughtExceptionHandler(exceptionHandler)

          // start query execution
          println("> Query execution starts.")
          execThread.start()

          // wait for <ENTER> to terminate query
          if (query == null || query.isEmpty) {
            StdIn.readLine()
          }

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

  def readCatalog(catalogPath: String, classLoader: ClassLoader)
    : (Seq[(String, StreamTableSource[_])], Seq[(String, UserDefinedFunction)]) = {

    val catalogNode = CatalogParser.parseCatalog(new File(catalogPath))

    // create table sources
    val sources: Seq[(String, StreamTableSource[_])] = catalogNode.tables.asScala.map { t =>
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

    // create functions
    val functions: Seq[(String, UserDefinedFunction)] = if (catalogNode.functions != null) {

      catalogNode.functions.asScala.map { f =>
        val clazz = try {
           Class.forName(f.clazz, true, classLoader)
        } catch {
          case e: ClassNotFoundException =>
            throw TableException(
              s"Could not find class '${f.clazz}' for registering '${f.name}'. " +
                s"Make sure that the class is available in your class path.", e)
          case e: ReflectiveOperationException =>
            throw TableException(
              s"Could not instantiate class '${f.clazz}' for registering '${f.name}'.", e)
        }

        val instance = if (f.parameters == null || f.parameters.isEmpty) {
          // use default constructor
          clazz.newInstance()
        } else {
          // find matching constructor for parameters
          // this is very basic we need to improve this by
          // using methods form UserDefinedFunctionUtils
          val paramClasses: Seq[Class[_]] = f.parameters.asScala.map(_.getClass)
          val constructor = clazz
            .getConstructors
            .toSeq
            .find { expected =>
              val expectedParam: Seq[Class[_]] = expected.getParameterTypes

              // parameters differ
              if (expectedParam.length != paramClasses.length) {
                false
              }
              // check all parameter types
              else {
                paramClasses
                  .zip(expectedParam)
                  .forall { case (act, exp) =>
                    UserDefinedFunctionUtils.parameterTypeEquals(act, exp)
                  }
              }
            }
            .getOrElse(throw TableException(
                s"Could not find matching constructor of class '${f.clazz}' for registering " +
                  s"'${f.name}'. Given parameters: ${paramClasses.mkString(", ")}"))

          constructor.newInstance(f.parameters.toArray: _*)
        }

        if (!instance.isInstanceOf[UserDefinedFunction]) {
          throw TableException(
            s"Class '${f.clazz}' for registering '${f.name}' is not a user-defined function.")
        }

        (f.name, instance.asInstanceOf[UserDefinedFunction])
      }
    } else {

      Seq()
    }

    (sources, functions)
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
