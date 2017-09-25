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

import java.io.{File, IOException}
import java.lang.Thread.UncaughtExceptionHandler
import java.lang.{Boolean => JBoolean}
import java.net.URLClassLoader
import java.security.Permission
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
import org.apache.flink.table.api.java.StreamTableEnvironment
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
    val params = ParameterTool.fromArgs(args)
    val mode: String = params.get("mode", "cluster")
    val isEmbeddedMode: Boolean = mode.toLowerCase == "embedded"
    val configPath: String = params.getRequired("config")
    val catalogPath: String = params.getRequired("catalog")
    val query: String = params.get("query", "")
    val outputPath: String = params.get("output", "")
    val usePrompt: Boolean = query.isEmpty

    def info(str: String): Unit = {
      if (usePrompt) {
        println(str)
      }
    }

    info(welcome())

    if (isEmbeddedMode) {
      info("> Executing client in EMBEDDED mode.")
    } else {
      info("> Executing client in CLUSTER mode.")
    }

    info("> Loading configuration from: " + configPath)

    // read config
    val config = ConfigParser.parseConfig(new File(configPath))

    info("> Loading catalog from: " + catalogPath)

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

    info("""
        |> Welcome!
        |
        |> Please enter SQL query or Q to exit.
        |> Terminate a running query by pressing ENTER
        |> Commands: 'SHOW TABLES', 'SHOW FUNCTIONS', 'SELECT ...', 'EXPLAIN ...'""".stripMargin)

    // create and configure stream exec environment
    // we are using the Java environment in order to support dynamically registration of functions
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
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

    if (usePrompt) {
      // we have no way to gracefully stop the query.
      var continue: Boolean = true
      while (continue) {

        Thread.sleep(1000L)

        println()
        print("> ")

        // read action from CLI
        val action = try {
          StdIn.readLine().trim
        } catch {
          case e: IOException =>
            // fail silently
          ""
        }

        if (action == "Q") {
          continue = false
        } else {
          val output = parseQuery(config, env, tEnv, action, outputPath)
          output match {
            case Some(outputInfo) =>
              executeAsync(
                isEmbeddedMode,
                jarPath,
                configPath,
                catalogPath,
                env,
                action,
                outputInfo)
            case _ => // no output
          }
        }
      }
    } else {
      parseQuery(config, env, tEnv, query, outputPath)
      // just execute it
      try {
        env.execute()
      } catch {
        case e: Throwable =>
          println(s"> Query terminated with: $e")
      }
    }
  }

  def executeAsync(
      isEmbeddedMode: Boolean,
      jarPath: String,
      configPath: String,
      catalogPath: String,
      env: StreamExecutionEnvironment,
      query: String,
      outputInfo: OutputInfo) {

    outputInfo.thread.foreach(_.start())

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
              "-q",
              "-c", "org.apache.flink.table.client.SqlClient", // use this client for submission
              jarPath, // ship jar
              "-mode", "embedded", // call execute in cluster configured environment
              "-config", configPath,
              "-catalog", catalogPath,
              "-output", outputInfo.path,
              "-query", query)) // execute query immediately
          }
        }
      }, "Query Execution")
    execThread.setUncaughtExceptionHandler(exceptionHandler)

    // start query execution
    println("> Query execution starts.")
    execThread.start()

    // wait for <ENTER> to terminate query
    StdIn.readLine()

    // stop execution thread hard (there is no way to stop it gracefully).
    execThread.stop()
    exceptionHandler.collected.foreach { t =>
      println("> Terminated with exception:")
      t.printStackTrace(System.out)
    }

    // clean up
    outputInfo.cleanUp.foreach(c => c())
  }

  def parseQuery(
      config: ConfigNode,
      env: StreamExecutionEnvironment,
      tEnv: StreamTableEnvironment,
      query: String,
      outputPath: String)
    : Option[OutputInfo] = {

    if (query.toUpperCase == "SHOW TABLES") {
      tEnv.listTables().foreach { t =>
        println(t)
        println(tEnv.scan(t).getSchema)
      }
    } else if (query.toUpperCase == "SHOW FUNCTIONS") {
      tEnv.listUserDefinedFunctions().foreach { t =>
        println(t)
      }
    } else if (query.toUpperCase.startsWith("EXPLAIN ")) {
      try {
        val tableOrStmt = query.substring(8)
        val explain = if (!query.contains("SELECT")) {
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
    } else if (!query.isEmpty) {

      val select = if (query.toUpperCase.startsWith("EXECUTE ")) {
        val path = query.substring(8)
        val source = scala.io.Source.fromFile(path)
        val lines = try source.mkString finally source.close()
        lines.replace("\n", " ")
      } else {
        query
      }

      // parse query
      val table: Option[Table] = try {
        Some(tEnv.sql(select))
      } catch {
        case e: Exception =>
          println("> Query could not be parsed:")
          println(e.getMessage)
          None
      }

      // query with output
      if (table.isDefined) {

        // prepare output
        val (sink, outputInfo) = config.output match {
          case "kafka" =>
            createKafkaOutput(config, outputPath)
          case o@_ =>
            throw ValidationException(s"Unsupported output '$o'.")
        }

        try {
          // try to create append stream
          tEnv.toAppendStream(table.get, classOf[Row])
            .map(new MapFunction[Row, String] {
              override def map(value: Row): String = value.toString
            })
            .addSink(sink)
        } catch {
          case _: Exception =>
            tEnv.toRetractStream(table.get, classOf[Row])
              .map(new MapFunction[JTuple2[JBoolean, Row], String] {

                def map(value: JTuple2[JBoolean, Row]): String = value.toString
              })
              .addSink(sink)
        }

        return Some(outputInfo)
      }
    }
    None
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

        case "custom" =>
          val clazz = Class.forName(t.source.clazz, true, classLoader)
          val propConst = clazz.getConstructors.toSeq.find { c =>
            c.getParameterCount == 1 && c.getParameterTypes()(0) == classOf[Properties]
          }

          val inst = propConst match {
            case Some(c) =>
              val props = new Properties()
              props.putAll(t.source.properties)
              c.newInstance(props)
            case None => clazz.newInstance()
          }

          inst.asInstanceOf[StreamTableSource[_]]
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

  def printIf(cond: Boolean, str: String): Unit = {
    if (cond) {
      println(str)
    }
  }

  def welcome(): String = {
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
  }

  def getRandomOutputTopic(prefix: String): String = {
    prefix + "_" + Random.nextLong().toHexString
  }

  def createKafkaOutput(
      config: ConfigNode,
      topic: String)
    : (SinkFunction[String], OutputInfo) = {

    val outputKafkaProps = new Properties()
    outputKafkaProps.putAll(config.defaults.kafka.properties)

    // topic should already exists
    if (!topic.isEmpty) {
      // create Kafka producer
      val producer = new FlinkKafkaProducer010(
        topic,
        new SimpleStringSchema(),
        outputKafkaProps)
      (producer, OutputInfo(topic, None, None))
    }
    // topic needs to be created and maintained
    else {
      // create topic
      val outputTopicPrefix = config.defaults.kafka.outputTopicPrefix
      val topicName = getRandomOutputTopic(outputTopicPrefix)
      // create output topic with 1 partition and replication 1
      createKafkaTopic(topicName, outputKafkaProps)

      // create Kafka producer
      val producer = new FlinkKafkaProducer010(
        topicName,
        new SimpleStringSchema(),
        outputKafkaProps)

      // create a thread to print query results
      val topicPrinter = new TopicPrinter(topicName, outputKafkaProps)
      val outputThread = new Thread(topicPrinter, "Topic Printer")

      // register shutdown hook to stop output printer and delete output topic
//      Runtime.getRuntime.addShutdownHook(
//        new Thread() {
//          override def run(): Unit = {
//            // stop printing of query results
//            topicPrinter.stopPrinting()
//            // delete Kakfa output topic
//            deleteKafkaTopic(topicName, outputKafkaProps)
//          }
//        }
//      )

      // clean up procedure
      val cleanUp = () => {
        // stop printing of query results
        topicPrinter.stopPrinting()
        // delete Kafka output topic
        deleteKafkaTopic(topicName, outputKafkaProps)
      }

      (producer, OutputInfo(topicName, Some(outputThread), Some(cleanUp)))
    }
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

  case class OutputInfo(path: String, thread: Option[Thread], cleanUp: Option[() => Unit])
}
