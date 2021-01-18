package org.apache.flink.table.examples.scala.basics

import io.circe.Json
import io.circe.parser.parse
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer

import java.time.Duration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{$, AnyWithOperations}
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction}
import org.apache.flink.table.types.logical.RawType

import java.time.Instant

import scala.util.Random

object BugRepro {
  def text: String =
    s"""
       |{
       |  "s": "hello",
       |  "i": ${Random.nextInt()}
       |}
       |""".stripMargin

  @DataTypeHint(value = "RAW", bridgedTo = classOf[JsonContainer])
class JsonContainer(var json: Json)
class AnyJsonValue extends AggregateFunction[Json, JsonContainer] {
  override def getValue(accumulator: JsonContainer): Json = accumulator.json
  override def createAccumulator(): JsonContainer = new JsonContainer(null)

  @FunctionHint(
    input = Array(
      new DataTypeHint(
        value =
          "RAW('io.circe.Json', 'AEdvcmcuYXBhY2hlLmZsaW5rLmFwaS5qYXZhLnR5cGV1dGlscy5ydW50aW1lLmtyeW8uS3J5b1NlcmlhbGl6ZXJTbmFwc2hvdAAAAAIADWlvLmNpcmNlLkpzb24AAATyxpo9cAAAAAIADWlvLmNpcmNlLkpzb24BAAAADwANaW8uY2lyY2UuSnNvbgEAAAATAA1pby5jaXJjZS5Kc29uAAAAAAApb3JnLmFwYWNoZS5hdnJvLmdlbmVyaWMuR2VuZXJpY0RhdGEkQXJyYXkBAAAAKwApb3JnLmFwYWNoZS5hdnJvLmdlbmVyaWMuR2VuZXJpY0RhdGEkQXJyYXkBAAAAtgBVb3JnLmFwYWNoZS5mbGluay5hcGkuamF2YS50eXBldXRpbHMucnVudGltZS5rcnlvLlNlcmlhbGl6ZXJzJER1bW15QXZyb1JlZ2lzdGVyZWRDbGFzcwAAAAEAWW9yZy5hcGFjaGUuZmxpbmsuYXBpLmphdmEudHlwZXV0aWxzLnJ1bnRpbWUua3J5by5TZXJpYWxpemVycyREdW1teUF2cm9LcnlvU2VyaWFsaXplckNsYXNzAAAE8saaPXAAAAAAAAAE8saaPXAAAAAA')",
        bridgedTo = classOf[Json]
      )
    ),
    output = new DataTypeHint(
      value =
        "RAW('io.circe.Json', 'AEdvcmcuYXBhY2hlLmZsaW5rLmFwaS5qYXZhLnR5cGV1dGlscy5ydW50aW1lLmtyeW8uS3J5b1NlcmlhbGl6ZXJTbmFwc2hvdAAAAAIADWlvLmNpcmNlLkpzb24AAATyxpo9cAAAAAIADWlvLmNpcmNlLkpzb24BAAAADwANaW8uY2lyY2UuSnNvbgEAAAATAA1pby5jaXJjZS5Kc29uAAAAAAApb3JnLmFwYWNoZS5hdnJvLmdlbmVyaWMuR2VuZXJpY0RhdGEkQXJyYXkBAAAAKwApb3JnLmFwYWNoZS5hdnJvLmdlbmVyaWMuR2VuZXJpY0RhdGEkQXJyYXkBAAAAtgBVb3JnLmFwYWNoZS5mbGluay5hcGkuamF2YS50eXBldXRpbHMucnVudGltZS5rcnlvLlNlcmlhbGl6ZXJzJER1bW15QXZyb1JlZ2lzdGVyZWRDbGFzcwAAAAEAWW9yZy5hcGFjaGUuZmxpbmsuYXBpLmphdmEudHlwZXV0aWxzLnJ1bnRpbWUua3J5by5TZXJpYWxpemVycyREdW1teUF2cm9LcnlvU2VyaWFsaXplckNsYXNzAAAE8saaPXAAAAAAAAAE8saaPXAAAAAA')",
      bridgedTo = classOf[Json]
    )
  )
  def accumulate(acc: JsonContainer, value: Json): Unit =
    if ((value eq null) || (acc.json ne null)) ()
    else {
      acc.json = value
    }
}

class ParseJson extends ScalarFunction {
  @DataTypeHint(
    value =
      "RAW('io.circe.Json', 'AEdvcmcuYXBhY2hlLmZsaW5rLmFwaS5qYXZhLnR5cGV1dGlscy5ydW50aW1lLmtyeW8uS3J5b1NlcmlhbGl6ZXJTbmFwc2hvdAAAAAIADWlvLmNpcmNlLkpzb24AAATyxpo9cAAAAAIADWlvLmNpcmNlLkpzb24BAAAADwANaW8uY2lyY2UuSnNvbgEAAAATAA1pby5jaXJjZS5Kc29uAAAAAAApb3JnLmFwYWNoZS5hdnJvLmdlbmVyaWMuR2VuZXJpY0RhdGEkQXJyYXkBAAAAKwApb3JnLmFwYWNoZS5hdnJvLmdlbmVyaWMuR2VuZXJpY0RhdGEkQXJyYXkBAAAAtgBVb3JnLmFwYWNoZS5mbGluay5hcGkuamF2YS50eXBldXRpbHMucnVudGltZS5rcnlvLlNlcmlhbGl6ZXJzJER1bW15QXZyb1JlZ2lzdGVyZWRDbGFzcwAAAAEAWW9yZy5hcGFjaGUuZmxpbmsuYXBpLmphdmEudHlwZXV0aWxzLnJ1bnRpbWUua3J5by5TZXJpYWxpemVycyREdW1teUF2cm9LcnlvU2VyaWFsaXplckNsYXNzAAAE8saaPXAAAAAAAAAE8saaPXAAAAAA')",
    bridgedTo = classOf[Json]
  )
  def eval(s: String): Json =
    parse(s) match {
      case Left(err)   => throw err
      case Right(json) => json
    }
}

  def main(args: Array[String]): Unit = {
    val flink =
      StreamExecutionEnvironment.createLocalEnvironment()
    val tableEnv = StreamTableEnvironment.create(flink)

    val rt = new RawType[Json](classOf[Json], new KryoSerializer[Json](classOf[Json], flink.getConfig))
    println(rt.asSerializableString())

    tableEnv.createTemporarySystemFunction("parse_json", new ParseJson)
    tableEnv.createTemporarySystemFunction("first_json_value", new AnyJsonValue)
    val dataStream = flink
      .addSource {
        new SourceFunction[(Long, String)] {
          var isRunning = true

          override def run(
              ctx: SourceFunction.SourceContext[(Long, String)]
          ): Unit =
            while (isRunning) {
              val x = (Instant.now().toEpochMilli, text)
              ctx.collect(x)
              ctx.emitWatermark(new Watermark(x._1))
              Thread.sleep(300)
            }

          override def cancel(): Unit =
            isRunning = false
        }
      }
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[(Long, String)](Duration.ofSeconds(30))
          .withTimestampAssigner {
            new SerializableTimestampAssigner[(Long, String)] {
              override def extractTimestamp(
                  element: (Long, String),
                  recordTimestamp: Long
              ): Long =
                element._1
            }
          }
      )

    tableEnv.createTemporaryView(
      "testview",
      dataStream,
      $("event_time").rowtime(),
      $("json_text")
    )
    val res = tableEnv.sqlQuery("""
                                  |SELECT first_json_value(parse_json(json_text))
                                  |FROM testview
                                  |GROUP BY TUMBLE(event_time, INTERVAL '10' SECOND)
                                  |""".stripMargin)

    val sink = tableEnv.executeSql(
      """
        |CREATE TABLE SINK (
        |  json_text RAW('io.circe.Json', 'AEdvcmcuYXBhY2hlLmZsaW5rLmFwaS5qYXZhLnR5cGV1dGlscy5ydW50aW1lLmtyeW8uS3J5b1NlcmlhbGl6ZXJTbmFwc2hvdAAAAAIADWlvLmNpcmNlLkpzb24AAATyxpo9cAAAAAIADWlvLmNpcmNlLkpzb24BAAAADwANaW8uY2lyY2UuSnNvbgEAAAATAA1pby5jaXJjZS5Kc29uAAAAAAApb3JnLmFwYWNoZS5hdnJvLmdlbmVyaWMuR2VuZXJpY0RhdGEkQXJyYXkBAAAAKwApb3JnLmFwYWNoZS5hdnJvLmdlbmVyaWMuR2VuZXJpY0RhdGEkQXJyYXkBAAAAtgBVb3JnLmFwYWNoZS5mbGluay5hcGkuamF2YS50eXBldXRpbHMucnVudGltZS5rcnlvLlNlcmlhbGl6ZXJzJER1bW15QXZyb1JlZ2lzdGVyZWRDbGFzcwAAAAEAWW9yZy5hcGFjaGUuZmxpbmsuYXBpLmphdmEudHlwZXV0aWxzLnJ1bnRpbWUua3J5by5TZXJpYWxpemVycyREdW1teUF2cm9LcnlvU2VyaWFsaXplckNsYXNzAAAE8saaPXAAAAAAAAAE8saaPXAAAAAA')
        |)
        |WITH (
        |  'connector' = 'print'
        |)
        |""".stripMargin
    )

    res.executeInsert("SINK").await()
    ()
  }
}
