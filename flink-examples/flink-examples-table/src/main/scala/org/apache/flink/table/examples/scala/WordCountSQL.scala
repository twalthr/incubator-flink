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
package org.apache.flink.table.examples.scala

import org.apache.flink.api.scala._
import org.apache.flink.table.api.DataTypes._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala._

/**
  * Simple example that shows how the Batch SQL API is used in Scala.
  *
  * This example shows how to:
  *  - Convert DataSets to Tables
  *  - Register a Table under a name
  *  - Run a SQL query on the registered Table
  *
  */
object WordCountSQL {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    TIME()

//    val settings = EnvironmentSettings.newInstance()
//      .useBlinkPlanner()
//      .inBatchMode()
//      .build();
//
//    val env = TableEnvironment.create(settings);
//
//    env.registerCatalog("enterprise_hive", new HiveCatalog(...))
//    env.registerCatalog("enterprise_kafka", new SchemaRegistry(...))
//
//    env.createTemporaryFunction("jsonParser", JsonParser.class)
//
//    val data = env.sqlQuery("SELECT parse(json) AS customer FROM enterprise_hive.sensitive.customers")
//
//    val enrichedData = data
//      .select('json.flatten())
//      .dropColumns(12 to 34)
//      .addColumns('firstname + " " + 'lastname as 'fullname)
//
//    env.createView("enrichedData", enrichedData)
//
//    env.sqlUpdate("INSERT INTO enterprise_kafka.topics.customers SELECT * FROM enrichedData");
//
//    env.execute("ETL pipeline");
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class WC(var word: String, var frequency: Long)

}
