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

package org.apache.flink.table.client.config

import java.io.{File, IOException}

import com.fasterxml.jackson.core.{JsonGenerator, JsonParseException, JsonParser}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonMappingException, ObjectMapper}
import org.apache.flink.table.api.TableException

/**
  * Parser for the SQL client schema configuration.
  */
object CatalogParser {

  def parseCatalog(path: File): CatalogNode = {
    val mapper = new ObjectMapper()
    mapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, false)
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true)

    try {
      val catalog = mapper.readValue(path, classOf[CatalogNode])
      catalog.validate()

      catalog
    } catch {
      case ioe: IOException =>
        throw TableException("Could not read catalog file.", ioe)
      case jpe: JsonParseException =>
        throw TableException("The JSON of the catalog file is invalid.", jpe)
      case jme: JsonMappingException =>
        throw TableException("The JSON of the catalog file is invalid.", jme)
    }
  }
}

object ConfigParser {

  def parseConfig(path: File): ConfigNode = {
    val mapper = new ObjectMapper()
    mapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, false)
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true)

    try {
      val config = mapper.readValue(path, classOf[ConfigNode])
      config.validate()

      config
    } catch {
      case ioe: IOException =>
        throw TableException("Could not read client configuration file.", ioe)
      case jpe: JsonParseException =>
        throw TableException("The JSON of the client configuration file is invalid.", jpe)
      case jme: JsonMappingException =>
        throw TableException("The JSON of the client configuration file is invalid.", jme)
    }
  }
}
