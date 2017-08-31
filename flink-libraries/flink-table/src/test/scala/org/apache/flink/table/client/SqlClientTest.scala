package org.apache.flink.table.client

import java.io.File

import org.apache.flink.table.client.config.ClientParser
import org.junit.Test

/**
  * Tests for the SQL client.
  */
class SqlClientTest {

  @Test
  def testSchemaParsing(): Unit = {
    ClientParser.parse(new File("/Users/twalthr/flink/tmp/test.json"))
  }

}
