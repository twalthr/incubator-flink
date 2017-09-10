package org.apache.flink.table.client.demo

import org.apache.flink.table.functions.ScalarFunction

class Adder(f: Int) extends ScalarFunction {

  def eval(a: Int, b: Int): Int = {
    a + b * f
  }
}
