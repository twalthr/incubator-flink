package org.apache.flink.api.table.sql

import org.apache.flink.api.table.Table

import scala.collection.mutable


class TableRegistry {
  val registry = mutable.HashMap.empty[String, Table]

  def registerTable(name: String, table: Table) = {
    registry += (name -> table)
  }

  def unregisterTable(name: String) = {
    registry -= name
  }

  def getTable(name: String): Table = {
    val table = registry.get(name)
    table match {
      case Some(value) => value
      case None => throw new SqlException("Table name does not exist.")
    }
  }
}
