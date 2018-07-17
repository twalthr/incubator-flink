package org.apache.flink.table.connectors

import javax.annotation.Nullable

import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.sources.TableSource

/**
  * Extends a [[TableSource]] to specify a processing time attribute.
  */
trait DefinedProctimeAttribute {

  /**
    * Returns the name of a processing time attribute or null if no processing time attribute is
    * present.
    *
    * The referenced attribute must be present in the [[TableSchema]] of the [[TableSource]] and of
    * type [[Types.SQL_TIMESTAMP]].
    */
  @Nullable
  def getProctimeAttribute: String
}
