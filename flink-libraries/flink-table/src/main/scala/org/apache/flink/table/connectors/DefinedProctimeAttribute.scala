package org.apache.flink.table.connectors

import javax.annotation.Nullable

import org.apache.flink.table.api.Types
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource

/**
  * Extends a [[TableSource]] or [[TableSink]] to specify a processing time attribute.
  */
trait DefinedProctimeAttribute {

  /**
    * Returns the name of a processing time attribute or null if no processing time attribute is
    * present.
    *
    * The referenced attribute must be present in the table schema of a [[TableSource]] or
    * [[TableSink]] with type [[Types.SQL_TIMESTAMP]].
    */
  @Nullable
  def getProctimeAttribute: String
}
