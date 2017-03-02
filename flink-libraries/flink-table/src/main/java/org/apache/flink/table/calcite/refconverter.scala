package org.apache.flink.table.calcite

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.{RexInputRef, RexNode, RexShuttle}
import org.apache.flink.table.calcite.FlinkTypeFactory.isTimeIndicatorType

import scala.collection.JavaConversions._

class RexInputRefUpdater(logicalRowType: RelDataType) extends RexShuttle {

  private val fieldTypes = logicalRowType.getFieldList.map(_.getType)

  override def visitInputRef(inputRef: RexInputRef): RexNode = {
    var countTimeIndicators = 0
    var i = 0
    while (i < inputRef.getIndex) {
      if (isTimeIndicatorType(fieldTypes(i))) {
        countTimeIndicators += 1
      }
      i += 1
    }
    new RexInputRef(inputRef.getIndex - countTimeIndicators, inputRef.getType)
  }
}

class AggregateCallRefUpdater(logicalRowType: RelDataType) {

  private val fieldTypes = logicalRowType.getFieldList.map(_.getType)

  def updateAggregateCall(aggCall: AggregateCall): AggregateCall = {
    aggCall.copy(
      aggCall.getArgList.map(convertIndex(_).asInstanceOf[Integer]),
      if (aggCall.filterArg < 0) {
        aggCall.filterArg
      } else {
        convertIndex(aggCall.filterArg)
      }
    )
  }

  private def convertIndex(idx: Int): Int = {
    var countTimeIndicators = 0
    var i = 0
    while (i < idx) {
      if (isTimeIndicatorType(fieldTypes(i))) {
        countTimeIndicators += 1
      }
      i += 1
    }
    idx - countTimeIndicators
  }
}
