package org.apache.flink.table.calcite

import org.apache.calcite.rel.`type`.RelDataType
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
