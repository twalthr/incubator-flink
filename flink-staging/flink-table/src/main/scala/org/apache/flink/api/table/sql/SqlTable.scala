package org.apache.flink.api.table.sql

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.table.plan.PlanNode

class SqlTable(val planNode: PlanNode) extends AbstractTable{
  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val builder = typeFactory.builder()
    planNode.outputFields.foreach(field => {
      builder.add(field._1, typeFactory.createSqlType(typeInfoToSqlType(field._2)))
    })
    builder.build()
  }

  // ----------------------------------------------------------------------------------------------

  def typeInfoToSqlType(typeInfo: TypeInformation[_]): SqlTypeName = {
    typeInfo match {
      case BasicTypeInfo.STRING_TYPE_INFO => SqlTypeName.VARCHAR
      case BasicTypeInfo.INT_TYPE_INFO => SqlTypeName.INTEGER
        // TODO more types
    }
  }
}
