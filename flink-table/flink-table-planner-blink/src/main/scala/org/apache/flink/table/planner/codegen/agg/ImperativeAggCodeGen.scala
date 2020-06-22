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
package org.apache.flink.table.planner.codegen.agg

import java.lang.{Iterable => JIterable}

import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.data.{GenericRowData, RowData, UpdatableRowData}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{UserDefinedAggregateFunction, UserDefinedFunctionHelper}
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GenerateUtils.generateFieldAccess
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator._
import org.apache.flink.table.planner.codegen.{CodeGenUtils, CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.planner.expressions.DeclarativeExpressionResolver
import org.apache.flink.table.planner.expressions.DeclarativeExpressionResolver.toRexInputRef
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter
import org.apache.flink.table.planner.plan.utils.AggregateInfo
import org.apache.flink.table.planner.typeutils.DataViewUtils.DataViewSpec
import org.apache.flink.table.planner.utils.SingleElementIterator
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

/**
  * It is for code generate aggregation functions that are specified in terms of
  * accumulate(), retract() and merge() functions. The aggregate accumulator is
  * embedded inside of a larger shared aggregation buffer.
  *
  * @param ctx the code gen context
  * @param aggInfo  the aggregate information
  * @param filterExpression filter argument access expression, none if no filter
  * @param mergedAccOffset the mergedAcc may come from local aggregate,
  *                        this is the first buffer offset in the row
  * @param aggBufferOffset  the offset in the buffers of this aggregate
  * @param aggBufferSize  the total size of aggregate buffers
  * @param inputTypes   the input field type infos
  * @param constantExprs  the constant expressions
  * @param relBuilder  the rel builder to translate expressions to calcite rex nodes
  * @param hasNamespace  whether the accumulators state has namespace
  * @param inputFieldCopy    copy input field element if true (only mutable type will be copied)
  */
class ImperativeAggCodeGen(
    ctx: CodeGeneratorContext,
    aggInfo: AggregateInfo,
    filterExpression: Option[Expression],
    mergedAccOffset: Int,
    aggBufferOffset: Int,
    aggBufferSize: Int,
    inputTypes: Seq[LogicalType],
    constantExprs: Seq[GeneratedExpression],
    relBuilder: RelBuilder,
    hasNamespace: Boolean,
    mergedAccOnHeap: Boolean,
    mergedAccType: LogicalType,
    inputFieldCopy: Boolean)
  extends AggCodeGen {

  private val SINGLE_ITERABLE = className[SingleElementIterator[_]]
  private val UPDATABLE_ROW = className[UpdatableRowData]

  private val function = aggInfo.function.asInstanceOf[UserDefinedAggregateFunction[_, _]]
  private val functionTerm: String = ctx.addReusableFunction(function)
  private val aggIndex: Int = aggInfo.aggIndex

  private val externalAccType = aggInfo.externalAccTypes(0)
  private val internalAccType = externalAccType.getLogicalType

  /** whether the acc type is an internal type.
    * Currently we only support GenericRowData as internal acc type */
  private val isAccTypeInternal: Boolean =
    classOf[RowData].isAssignableFrom(externalAccType.getConversionClass)

  private val accInternalTerm: String = s"agg${aggIndex}_acc_internal"
  private val accExternalTerm: String = s"agg${aggIndex}_acc_external"
  private val accTypeInternalTerm: String = if (isAccTypeInternal) {
    GENERIC_ROW
  } else {
    boxedTypeTermForType(externalAccType.getLogicalType)
  }
  private val accTypeExternalTerm: String = typeTerm(externalAccType.getConversionClass)

  private val argTypes: Array[LogicalType] = {
    val types = inputTypes ++ constantExprs.map(_.resultType)
    aggInfo.argIndexes.map(types(_))
  }

  private val externalResultType = aggInfo.externalResultType
  private val internalResultType = externalResultType.getLogicalType

  private val rexNodeGen = new ExpressionConverter(relBuilder)

  private val viewSpecs: Array[DataViewSpec] = aggInfo.viewSpecs
  // add reusable dataviews to context
  addReusableStateDataViews(ctx, viewSpecs, hasNamespace, !mergedAccOnHeap)

  def createAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    // do not set dataview into the acc in createAccumulator
    val accField = if (isAccTypeInternal) {
      // do not need convert to internal type
      s"($accTypeInternalTerm) $functionTerm.createAccumulator()"
    } else {
      genToInternalConverter(ctx, externalAccType)(s"$functionTerm.createAccumulator()")
    }
    val accInternal = newName("acc_internal")
    val code = s"$accTypeInternalTerm $accInternal = ($accTypeInternalTerm) $accField;"
    Seq(GeneratedExpression(accInternal, "false", code, internalAccType))
  }

  def setAccumulator(generator: ExprCodeGenerator): String = {
    // generate internal acc field
    val expr = generateAccumulatorAccess(
      ctx,
      generator.input1Type,
      generator.input1Term,
      aggBufferOffset,
      viewSpecs,
      useStateDataView = true,
      useBackupDataView = false)

    if (isAccTypeInternal) {
      ctx.addReusableMember(s"private $accTypeInternalTerm $accInternalTerm;")
      s"$accInternalTerm = ${expr.resultTerm};"
    } else {
      ctx.addReusableMember(s"private $accTypeInternalTerm $accInternalTerm;")
      ctx.addReusableMember(s"private $accTypeExternalTerm $accExternalTerm;")
      s"""
         |$accInternalTerm = ${expr.resultTerm};
         |$accExternalTerm = ${genToExternalConverter(ctx, externalAccType, accInternalTerm)};
      """.stripMargin
    }
  }

  override def resetAccumulator(generator: ExprCodeGenerator): String = {
    if (isAccTypeInternal) {
      s"$accInternalTerm = ($accTypeInternalTerm) $functionTerm.createAccumulator();"
    } else {
      s"""
         |$accExternalTerm = ($accTypeExternalTerm) $functionTerm.createAccumulator();
         |$accInternalTerm = ${genToInternalConverter(ctx, externalAccType)(accExternalTerm)};
       """.stripMargin
    }
  }

  def getAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    val code = if (isAccTypeInternal) {
      // do not need convert to internal type
      ""
    } else {
      s"$accInternalTerm = ${genToInternalConverter(ctx, externalAccType)(accExternalTerm)};"
    }
    Seq(GeneratedExpression(accInternalTerm, "false", code, internalAccType))
  }

  def accumulate(generator: ExprCodeGenerator): String = {
    val (operands, code) = prepareExternalOperands(generator)
    val call = s"$functionTerm.accumulate($operands);"
    filterExpression match {
      case None =>
        s"""
           |$code
           |$call
         """.stripMargin
      case Some(expr) =>
        val generated = generator.generateExpression(expr.accept(rexNodeGen))
        s"""
           |if (${generated.resultTerm}) {
           |  $code
           |  $call
           |}
         """.stripMargin
    }
  }

  def retract(generator: ExprCodeGenerator): String = {
    val (operands, code) = prepareExternalOperands(generator)
    val call = s"$functionTerm.retract($operands);"
    filterExpression match {
      case None =>
        s"""
           |$code
           |$call
         """.stripMargin
      case Some(expr) =>
        val generated = generator.generateExpression(expr.accept(rexNodeGen))
        s"""
           |if (${generated.resultTerm}) {
           |  $code
           |  $call
           |}
         """.stripMargin
    }
  }

  def merge(generator: ExprCodeGenerator): String = {
    val accIterTerm = s"agg${aggIndex}_acc_iter"
    ctx.addReusableMember(s"private final $SINGLE_ITERABLE $accIterTerm = new $SINGLE_ITERABLE();")

    // generate internal acc field
    val expr = generateAccumulatorAccess(
      ctx,
      generator.input1Type,
      generator.input1Term,
      mergedAccOffset + aggBufferOffset,
      viewSpecs,
      useStateDataView = !mergedAccOnHeap,
      useBackupDataView = true)

    if (isAccTypeInternal) {
      s"""
         |$accIterTerm.set(${expr.resultTerm});
         |$functionTerm.merge($accInternalTerm, $accIterTerm);
       """.stripMargin
    } else {
      val otherAccExternal = newName("other_acc_external")
      s"""
         |$accTypeExternalTerm $otherAccExternal = ${
            genToExternalConverter(ctx, externalAccType, expr.resultTerm)};
         |$accIterTerm.set($otherAccExternal);
         |$functionTerm.merge($accExternalTerm, $accIterTerm);
      """.stripMargin
    }
  }

  def getValue(generator: ExprCodeGenerator): GeneratedExpression = {
    val valueExternalTerm = newName("value_external")
    val valueExternalTypeTerm = typeTerm(externalResultType.getConversionClass)
    val valueInternalTerm = newName("value_internal")
    val valueInternalTypeTerm = boxedTypeTermForType(internalResultType)
    val nullTerm = newName("valueIsNull")
    val accTerm = if (isAccTypeInternal) accInternalTerm else accExternalTerm
    val code =
      s"""
         |$valueExternalTypeTerm $valueExternalTerm = ($valueExternalTypeTerm)
         |  $functionTerm.getValue($accTerm);
         |$valueInternalTypeTerm $valueInternalTerm =
         |  ${genToInternalConverter(ctx, externalResultType)(valueExternalTerm)};
         |boolean $nullTerm = $valueInternalTerm == null;
      """.stripMargin

    GeneratedExpression(valueInternalTerm, nullTerm, code, internalResultType)
  }

  private def prepareExternalOperands(generator: ExprCodeGenerator): (String, String) = {
    var codes: ArrayBuffer[String] = ArrayBuffer.empty[String]
    val inputFields = aggInfo.argIndexes.zipWithIndex.map { case (f, index) =>
      if (f >= inputTypes.length) {
        // index to constant
        val expr = constantExprs(f - inputTypes.length)
        genToExternalConverterAll(ctx, aggInfo.externalArgTypes(index), expr)
      } else {
        // index to input field
        val inputRef = if (generator.input1Term.startsWith(DISTINCT_KEY_TERM)) {
          if (argTypes.length == 1) {
            // called from distinct merge and the inputTerm is the only argument
            DeclarativeExpressionResolver.toRexDistinctKey(
              relBuilder, generator.input1Term, inputTypes(f))
          } else {
            // called from distinct merge call and the inputTerm is RowData type
            toRexInputRef(relBuilder, index, inputTypes(f))
          }
        } else {
          // called from accumulate
          toRexInputRef(relBuilder, f, inputTypes(f))
        }
        var inputExpr = generator.generateExpression(inputRef.accept(rexNodeGen))
        if (inputFieldCopy) inputExpr = inputExpr.deepCopy(ctx)
        codes += inputExpr.code
        genToExternalConverterAll(ctx, aggInfo.externalArgTypes(index), inputExpr)
      }
    }

    val accTerm = if (isAccTypeInternal) accInternalTerm else accExternalTerm
    // insert acc to the head of the list
    val fields = Seq(accTerm) ++ inputFields
    // acc, arg1, arg2
    (fields.mkString(", "), codes.mkString("\n"))
  }

  /**
    * This method is mainly the same as CodeGenUtils.generateFieldAccess(), the only difference is
    * that this method using UpdatableRowData to wrap RowData to handle DataViews.
    */
  def generateAccumulatorAccess(
    ctx: CodeGeneratorContext,
    inputType: LogicalType,
    inputTerm: String,
    index: Int,
    viewSpecs: Array[DataViewSpec],
    useStateDataView: Boolean,
    useBackupDataView: Boolean): GeneratedExpression = {

    // if input has been used before, we can reuse the code that
    // has already been generated
    val inputExpr = ctx.getReusableInputUnboxingExprs(inputTerm, index) match {
      // input access and unboxing has already been generated
      case Some(expr) => expr

      // generate input access and unboxing if necessary
      case None =>
        // this field access is not need to reuse
        val expr = generateFieldAccess(ctx, inputType, inputTerm, index)

        val newExpr = inputType match {
          case ct: RowType if isAccTypeInternal =>
            // acc is never be null
            val fieldType = ct.getTypeAt(index).asInstanceOf[RowType]
            val exprGenerator = new ExprCodeGenerator(ctx, false)
              .bindInput(fieldType, inputTerm = expr.resultTerm)
            val converted = exprGenerator.generateConverterResultExpression(
              fieldType,
              classOf[GenericRowData],
              outRecordTerm = newName("acc"),
              reusedOutRow = false,
              fieldCopy = inputFieldCopy)
            val code =
              s"""
                 |${expr.code}
                 |${ctx.reuseInputUnboxingCode(expr.resultTerm)}
                 |${converted.code}
               """.stripMargin

            GeneratedExpression(
              converted.resultTerm,
              converted.nullTerm,
              code,
              converted.resultType)
          case _ => expr
        }

        val exprWithDataView = inputType match {
          case ct: RowType if viewSpecs.nonEmpty && useStateDataView =>
            if (isAccTypeInternal) {
              val code =
                s"""
                  |${newExpr.code}
                  |${generateDataViewFieldSetter(newExpr.resultTerm, viewSpecs, useBackupDataView)}
                """.stripMargin
              GeneratedExpression(newExpr.resultTerm, newExpr.nullTerm, code, newExpr.resultType)
            } else {
              val fieldType = ct.getTypeAt(index)
              val fieldTerm = newName("field")
              ctx.addReusableMember(s"$UPDATABLE_ROW $fieldTerm;")
              val code =
                s"""
                   |${newExpr.code}
                   |$fieldTerm = null;
                   |if (!${newExpr.nullTerm}) {
                   |  $fieldTerm = new $UPDATABLE_ROW(${newExpr.resultTerm}, ${
                        LogicalTypeChecks.getFieldCount(fieldType)});
                   |  ${generateDataViewFieldSetter(fieldTerm, viewSpecs, useBackupDataView)}
                   |}
                """.stripMargin
              GeneratedExpression(fieldTerm, newExpr.nullTerm, code, newExpr.resultType)
            }

          case _ => newExpr
        }

        ctx.addReusableInputUnboxingExprs(inputTerm, index, exprWithDataView)
        exprWithDataView
    }
    // hide the generated code as it will be executed only once
    GeneratedExpression(inputExpr.resultTerm, inputExpr.nullTerm, "", inputExpr.resultType)
  }

  /**
    * Generate statements to set data view field when use state backend.
    *
    * @param accTerm aggregation term
    * @return data view field set statements
    */
  private def generateDataViewFieldSetter(
      accTerm: String,
      viewSpecs: Array[DataViewSpec],
      useBackupDataView: Boolean): String = {
    val setters = for (spec <- viewSpecs) yield {
      if (hasNamespace) {
        val dataViewTerm = if (useBackupDataView) {
          createDataViewBackupTerm(spec)
        } else {
          createDataViewTerm(spec)
        }

        val dataViewInternalTerm = if (useBackupDataView) {
          createDataViewBackupRawValueTerm(spec)
        } else {
          createDataViewRawValueTerm(spec)
        }

        s"""
           |// when namespace is null, the dataview is used in heap, no key and namespace set
           |if ($NAMESPACE_TERM != null) {
           |  $dataViewTerm.setCurrentNamespace($NAMESPACE_TERM);
           |  $dataViewInternalTerm.setJavaObject($dataViewTerm);
           |  $accTerm.setField(${spec.getFieldIndex}, $dataViewInternalTerm);
           |}
         """.stripMargin
      } else {
        val dataViewTerm = createDataViewTerm(spec)
        val dataViewInternalTerm = createDataViewRawValueTerm(spec)

        s"""
           |$dataViewInternalTerm.setJavaObject($dataViewTerm);
           |$accTerm.setField(${spec.getFieldIndex}, $dataViewInternalTerm);
        """.stripMargin
      }
    }
    setters.mkString("\n")
  }

  override def checkNeededMethods(
      needAccumulate: Boolean = false,
      needRetract: Boolean = false,
      needMerge: Boolean = false,
      needReset: Boolean = false,
      needEmitValue: Boolean = false): Unit = {

    if (aggInfo.isLegacyMode) {
      return
    }

    val functionName = aggInfo.agg.getAggregation.toString
    val accumulatorClass = aggInfo.externalAccTypes.map(_.getConversionClass)
    val argumentClasses = aggInfo.externalArgTypes.map(_.getConversionClass)

    if (needAccumulate) {
      UserDefinedFunctionHelper.validateClassForRuntime(
        function.getClass,
        UserDefinedFunctionHelper.AGGREGATE_ACCUMULATE,
        accumulatorClass ++ argumentClasses,
        classOf[Unit],
        functionName
      )
    }

    if (needRetract) {
      UserDefinedFunctionHelper.validateClassForRuntime(
        function.getClass,
        UserDefinedFunctionHelper.AGGREGATE_RETRACT,
        accumulatorClass ++ argumentClasses,
        classOf[Unit],
        functionName
      )
    }

    if (needMerge) {
      UserDefinedFunctionHelper.validateClassForRuntime(
        function.getClass,
        UserDefinedFunctionHelper.AGGREGATE_MERGE,
        accumulatorClass ++ Array(classOf[JIterable[Any]]),
        classOf[Unit],
        functionName
      )
    }

    if (needReset) {
      UserDefinedFunctionHelper.validateClassForRuntime(
        function.getClass,
        UserDefinedFunctionHelper.AGGREGATE_RESET,
        accumulatorClass,
        classOf[Unit],
        functionName
      )
    }

    if (needEmitValue) {
      UserDefinedFunctionHelper.validateClassForRuntime(
        function.getClass,
        UserDefinedFunctionHelper.TABLE_AGGREGATE_EMIT,
        accumulatorClass ++ Array(classOf[Collector[_]]),
        classOf[Unit],
        functionName
      )
    }
  }

  def emitValue: String = {
    val accTerm = if (isAccTypeInternal) accInternalTerm else accExternalTerm
    s"$functionTerm.emitValue($accTerm, $MEMBER_COLLECTOR_TERM);"
  }

  // ----------------------------------------------------------------------------------------------
  // Best effort legacy handling
  // ----------------------------------------------------------------------------------------------

  private def genToInternalConverter(
      ctx: CodeGeneratorContext,
      sourceDataType: DataType)
    : String => String = {
    if (aggInfo.isLegacyMode) {
      genToInternal(ctx, sourceDataType)
    } else {
      CodeGenUtils.genToInternalConverter(ctx, sourceDataType)
    }
  }

  private def genToExternalConverter(
      ctx: CodeGeneratorContext,
      targetDataType: DataType,
      internalTerm: String)
    : String = {
    if (aggInfo.isLegacyMode) {
      genToExternal(ctx, targetDataType, internalTerm)
    } else {
      CodeGenUtils.genToExternalConverter(ctx, targetDataType, internalTerm)
    }
  }
}
