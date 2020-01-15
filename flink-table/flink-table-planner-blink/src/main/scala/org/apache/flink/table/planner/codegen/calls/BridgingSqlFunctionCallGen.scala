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

package org.apache.flink.table.planner.codegen.calls
import java.lang.reflect.Method
import java.util
import java.util.Optional

import org.apache.flink.table.catalog.DataTypeLookup
import org.apache.flink.table.functions.{FunctionDefinition, ScalarFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{genToExternalIfNeeded, genToInternalIfNeeded, typeTerm}
import org.apache.flink.table.planner.codegen.{CodeGenException, CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.extraction.utils.ExtractionUtils
import org.apache.flink.table.types.extraction.utils.ExtractionUtils.{createMethodSignatureString, isAssignable, isMethodInvokable}
import org.apache.flink.table.types.inference.{CallContext, TypeInferenceUtil}
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.utils.TypeConversions

/**
 * Generates a call to a user-defined [[ScalarFunction]] or [[TableFunction]] (future work).
 */
class BridgingSqlFunctionCallGen(function: BridgingSqlFunction) extends CallGenerator {

  private val udf: UserDefinedFunction = function.getDefinition.asInstanceOf[UserDefinedFunction]

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType)
    : GeneratedExpression = {

    val inference = function.getTypeInference
    val callContext = new CodeGenerationCallContext(operands, returnType)

    // enrich argument types with conversion class
    val adaptedCallContext = TypeInferenceUtil.adaptArguments(
      inference,
      callContext,
      TypeConversions.fromLogicalToDataType(returnType))
    val enrichedArgumentDataTypes = toScala(adaptedCallContext.getArgumentDataTypes)
    verifyArgumentTypes(operands.map(_.resultType), enrichedArgumentDataTypes)

    // enrich output types with conversion class
    val enrichedOutputDataType = TypeInferenceUtil.inferOutputType(
      adaptedCallContext,
      inference.getOutputTypeStrategy)
    verifyOutputType(returnType, enrichedOutputDataType)

    // find runtime method and generate call
    verifyImplementation(enrichedArgumentDataTypes, enrichedOutputDataType)
    generateFunctionCall(ctx, operands, enrichedArgumentDataTypes, enrichedOutputDataType)
  }

  private def generateFunctionCall(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      argumentDataTypes: Seq[DataType],
      outputDataType: DataType)
    : GeneratedExpression = {

    val functionTerm = ctx.addReusableFunction(udf)

    // operand conversion
    val externalOperands = prepareExternalOperands(ctx, operands, argumentDataTypes)
    val externalOperandTerms = externalOperands.map(_.resultTerm).mkString(", ")

    // result conversion
    val externalResultTypeTerm = typeTerm(outputDataType.getConversionClass)
    val externalResultTerm = ctx.addReusableLocalVariable(externalResultTypeTerm, "externalResult")
    val internalExpr = genToInternalIfNeeded(ctx, outputDataType, externalResultTerm)

    // function call
    internalExpr.copy(code =
      s"""
        |${externalOperands.map(_.code).mkString("\n")}
        |$externalResultTerm = ($externalResultTypeTerm) $functionTerm.getClass().getMethod("eval").invoke($externalOperandTerms);
        |${internalExpr.code}
        |""".stripMargin)
  }

  private def prepareExternalOperands(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      argumentDataTypes: Seq[DataType])
    : Seq[GeneratedExpression] = {
    operands
      .zip(argumentDataTypes)
      .map { case (operand, dataType) =>
        operand.copy(resultTerm = genToExternalIfNeeded(ctx, dataType, operand))
      }
  }

  private def verifyArgumentTypes(
      operandTypes: Seq[LogicalType],
      enrichedDataTypes: Seq[DataType])
    : Unit = {
    val enrichedTypes = enrichedDataTypes.map(_.getLogicalType)
    if (operandTypes != enrichedTypes) {
      throw new CodeGenException(
        "Mismatch of inferred argument data types and logical argument types.")
    }
    // the data type class can only partially verify the conversion class,
    // now is the time for the final check
    enrichedDataTypes.foreach(dataType => {
      if (!dataType.getLogicalType.supportsOutputConversion(dataType.getConversionClass)) {
        throw new CodeGenException(
          s"Data type '$dataType' does not support an output conversion " +
            s"to class '${dataType.getConversionClass}'.")
      }
    })
  }

  private def verifyOutputType(
      outputType: LogicalType,
      enrichedDataType: DataType)
    : Unit = {
    val enrichedType = enrichedDataType.getLogicalType
    if (outputType != enrichedType) {
      throw new CodeGenException(
        "Mismatch of inferred output data type and logical output type.")
    }
    // the data type class can only partially verify the conversion class,
    // now is the time for the final check
    if (!enrichedType.supportsInputConversion(enrichedDataType.getConversionClass)) {
      throw new CodeGenException(
        s"Data type '$enrichedDataType' does not support an input conversion " +
          s"to class '${enrichedDataType.getConversionClass}'.")
    }
  }

  private def verifyImplementation(
      argumentDataTypes: Seq[DataType],
      outputDataType: DataType)
    : Unit = {
    val methods = toScala(ExtractionUtils.collectMethods(udf.getClass, "eval"))
    val argumentClasses = argumentDataTypes.map(_.getConversionClass).toArray
    val outputClass = outputDataType.getConversionClass
    def methodMatches(method: Method): Boolean = {
      isMethodInvokable(method, argumentClasses: _*) &&
        isAssignable(method.getReturnType, outputClass, true)
    }
    if (!methods.exists(methodMatches)) {
      throw new CodeGenException(
        s"Could not find a implementation method that matches the following signature: " +
          s"${createMethodSignatureString("eval", argumentClasses, outputClass)}")
    }
  }

  private class CodeGenerationCallContext(
      operands: Seq[GeneratedExpression],
      outputType: LogicalType)
    extends CallContext {

    private val argumentDataTypes = new util.AbstractList[DataType] {
      override def get(index: Int): DataType = {
        TypeConversions.fromLogicalToDataType(operands(index).resultType)
      }

      override def size(): Int = {
        operands.length
      }
    }

    override def getDataTypeLookup: DataTypeLookup = {
      throw new UnsupportedOperationException(
        "Data type lookup not available in code generation yet.")
    }

    override def getFunctionDefinition: FunctionDefinition = {
      function.getDefinition
    }

    override def isArgumentLiteral(pos: Int): Boolean = {
      operands(pos).literal
    }

    override def isArgumentNull(pos: Int): Boolean = {
      operands(pos).literalValue.contains(null)
    }

    override def getArgumentValue[T](pos: Int, clazz: Class[T]): Optional[T] = {
      val value = operands(pos)
        .literalValue
        .getOrElse(throw new IllegalArgumentException("Literal value expected."))
      if (clazz.isAssignableFrom(value.getClass)) {
        return Optional.of(value.asInstanceOf[T])
      }
      throw new UnsupportedOperationException("TODO") // TODO
    }

    override def getName: String = function.getNameAsId.toString

    override def getArgumentDataTypes: util.List[DataType] = {
      argumentDataTypes
    }

    override def getOutputDataType: Optional[DataType] = {
      Optional.of(TypeConversions.fromLogicalToDataType(outputType))
    }
  }
}
