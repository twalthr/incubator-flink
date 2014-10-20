package org.apache.flink.api.scala.runtime

import org.apache.flink.api.common.typeutils.{TypeSerializer, SerializerTestInstance}
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.junit.Test

import scala.reflect._
import scala.reflect.runtime.universe._

class KryoGenericTypeSerializerTest {

  @Test
  def testScalaListSerialization: Unit = {
    val a = List(42,1,49,1337)

    runTests(a)
  }

  @Test
  def testScalaMutablelistSerialization: Unit = {
    val a = scala.collection.mutable.ListBuffer(42,1,49,1337)

    runTests(a)
  }

  @Test
  def testScalaMapSerialization: Unit = {
    val a = Map(("1" -> 1), ("2" -> 2), ("42" -> 42), ("1337" -> 1337))

    runTests(a)
  }

  def runTests[T : ClassTag](objects: T *): Unit ={
    val clsTag = classTag[T]
    val typeInfo = new GenericTypeInfo[T](clsTag.runtimeClass.asInstanceOf[Class[T]])
    val serializer = typeInfo.createSerializer()
    val typeClass = typeInfo.getTypeClass

    val instance = new SerializerTestInstance[T](serializer, typeClass, -1, objects: _*)

    instance.testAll()
  }

}
