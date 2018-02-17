package org.apache.flink.table.sources.generator

import org.apache.flink.table.descriptors.{DescriptorProperties, DescriptorValidator}

class DataGeneratorValidator extends DescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = ???

}

object DataGeneratorValidator {

  val MAX_VALUE = "max-value"
  val MIN_VALUE = "min-value"
  val MIN_LENGTH = "min-length"
  val MAX_LENGTH = "max-length"
  val LOCALE_LANGUAGE = "language"
  val LOCALE_COUNTRY = "country"
  val LOCALE_VARIANT = "variant"

}
