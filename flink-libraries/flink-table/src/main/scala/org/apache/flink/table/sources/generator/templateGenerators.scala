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

package org.apache.flink.table.sources.generator

import java.lang
import java.nio.file.{Files, Paths}
import java.text.NumberFormat
import java.util.logging.SimpleFormatter
import java.util.{Currency, Formatter, Locale}

import org.apache.flink.table.api.TableException
import org.apache.flink.table.descriptors.DescriptorProperties
import org.apache.flink.table.sources.generator.DataGeneratorValidator._

import scala.collection.JavaConverters._

class NameGenerator(part: Int) extends DataGenerator[String] {

  val names: Array[String] = {
    val url = getClass.getClassLoader.getResource("contributors-dataset.txt")
    if (url == null) {
      throw new TableException("Could not find data set 'contributors-dataset.txt'.")
    }
    val lines = Files.readAllLines(Paths.get(url.toURI))
    lines.asScala.map { l =>
      if (part == 0) {
        l.split('|')(0) // first name
      } else if (part == 1) {
        l.split('|')(1) // second name
      } else {
        l.replace('|', ' ') // full name
      }
    }.toArray
  }

  val intGenerator: DataGenerator[Int] = {
    val gen = new IntGenerator
    gen.min = 0
    gen.max = names.length
    gen
  }

  override def generate(context: DataGeneratorContext): String = {
    names(intGenerator.generate(context))
  }
}

abstract class AbstractLocaleGenerator extends DataGenerator[String] {

  var displayLocale: Locale = _
  var availableLocales: Array[Locale] = Locale.getAvailableLocales

  val intGenerator: DataGenerator[Int] = {
    val gen = new IntGenerator
    gen.min = 0
    gen.max = availableLocales.length
    gen
  }

  override def configure(properties: DescriptorProperties): Unit = {
    val language = properties.getString(LOCALE_LANGUAGE).getOrElse("")
    val country = properties.getString(LOCALE_COUNTRY).getOrElse("")
    val variant = properties.getString(LOCALE_VARIANT).getOrElse("")

    if (country.isEmpty && language.isEmpty && variant.isEmpty) {
      displayLocale = Locale.US
    } else {
      displayLocale = new Locale(language, country, variant)
    }
  }
}

class LocaleCountryGenerator extends AbstractLocaleGenerator {

  override def generate(context: DataGeneratorContext): String = {
    availableLocales(intGenerator.generate(context)).getDisplayCountry(displayLocale)
  }
}

class LocaleLanguageGenerator extends AbstractLocaleGenerator {

  override def generate(context: DataGeneratorContext): String = {
    availableLocales(intGenerator.generate(context)).getDisplayLanguage(displayLocale)
  }
}

class LocaleVariantGenerator extends AbstractLocaleGenerator {

  override def generate(context: DataGeneratorContext): String = {
    availableLocales(intGenerator.generate(context)).getDisplayVariant(displayLocale)
  }
}

class LocaleGenerator(part: Int) extends AbstractLocaleGenerator {

  override def generate(context: DataGeneratorContext): String = {
    val locale = availableLocales(intGenerator.generate(context))
    if (part == 0) {
      locale.getLanguage // en
    } else if (part == 1) {
      locale.getCountry // US
    } else if (part == 2) {
      locale.getVariant
    } else {
      locale.toString // en_US
    }
  }
}

class CurrencyNameGenerator extends AbstractLocaleGenerator {

  override def generate(context: DataGeneratorContext): String = {
    var currency: Currency = null
    do {
      val locale = availableLocales(intGenerator.generate(context))
      currency = Currency.getInstance(locale)
    } while (currency == null)
    currency.getDisplayName(displayLocale)
  }
}

class CurrencyGenerator extends AbstractLocaleGenerator {

  override def generate(context: DataGeneratorContext): String = {
    var currency: Currency = null
    do {
      val locale = availableLocales(intGenerator.generate(context))
      currency = Currency.getInstance(locale)
    } while (currency == null)
    currency.getCurrencyCode
  }
}

class CurrencySymbolGenerator extends AbstractLocaleGenerator {

  override def generate(context: DataGeneratorContext): String = {
    var currency: Currency = null
    do {
      val locale = availableLocales(intGenerator.generate(context))
      currency = Currency.getInstance(locale)
    } while (currency == null)
    currency.getSymbol(displayLocale)
  }
}

class CurrencyFormattedGenerator extends AbstractLocaleGenerator {

  val doubleGenerator: DataGenerator[Double] = new DoubleGenerator

  override def configure(properties: DescriptorProperties): Unit = {
    super.configure(properties)
    doubleGenerator.configure(properties)
  }

  override def generate(context: DataGeneratorContext): String = {
    val locale = availableLocales(intGenerator.generate(context))
    val format = NumberFormat.getCurrencyInstance(locale)
    val value = doubleGenerator.generate(context)
    format.format(value)
  }
}

class IPv4Generator extends DataGenerator[String] {

  val sb: lang.StringBuilder = new lang.StringBuilder()

  val intGenerator: DataGenerator[Int] = {
    val gen = new IntGenerator
    gen.min = 1
    gen.max = 254
    gen
  }

  override def generate(context: DataGeneratorContext): String = {
    sb.setLength(0)
    sb.append(intGenerator.generate(context))
    sb.append('.')
    sb.append(intGenerator.generate(context))
    sb.append('.')
    sb.append(intGenerator.generate(context))
    sb.append('.')
    sb.append(intGenerator.generate(context))
    sb.toString
  }
}

class IPv6Generator extends DataGenerator[String] {

  val sb: lang.StringBuilder = new lang.StringBuilder()
  var formatter: Formatter = _

  val shortGenerator: DataGenerator[Short] = {
    val gen = new ShortGenerator
    gen.min = 0
    gen
  }

  override def open(): Unit = {
    formatter = new Formatter(sb)
  }

  override def generate(context: DataGeneratorContext): String = {
    formatter.format("2001:0db8:%x:%x:%x:%x:%x:%x",
      shortGenerator.generate(context),
      shortGenerator.generate(context),
      shortGenerator.generate(context),
      shortGenerator.generate(context),
      shortGenerator.generate(context),
      shortGenerator.generate(context))
    formatter.toString
  }
}

class SentenceGenerator extends DataGenerator[String] {

  val sb: lang.StringBuilder = new lang.StringBuilder()

  val s: String =
    """Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
      |incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud
      |exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure
      |dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
      |Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit
      |anim id est laborum.""".stripMargin

  val intGenerator: IntGenerator = new IntGenerator

  override def configure(properties: DescriptorProperties): Unit = {
    val minLen = properties.getInt(MIN_LENGTH).getOrElse(27)
    intGenerator.min = minLen
    val maxLen = properties.getInt(MAX_LENGTH).getOrElse(446)
    intGenerator.max = maxLen
  }

  override def generate(context: DataGeneratorContext): String = {
    sb.setLength(0)
    val len = intGenerator.generate(context)
    do {
      sb.append(s.substring(0, Math.min(s.length, Math.max(0, len - 1))))
    } while (sb.length() < len - 1)
    sb.append('.')
    sb.toString
  }
}

class UsernameGenerator extends DataGenerator[String] {

  val nameGenerator: DataGenerator[String] = new NameGenerator(1)
  val intGenerator: DataGenerator[Int] = {
    val gen = new IntGenerator
    gen.min = 0
    gen.max= 150
    gen
  }

  override def generate(context: DataGeneratorContext): String = {
    // remove non-ascii letters
    val name = nameGenerator.generate(context).toLowerCase.replaceAll("[^\\x00-\\x7F]", "")
    val suffix = intGenerator.generate(context)
    if (suffix > 100) { // 50 % have no suffix
      name
    } else if (suffix % 3 == 0) {
      name + '_' + suffix
    } else {
      name + suffix
    }
  }
}

class MailGenerator extends DataGenerator[String] {

  override def generate(context: DataGeneratorContext): String = {

  }
}
