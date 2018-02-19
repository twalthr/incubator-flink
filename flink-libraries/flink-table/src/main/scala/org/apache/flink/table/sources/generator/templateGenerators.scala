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
import java.util.{Currency, Formatter, Locale, UUID}

import org.apache.flink.table.api.TableException
import org.apache.flink.table.descriptors.DescriptorProperties
import org.apache.flink.table.sources.generator.DataGeneratorValidator._

import scala.collection.JavaConverters._

class NameGenerator extends DataGenerator[String] {

  var firstName: Boolean = true
  var lastName: Boolean = true

  override def configure(properties: DescriptorProperties): Unit = {
    properties.getString(NAMES).foreach {
      case NAMES_VALUE_FIRST =>
        firstName = true
        lastName = false
      case NAMES_VALUE_LAST =>
        firstName = false
        lastName = true
      case NAMES_VALUE_FULL =>
        firstName = true
        lastName = true
    }
  }

  val names: Array[String] = {
    val url = getClass.getClassLoader.getResource("contributors-dataset.txt")
    if (url == null) {
      throw new TableException("Could not find data set 'contributors-dataset.txt'.")
    }
    val lines = Files.readAllLines(Paths.get(url.toURI))
    lines.asScala.map { l =>
      if (firstName && lastName) {
        l.replace('|', ' ') // full name
      } else if (firstName) {
        l.split('|')(0) // first name
      } else {
        l.split('|')(1) // second name
      }
    }.toArray
  }

  val nameGenerator: DataGenerator[Int] = {
    val gen = new IntGenerator
    gen.min = 0
    gen.max = names.length - 1
    gen
  }

  override def generate(context: DataGeneratorContext): String = {
    names(nameGenerator.generate(context))
  }
}

abstract class AbstractLocaleGenerator extends DataGenerator[String] {

  var displayLocale: Locale = _
  var availableLocales: Array[Locale] = Locale.getAvailableLocales

  val displayLocaleGenerator: DataGenerator[Int] = {
    val gen = new IntGenerator
    gen.min = 0
    gen.max = availableLocales.length - 1
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
    availableLocales(displayLocaleGenerator.generate(context)).getDisplayCountry(displayLocale)
  }
}

class LocaleLanguageGenerator extends AbstractLocaleGenerator {

  override def generate(context: DataGeneratorContext): String = {
    availableLocales(displayLocaleGenerator.generate(context)).getDisplayLanguage(displayLocale)
  }
}

class LocaleVariantGenerator extends AbstractLocaleGenerator {

  override def generate(context: DataGeneratorContext): String = {
    availableLocales(displayLocaleGenerator.generate(context)).getDisplayVariant(displayLocale)
  }
}

class LocaleGenerator(part: Int) extends AbstractLocaleGenerator {

  override def generate(context: DataGeneratorContext): String = {
    val locale = availableLocales(displayLocaleGenerator.generate(context))
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
      val locale = availableLocales(displayLocaleGenerator.generate(context))
      currency = Currency.getInstance(locale)
    } while (currency == null)
    currency.getDisplayName(displayLocale)
  }
}

class CurrencyGenerator extends AbstractLocaleGenerator {

  override def generate(context: DataGeneratorContext): String = {
    var currency: Currency = null
    do {
      val locale = availableLocales(displayLocaleGenerator.generate(context))
      currency = Currency.getInstance(locale)
    } while (currency == null)
    currency.getCurrencyCode
  }
}

class CurrencySymbolGenerator extends AbstractLocaleGenerator {

  override def generate(context: DataGeneratorContext): String = {
    var currency: Currency = null
    do {
      val locale = availableLocales(displayLocaleGenerator.generate(context))
      currency = Currency.getInstance(locale)
    } while (currency == null)
    currency.getSymbol(displayLocale)
  }
}

class CurrencyFormattedGenerator extends AbstractLocaleGenerator {

  val amountGenerator: DoubleGenerator = new DoubleGenerator
  amountGenerator.min = 0
  amountGenerator.max = 10000

  override def configure(properties: DescriptorProperties): Unit = {
    super.configure(properties)
    amountGenerator.configure(properties)
  }

  override def generate(context: DataGeneratorContext): String = {
    val locale = availableLocales(displayLocaleGenerator.generate(context))
    val format = NumberFormat.getCurrencyInstance(locale)
    val value = amountGenerator.generate(context)
    format.format(value)
  }
}

class IPv4Generator extends DataGenerator[String] {

  val sb: lang.StringBuilder = new lang.StringBuilder()

  val partGenerator: DataGenerator[Int] = {
    val gen = new IntGenerator
    gen.min = 1
    gen.max = 254
    gen
  }

  override def generate(context: DataGeneratorContext): String = {
    sb.setLength(0)
    sb.append(partGenerator.generate(context))
    sb.append('.')
    sb.append(partGenerator.generate(context))
    sb.append('.')
    sb.append(partGenerator.generate(context))
    sb.append('.')
    sb.append(partGenerator.generate(context))
    sb.toString
  }
}

class IPv6Generator extends DataGenerator[String] {

  val sb: lang.StringBuilder = new lang.StringBuilder()
  var formatter: Formatter = _

  val partGenerator: DataGenerator[Short] = {
    val gen = new ShortGenerator
    gen.min = 0
    gen
  }

  override def open(): Unit = {
    formatter = new Formatter(sb)
  }

  override def generate(context: DataGeneratorContext): String = {
    formatter.format("2001:0db8:%x:%x:%x:%x:%x:%x",
      partGenerator.generate(context),
      partGenerator.generate(context),
      partGenerator.generate(context),
      partGenerator.generate(context),
      partGenerator.generate(context),
      partGenerator.generate(context))
    formatter.toString
  }
}

class UUIDGenerator extends DataGenerator[String] {

  override def generate(context: DataGeneratorContext): String = {
    if (context.hasSeed) {
      new UUID(context.random.nextLong(), context.random.nextLong()).toString
    } else {
      UUID.randomUUID().toString
    }
  }
}

class IdGenerator extends DataGenerator[String] {

  val sb: lang.StringBuilder = new lang.StringBuilder()

  val chars: Array[Char] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray

  val charGenerator: IntGenerator = new IntGenerator
  lengthGenerator.min = 0
  lengthGenerator.max = chars.length - 1

  val lengthGenerator: IntGenerator = new IntGenerator
  lengthGenerator.min = 32
  lengthGenerator.max = 32

  override def configure(properties: DescriptorProperties): Unit = {
    properties.getInt(MIN_LENGTH).foreach(lengthGenerator.min = _)
    properties.getInt(MAX_LENGTH).foreach(lengthGenerator.max = _)
  }

  override def generate(context: DataGeneratorContext): String = {
    sb.setLength(0)
    val len = lengthGenerator.generate(context)
    while (sb.length() < len) {
      sb.append(chars(charGenerator.generate(context)))
    }
    sb.toString
  }
}

class TagGenerator extends DataGenerator[String] {

  val tags: Array[String] = {
    val url = getClass.getClassLoader.getResource("tags-dataset.txt")
    if (url == null) {
      throw new TableException("Could not find data set 'tags-dataset.txt'.")
    }
    Files.readAllLines(Paths.get(url.toURI)).asScala.toArray
  }

  val tagGenerator: DataGenerator[Int] = {
    val gen = new IntGenerator
    gen.min = 0
    gen.max = tags.length - 1
    gen
  }

  override def generate(context: DataGeneratorContext): String = {
    tags(tagGenerator.generate(context))
  }
}

class UrlGenerator extends DataGenerator[String] {

  val sb: lang.StringBuilder = new lang.StringBuilder()

  val tagGenerator: DataGenerator[String] = new TagGenerator

  val hostGenerator: DataGenerator[Boolean] = new BooleanGenerator

  val variationGenerator: DataGenerator[Int] = {
    val gen = new IntGenerator
    gen.min = 0
    gen.max = 4
    gen
  }

  val tldGenerator: DataGenerator[String] = {
    val gen = new EnumGenerator[String]
    gen.array = Array("com", "cn", "de", "net", "co.uk", "org", "nl", "info", "ru", "br", "eu",
      "au", "fr", "it", "in", "edu", "biz")
    gen
  }

  var withProtocol: Boolean = true
  var withPath: Boolean = true

  override def configure(properties: DescriptorProperties): Unit = {
    properties.getBoolean(WITH_PROTOCOL).foreach(withProtocol = _)
    properties.getBoolean(WITH_PATH).foreach(withPath = _)
  }

  override def generate(context: DataGeneratorContext): String = {
    sb.setLength(0)
    if (withProtocol) {
      sb.append("http://")
    }
    sb.append(tagGenerator.generate(context))
    // two words or one
    if (hostGenerator.generate(context)) {
      sb.append(tagGenerator.generate(context))
    }
    sb.append(tldGenerator.generate(context))
    if (withPath) {
      var i = variationGenerator.generate(context)
      while (i > 0) {
        sb.append('/')
        sb.append(tagGenerator.generate(context))
        i -= 1
      }
    }
    sb.toString
  }
}

class MailGenerator extends DataGenerator[String] {

  val sb: lang.StringBuilder = new lang.StringBuilder()

  var urlGenerator: DataGenerator[String] = {
    val gen = new UrlGenerator
    gen.withProtocol = false
    gen.withPath = false
    gen
  }

  var usernameGenerator: DataGenerator[String] = new UsernameGenerator

  override def generate(context: DataGeneratorContext): String = {
    sb.setLength(0)
    sb.append(usernameGenerator.generate(context))
    sb.append('@')
    sb.append(urlGenerator.generate(context))
    sb.toString
  }
}

// --------------------------------------------------------------------------------

// todo consistence across locale/name

// todo date, time, timestamp

class UsernameGenerator extends DataGenerator[String] {

  val sb: lang.StringBuilder = new lang.StringBuilder()

  val nameGenerator: DataGenerator[String] = new NameGenerator()
  val suffixGenerator: DataGenerator[Int] = {
    val gen = new IntGenerator
    gen.min = 0
    gen.max = 150
    gen
  }

  override def generate(context: DataGeneratorContext): String = {
    // remove non-ascii letters
    val name = nameGenerator.generate(context).toLowerCase.replaceAll("[^\\x00-\\x7F]", "")
    val suffix = suffixGenerator.generate(context)
    if (suffix > 100) { // 50 % have no suffix
      name
    } else if (suffix % 3 == 0) {
      name + '_' + suffix
    } else {
      name + suffix
    }
  }
}

class ColorGenerator extends EnumGenerator[String] {

  val colors = Array("black", "blue", "cyan", "dark grey", "grey", "green", "light grey", "magenta",
    "orange", "pink", "red", "white", "yellow")


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
  intGenerator.min = 27
  intGenerator.max = 446

  override def configure(properties: DescriptorProperties): Unit = {
    properties.getInt(MIN_LENGTH).foreach(intGenerator.min = _)
    properties.getInt(MAX_LENGTH).foreach(intGenerator.max = _)
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
