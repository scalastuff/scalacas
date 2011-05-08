/**
 * Copyright (c) 2011 ScalaStuff.org (joint venture of Alexander Dvorkovyy and Ruud Diterwich)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.scalastuff.scalacas

import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.factory.HFactory

/**
 * Maps objects to SuperColumn.
 *
 * Provides SuperColumn name and list of columns, representing object properties.
 * Provides also back conversion from list of columns to object instance.
 * Subclass to provide your own mapping to your domain objects of given class.
 * Must be thread-safe.
 *
 * @author Alexander Dvorkovyy
 */
abstract class Mapper[A <: AnyRef](val prefix: String) {
  import Serializers._
  
  type Column = HColumn[String, Array[Byte]]

  def name(obj: A): String = {
    val sb = new StringBuilder(fullPrefix)
    sb ++= id(obj)
    sb.result
  }

  def name[P <: AnyRef](obj: A, parent: P)(implicit mP: Mapper[P]): String = {
    val sb = new StringBuilder(fullPrefix(parent))
    sb ++= id(obj)
    sb.result
  }

  val fullPrefix = prefix + " "

  def fullPrefix[P <: AnyRef](parent: P)(implicit mP: Mapper[P]): String = {
    val sb = new StringBuilder(mP.name(parent))
    sb ++= "/"
    sb ++= fullPrefix
    sb.result
  }

  def id(obj: A): String
  def objectToColumn(obj: A): Column
  def columnToObject(column: Column): A
  def createColumn(name: String, value: Array[Byte]) = {
	  HFactory.createColumn(name, value, stringSerializer, bytesSerializer).
	  asInstanceOf[Column]
  }
}