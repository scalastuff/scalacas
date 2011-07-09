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
package org.scalastuff.scalacas.beans

import org.scalastuff.scalacas.{Mapper, Key, HasKey}
import org.scalastuff.proto.Preamble._
import org.scalastuff.proto._

/**
 * Maps JavaBeans of given class to protobuf byte array and saves it in '-protobuf' column. <p>
 *
 * @author Alexander Dvorkovyy
 */
class ProtobufMapper[A <: AnyRef]()(implicit mf: Manifest[A]) extends AbstractProtobufMapper[A]

abstract class AbstractProtobufMapper[A <: AnyRef]()(implicit mf: Manifest[A]) extends Mapper[A] {
  val reader = readerOf[A]
  val writer = writerOf[A]
  val format = ProtobufFormat
  
  def objectToBytes(obj: A) = writer.toByteArray(obj, format)
  def columnToObject(column: Column): A = reader.readFrom(column.getValue, ProtobufFormat)
}