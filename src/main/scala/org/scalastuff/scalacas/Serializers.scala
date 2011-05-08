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

import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers._
import me.prettyprint.hector.api.Serializer

object Serializers {
  implicit val stringSerializer = StringSerializer.get()
  implicit val bytesSerializer = BytesArraySerializer.get()
  implicit val shortSerializer = ShortSerializer.get()
  implicit val intSerializer = new AbstractSerializer[Int] {
	val wrapped = IntegerSerializer.get()
	
	override def toByteBuffer(i: Int) = wrapped.toByteBuffer(i)
	override def fromByteBuffer(buffer: ByteBuffer): Int = wrapped.fromByteBuffer(buffer).intValue
	override def fromBytes(buffer: Array[Byte]): Int = wrapped.fromBytes(buffer).intValue
  }
  implicit val longSerializer = LongSerializer.get()
  
  def toBytes[A](a: A)(implicit s: Serializer[A]): Array[Byte] = s.toBytes(a)
  def fromBytes[A](buffer: Array[Byte])(implicit s: Serializer[A]): A = s.fromBytes(buffer)
}