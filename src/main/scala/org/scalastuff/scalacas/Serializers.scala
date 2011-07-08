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

trait Serializers {
  type Serializer[A] = me.prettyprint.hector.api.Serializer[A]
  
  implicit val stringSerializer = StringSerializer.get()
  val bytesSerializer = BytesArraySerializer.get()
  
  implicit val byteSerializer = new AbstractSerializer[Byte] {
    override def toByteBuffer(v: Byte) = {
      val bb = ByteBuffer.allocate(1)
      bb.put(v)
      bb.rewind()
      bb
    }
    override def fromByteBuffer(buffer: ByteBuffer): Byte = buffer.get()
    override def fromBytes(buffer: Array[Byte]): Byte = buffer(0)
  }
  
  implicit val shortSerializer = new AbstractSerializer[Short] {
    val wrapped = ShortSerializer.get()

    override def toByteBuffer(v: Short) = wrapped.toByteBuffer(v)
    override def fromByteBuffer(buffer: ByteBuffer): Short = wrapped.fromByteBuffer(buffer).shortValue
    override def fromBytes(buffer: Array[Byte]): Short = wrapped.fromBytes(buffer).shortValue
  }
  implicit val intSerializer = new AbstractSerializer[Int] {
    val wrapped = IntegerSerializer.get()

    override def toByteBuffer(i: Int) = wrapped.toByteBuffer(i)
    override def fromByteBuffer(buffer: ByteBuffer): Int = wrapped.fromByteBuffer(buffer).intValue
    override def fromBytes(buffer: Array[Byte]): Int = wrapped.fromBytes(buffer).intValue
  }
  implicit val longSerializer = new AbstractSerializer[Long] {
    val wrapped = LongSerializer.get()

    override def toByteBuffer(v: Long) = wrapped.toByteBuffer(v)
    override def fromByteBuffer(buffer: ByteBuffer): Long = wrapped.fromByteBuffer(buffer).longValue
    override def fromBytes(buffer: Array[Byte]): Long = wrapped.fromBytes(buffer).longValue
  }

  def toBytes[A](a: A)(implicit s: Serializer[A]): Array[Byte] = s.toBytes(a)
  def fromBytes[A](buffer: Array[Byte])(implicit s: Serializer[A]): A = s.fromBytes(buffer)
}

object Serializers extends Serializers