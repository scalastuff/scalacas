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
package org.scalastuff.scalacas.keys
import java.nio.charset.Charset
import java.nio.ByteBuffer

trait IdCodec[A] {
  def encode(a: A): Array[Byte]
  def canDecode(b: KeyValue, prefixLength: Int, length: Int): Boolean
  //  def decode(b: Array[Byte], prefixLength: Int): Option[A]
}

/**
 * Abstracts from concrete entity Id type
 */
trait EntityIdCodec[B] {
  def encodeId(b: B): KeyValue
  def canDecodeId(b: KeyValue, start: Int, length: Int): Boolean

  //  /**
  //   * Need implicits here to get concrete entity id type
  //   */
  //  def decodeId[K](b: Array[Byte], prefixLength: Int)(implicit e: Identify[B, K], s: IdCodec[K]): Option[K] = {
  //    s.decode(b, prefixLength)
  //  }
}
object EntityIdCodec {
  def apply[B, A](f: B => A)(implicit c: IdCodec[A]) = new EntityIdCodec[B] {
    def encodeId(b: B) = KeyValue(f(b))
    def canDecodeId(b: KeyValue, start: Int, length: Int) = c.canDecode(b, start, length)
  }
}

trait IdCodecs {
  import IdCodecs._
  implicit def stringIdCodec = StringIdCodec
  implicit def longIdCodec = LongIdCodec
  implicit def intIdCodec = IntIdCodec
  implicit def shortIdCodec = ShortIdCodec
  implicit def byteIdCodec = ByteIdCodec
}

object IdCodecs extends IdCodecs {
  object StringIdCodec extends IdCodec[String] {
    val charset = Charset.forName("UTF-8")
    def encode(v: String): Array[Byte] = {
      val result = v.getBytes(charset)
      if (result contains 0)
        throw new UnsupportedOperationException("Cannot encode string id: it contains 0x00 char")

      result
    }

    def canDecode(b: KeyValue, start: Int, length: Int): Boolean = !(b.iterator.drop(start).take(length) contains 0)
  }
  
  class FixedLengthCodec[F](val byteLength: Int, put: (ByteBuffer, F) => Unit) extends IdCodec[F] {
    def encode(v: F): Array[Byte] = {
      val bb = ByteBuffer.allocate(byteLength)
      put(bb, v)
      bb.array()
    }

    def canDecode(b: KeyValue, start: Int, length: Int): Boolean = (length == byteLength) && (b.length >= start + length)
  }

  object LongIdCodec extends FixedLengthCodec[Long](8, _.putLong(_))
  object IntIdCodec extends FixedLengthCodec[Int](4, _.putInt(_))
  object ShortIdCodec extends FixedLengthCodec[Short](2, _.putShort(_))
  object ByteIdCodec extends FixedLengthCodec[Byte](1, _.put(_))
}