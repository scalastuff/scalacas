package org.scalastuff.scalacas.keys
import java.nio.charset.Charset
import java.nio.ByteBuffer

trait IdCodec[A] {
  def encode(a: A): Array[Byte]
  def canDecode(b: KeyValue, prefixLength: Int): Boolean
  //  def decode(b: Array[Byte], prefixLength: Int): Option[A]
}

/**
 * Abstracts from concrete entity Id type
 */
trait EntityIdCodec[B] {
  def encodeId(b: B): KeyValue
  def canDecodeId(b: KeyValue, prefixLength: Int): Boolean

  //  /**
  //   * Need implicits here to get concrete entity id type
  //   */
  //  def decodeId[K](b: Array[Byte], prefixLength: Int)(implicit e: Identify[B, K], s: IdCodec[K]): Option[K] = {
  //    s.decode(b, prefixLength)
  //  }
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

    def canDecode(b: KeyValue, prefixLength: Int): Boolean = !(b.iterator.drop(prefixLength) contains 0)
  }
  
  class FixedLengthCodec[F](val byteLength: Int, put: (ByteBuffer, F) => Unit) extends IdCodec[F] {
    def encode(v: F): Array[Byte] = {
      val bb = ByteBuffer.allocate(byteLength)
      put(bb, v)
      bb.array()
    }

    def canDecode(b: KeyValue, prefixLength: Int): Boolean = (b.length - prefixLength) == byteLength
  }

  object LongIdCodec extends FixedLengthCodec[Long](8, _.putLong(_))
  object IntIdCodec extends FixedLengthCodec[Int](4, _.putInt(_))
  object ShortIdCodec extends FixedLengthCodec[Short](2, _.putShort(_))
  object ByteIdCodec extends FixedLengthCodec[Byte](1, _.put(_))
}