package org.scalastuff.scalacas
import java.nio.ByteBuffer
import Serializers._
import java.util.Arrays
import me.prettyprint.cassandra.serializers.AbstractSerializer

/**
 * Value of entity key.
 *
 * Encapsulates byte array. Byte arrays are never changed in place, new copy is created when necessary.
 */
abstract class KeyValue {
  def bytes: Array[Byte]
  
  override def equals(other: Any) = other match {
    case otherKeyValue: KeyValue => bytes sameElements otherKeyValue.bytes
    case _ => false
  }
  
  override def hashCode() = bytes.foldLeft(0) { (sum: Int, b: Byte) => (sum + b) & 0xdfffffff }

  override def toString = bytes map ("%02x".format(_)) mkString ("[", ",", "]")
}

object KeyValue {
 
  object Serializer extends AbstractSerializer[KeyValue] {
    override def toByteBuffer(v: KeyValue) = {
      if (v != null) null
      else ByteBuffer.wrap(v.bytes)
    }
    override def fromByteBuffer(buffer: ByteBuffer): KeyValue = {
      if (buffer == null) {
        return null
      } else {
        val bytes = Array.ofDim[Byte](buffer.remaining())
        buffer.get(bytes, 0, bytes.length);
        new DeserializedKeyValue(bytes)
      }
    }
  }
}

class DeserializedKeyValue(val bytes: Array[Byte]) extends KeyValue

sealed abstract class SyntheticKeyValue extends KeyValue {
  def append(other: SyntheticKeyValue): SyntheticKeyValue
  
  // FIXME: totally broken
  def isPrefixOf(other: KeyValue): Boolean = {
    val otherBytes = other.bytes
    if (otherBytes.length < bytes.length) return false
    var i = 0
    while (i < bytes.length) {
      if (bytes(i) != otherBytes(i)) return false
      i += 1
    }

    true
  }

  protected def parts: Vector[Array[Byte]]
}

object SyntheticKeyValue {
  
  def apply[A](a: A)(implicit s: Serializer[A]): SyntheticKeyValue = new OneKeyValue(s.toBytes(a))

  object empty extends SyntheticKeyValue {
    val bytes = Array[Byte]()
    def append(other: SyntheticKeyValue) = other

    protected val parts = Vector.empty[Array[Byte]]
  }

  private class OneKeyValue(_bytes: Array[Byte]) extends SyntheticKeyValue {
    val bytes = _bytes
    def append(other: SyntheticKeyValue) = new CompoundKeyValue(this, other)

    protected val parts = Vector(_bytes)
  }

  private class CompoundKeyValue(component1: SyntheticKeyValue, component2: SyntheticKeyValue) extends SyntheticKeyValue {
    lazy val bytes = {
      if (parts.isEmpty) {
        empty.bytes
      } else if (parts.size == 1) {
        parts.head
      } else {
        val bytesLength = parts.foldLeft(0)(_ + _.length + 1)
        val buffer = ByteBuffer.allocate(bytesLength)

        var remaining = parts
        var count = 1
        while (remaining.size > 1) {
          buffer.put(remaining.head)
          buffer.put(0.asInstanceOf[Byte])

          remaining = remaining.tail
          count = count + 1
        }

        buffer.put(remaining.head)
        buffer.put(count.asInstanceOf[Byte])

        buffer.array()
      }
    }

    def append(other: SyntheticKeyValue) = new CompoundKeyValue(this, other)

    protected val parts = component1.parts ++ component2.parts
  }
}

class Key[T](keyValue: KeyValue) extends KeyValue {
  def bytes = keyValue.bytes
}

/**
 * Mixin trait to your domain object to let scalacas know how to get entity key.
 *
 * Several convenience implementations provided.
 *
 * @see [[HasStringKey]]
 * @see [[HasIntKey]]
 * @see [[HasLongKey]]
 */
trait HasKey {
  def keyValue: SyntheticKeyValue
}

trait HasStringKey extends HasKey {
  def id: String
  def keyValue = SyntheticKeyValue(id)
}

trait HasIntKey extends HasKey {
  def id: Int
  def keyValue = SyntheticKeyValue(id)
}

trait HasLongKey extends HasKey {
  def id: Long
  def keyValue = SyntheticKeyValue(id)
}

//////////////////////////////////////
// Key Paths - describe the key path
//////////////////////////////////////

/**
 * Key path is a function for creating entity keys.
 *
 * Examples:
 *
 * {{{
 * val rowKey = path[Person]
 * val key = rowKey(person)
 *
 * val invoiceLineColumnKey = path[Invoice] :: "L" :: path[InvoiceLine]
 * val key = invoiceLineColumnKey(invoice, invoiceLine)
 * }}}
 *
 * @see [[KeyPath1]]
 * @see [[CompositePath]]
 */
sealed abstract class KeyPath[H](val prefix: SyntheticKeyValue) {
  type This <: KeyPath[H]
  def withPrefix(prefix: SyntheticKeyValue): This
}

final class KeyPath1[H](_prefix: SyntheticKeyValue, val getKeyValue: H => SyntheticKeyValue) extends KeyPath[H](_prefix) {
  type This = KeyPath1[H]
  def ::[H0](v: KeyPath1[H0]) = new CompositePath(v.prefix, v.getKeyValue, this)
  def ::(_prefix: String) = new KeyPath1(SyntheticKeyValue(_prefix).append(prefix), getKeyValue)
  def withPrefix(_prefix: SyntheticKeyValue) = new KeyPath1(_prefix.append(prefix), getKeyValue)

  def apply(h: H) = new Key[H](prefix.append(getKeyValue(h)))
}

final class CompositePath[H, T <: KeyPath[_]](_prefix: SyntheticKeyValue, head: H => SyntheticKeyValue, val tail: T) extends KeyPath[H](_prefix) {
  type This = CompositePath[H, T]
  def ::[H0](v: KeyPath1[H0]) = new CompositePath(v.prefix, v.getKeyValue, this)
  def ::(_prefix: String) = new CompositePath(SyntheticKeyValue(_prefix).append(prefix), head, tail)
  def withPrefix(_prefix: SyntheticKeyValue) = new CompositePath(_prefix.append(prefix), head, tail)

  def apply(h: H) = tail.withPrefix(prefix.append(head(h)))
}

trait Keys {
  trait KeyExtractor[A] extends (A => SyntheticKeyValue)

  implicit def hasId2KeyExtractor[A <: HasKey] = new KeyExtractor[A] { def apply(a: A) = a.keyValue }
  implicit def ser2KeyExtractor[A: Serializer] = new KeyExtractor[A] { def apply(a: A) = SyntheticKeyValue(a) }

  def path[A](implicit keyExtractor: KeyExtractor[A]) = new KeyPath1[A](SyntheticKeyValue.empty, keyExtractor)

  ///////////////////////////////
  // Shortcuts for up to 5 beans
  ///////////////////////////////

  type KeyPath2[A, B] = CompositePath[A, KeyPath1[B]]
  type KeyPath3[A, B, C] = CompositePath[A, KeyPath2[B, C]]
  type KeyPath4[A, B, C, D] = CompositePath[A, KeyPath3[B, C, D]]
  type KeyPath5[A, B, C, D, E] = CompositePath[A, KeyPath4[B, C, D, E]]

  implicit def applyKeyPath2[A, B](path: KeyPath2[A, B]) = { (a: A, b: B) => path(a)(b) }
  implicit def applyKeyPath3[A, B, C](path: KeyPath3[A, B, C]) = { (a: A, b: B, c: C) => path(a)(b)(c) }
  implicit def applyKeyPath4[A, B, C, D](path: KeyPath4[A, B, C, D]) = { (a: A, b: B, c: C, d: D) => path(a)(b)(c)(d) }
  implicit def applyKeyPath5[A, B, C, D, E](path: KeyPath5[A, B, C, D, E]) = { (a: A, b: B, c: C, d: D, e: E) => path(a)(b)(c)(d)(e) }
}

object Keys extends Keys with Serializers