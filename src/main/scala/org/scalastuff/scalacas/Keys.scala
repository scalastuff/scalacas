package org.scalastuff.scalacas
import java.nio.ByteBuffer
import Serializers._
import java.util.Arrays

/**
 * Value of entity key.
 *
 * Encapsulates byte array. Byte arrays are never changed in place, new copy is created when necessary.
 */
class KeyValue(val bytes: Array[Byte]) {
  def append(other: KeyValue): KeyValue = {
    if (other.bytes.length == 0) this
    else {
      val newArray = Arrays.copyOf(bytes, bytes.length + other.bytes.length + 1)
      newArray(bytes.length) = 0 // delimiter
      System.arraycopy(other.bytes, 0, newArray, bytes.length + 1, other.bytes.length)
      new KeyValue(newArray)
    }
  }

  /**
   * Returns new KeyValue instance with given byte appended at the end of the byte array
   */
  def append(last: Byte): KeyValue = {
    val newArray = Arrays.copyOf(bytes, bytes.length + 1)
    newArray(bytes.length) = last
    new KeyValue(newArray)
  }

  def append(last: Int): KeyValue = append(last.asInstanceOf[Byte])

  def isPrefixOf(otherBytes: Array[Byte]): Boolean = {
    if (otherBytes.length < bytes.length) return false
    var i = 0
    while (i < bytes.length) {
      if (bytes(i) != otherBytes(i)) return false
      i += 1
    }

    true
  }

  override def equals(other: Any) = other match {
    case otherKeyValue: KeyValue => bytes sameElements otherKeyValue.bytes
    case _ => false
  }

  override def hashCode() = bytes.foldLeft(0) { (sum: Int, b: Byte) => (sum + b) & 0xffffff }

  override def toString = bytes map ("%02x".format(_)) mkString ("[", ",", "]")
}

object KeyValue {
  val empty = new KeyValue(Array.ofDim[Byte](0)) {
    // little optimization
    override def append(other: KeyValue) = other
  }

  def apply[K: Serializer](k: K) = new KeyValue(toBytes(k))
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
  def keyValue: KeyValue
}

trait HasStringKey extends HasKey {
  def id: String
  def keyValue = KeyValue(id)
}

trait HasIntKey extends HasKey {
  def id: Int
  def keyValue = KeyValue(id)
}

trait HasLongKey extends HasKey {
  def id: Long
  def keyValue = KeyValue(id)
}

class Key[T](_bytes: Array[Byte]) extends KeyValue(_bytes)

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
sealed abstract class KeyPath[H](val prefix: KeyValue) {
  type This <: KeyPath[H]
  def withPrefix(prefix: KeyValue): This
}

final class KeyPath1[H](_prefix: KeyValue, val getKeyValue: H => KeyValue) extends KeyPath[H](_prefix) {
  type This = KeyPath1[H]
  def ::[H0](v: KeyPath1[H0]) = new CompositePath(v.prefix, v.getKeyValue, this)
  def ::(_prefix: String) = new KeyPath1(KeyValue(_prefix).append(prefix), getKeyValue)
  def withPrefix(_prefix: KeyValue) = new KeyPath1(_prefix.append(prefix), getKeyValue)

  def apply(h: H) = new Key[H](prefix.append(getKeyValue(h)).bytes)
}

final class CompositePath[H, T <: KeyPath[_]](_prefix: KeyValue, head: H => KeyValue, val tail: T) extends KeyPath[H](_prefix) {
  type This = CompositePath[H, T]
  def ::[H0](v: KeyPath1[H0]) = new CompositePath(v.prefix, v.getKeyValue, this)
  def ::(_prefix: String) = new CompositePath(KeyValue(_prefix).append(prefix), head, tail)
  def withPrefix(_prefix: KeyValue) = new CompositePath(_prefix.append(prefix), head, tail)

  def apply(h: H) = tail.withPrefix(prefix.append(head(h)))
}

trait Keys {
  trait KeyExtractor[A] extends (A => KeyValue)

  implicit def hasId2KeyExtractor[A <: HasKey] = new KeyExtractor[A] { def apply(a: A) = a.keyValue }
  implicit def ser2KeyExtractor[A: Serializer] = new KeyExtractor[A] { def apply(a: A) = KeyValue(a) }

  def path[A](implicit keyExtractor: KeyExtractor[A]) = new KeyPath1[A](KeyValue.empty, keyExtractor)

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