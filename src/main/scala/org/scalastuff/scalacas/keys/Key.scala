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
import me.prettyprint.cassandra.serializers.AbstractSerializer
import java.nio.ByteBuffer
import java.util.Arrays

/**
 * Value of entity key.
 *
 * Encapsulates byte array. Byte arrays are never changed in place, new copy is created when necessary.
 */
class KeyValue private[KeyValue] (private[KeyValue] val bytes: Array[Byte]) {
  def append(other: KeyValue): KeyValue = {
    if (other.bytes.length == 0) this
    else {
      val newArray = Arrays.copyOf(bytes, bytes.length + other.bytes.length + 1)
      newArray(bytes.length) = 0 // delimiter
      System.arraycopy(other.bytes, 0, newArray, bytes.length + 1, other.bytes.length)
      new KeyValue(newArray)
    }
  }
  
  protected[keys] def withSuffix(suffix: Byte): KeyValue = {
    val newArray = Arrays.copyOf(bytes, bytes.length + 1)
    newArray(bytes.length) = suffix
    new KeyValue(newArray)
  }
  
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
  
  def isSuffixOf(other: KeyValue): Boolean = {
    val otherBytes = other.bytes
    if (otherBytes.length < bytes.length) return false
    var i = 0
    val startOtherBytes = otherBytes.length - bytes.length
    while (i < bytes.length) {
      if (bytes(i) != otherBytes(startOtherBytes + i)) return false
      i += 1
    }

    true
  }
  
  @inline
  def length = bytes.length
  
  @inline
  def iterator = bytes.iterator
  
  override def equals(other: Any) = other match {
    case otherKeyValue: KeyValue => bytes sameElements otherKeyValue.bytes
    case otherKey: Key[_] => bytes sameElements otherKey.value.bytes
    case _ => false
  }
  
  override def hashCode() = bytes.foldLeft(0) { (sum: Int, b: Byte) => (sum + b) & 0xdfffffff }

  override def toString = bytes map ("%02x".format(_)) mkString ("[", ",", "]")  
}

object KeyValue {
  def apply[A](a: A)(implicit s: IdCodec[A]): KeyValue = new KeyValue(s.encode(a))
  
  object Serializer extends AbstractSerializer[KeyValue] {
    override def toByteBuffer(v: KeyValue) = {
      if (v == null) null
      else ByteBuffer.wrap(v.bytes)
    }
    override def fromByteBuffer(buffer: ByteBuffer): KeyValue = {
      if (buffer == null) {
        return null
      } else {
        val bytes = Array.ofDim[Byte](buffer.remaining())
        buffer.get(bytes, 0, bytes.length);
        new KeyValue(bytes)
      }
    }
  }

  object empty extends KeyValue(Array[Byte]()) {
    override def append(other: KeyValue) = other    
  }
}

class Key[T](val value: KeyValue) {
  override def equals(other: Any) = other match {
    case otherKeyValue: KeyValue => value == otherKeyValue
    case otherKey: Key[_] => value == otherKey.value
    case _ => false
  }
  
  override def hashCode() = value.hashCode

  override def toString = "Key" + value.toString
}

trait Keys extends KeyPaths with IdCodecs {
  implicit def key2value(key: Key[_]) = key.value
}
object Keys extends Keys
