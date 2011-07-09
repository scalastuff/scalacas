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
  def startRange = prefix.withSuffix(0)
  def endRange = prefix.withSuffix(1)
}

final class KeyPath1[H](_prefix: KeyValue, val entityIdCodec: EntityIdCodec[H]) extends KeyPath[H](_prefix) {
  import IdCodecs._
  
  type This = KeyPath1[H]
  def ::[H0](v: KeyPath1[H0]) = new CompositePath(v.prefix, v.entityIdCodec, this)
  def ::(_prefix: String) = new KeyPath1(KeyValue(_prefix).append(prefix), entityIdCodec)
  def withPrefix(_prefix: KeyValue) = new KeyPath1(_prefix.append(prefix), entityIdCodec)
  
  def isPathOf(keyValue: KeyValue) = prefix.isPrefixOf(keyValue) && entityIdCodec.canDecodeId(keyValue, prefix.length + 1)

  def apply(h: H) = new Key[H](prefix.append(entityIdCodec.encodeId(h)))
}

final class CompositePath[H, T <: KeyPath[_]](_prefix: KeyValue, head: EntityIdCodec[H], val tail: T) extends KeyPath[H](_prefix) {
  import IdCodecs._
  
  type This = CompositePath[H, T]
  def ::[H0](v: KeyPath1[H0]) = new CompositePath(v.prefix, v.entityIdCodec, this)
  def ::(_prefix: String) = new CompositePath(KeyValue(_prefix).append(prefix), head, tail)
  def withPrefix(_prefix: KeyValue) = new CompositePath(_prefix.append(prefix), head, tail)

  def apply(h: H) = tail.withPrefix(prefix.append(head.encodeId(h)))
}

trait KeyPaths {
  implicit def idCodecWithIdType[B, K](implicit id: Identify[B, K], s: IdCodec[K]) = new EntityIdCodec[B] {
    def encodeId(b: B) = KeyValue(id(b))
    def canDecodeId(b: KeyValue, prefixLength: Int) = s.canDecode(b, prefixLength)
  }
  
  implicit def idCodecSelf[I](implicit c: IdCodec[I]) = new EntityIdCodec[I] {
    def encodeId(entity: I) = KeyValue(entity)
    def canDecodeId(keyValue: KeyValue, prefixLength: Int) = c.canDecode(keyValue, prefixLength)
  }

  def path[A](implicit entityIdCodec: EntityIdCodec[A]) = new KeyPath1[A](KeyValue.empty, entityIdCodec)

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