package org.scalastuff.scalacas

import Serializers._

case class Query(keys: Seq[KeyValue],
  fromColumnName: KeyValue = KeyValue.empty,
  toColumnName: KeyValue = KeyValue.empty,
  maxColumnCount: Int = Integer.MAX_VALUE,
  rev: Boolean = false) {
  
  def from(keyValue: KeyValue) = this.copy(fromColumnName = keyValue)
  def from(keyPath: KeyPath[_]) = this.copy(fromColumnName = keyPath.prefix)
  
  def to(keyValue: KeyValue) = this.copy(toColumnName = keyValue)
  def to(keyPath: KeyPath[_]) = this.copy(toColumnName = keyPath.prefix) // FIXME: replace last byte with 255
  
  def havingPath(keyPath: KeyPath[_]) =
    this.copy(fromColumnName = keyPath.prefix,
      toColumnName = keyPath.prefix.append(KeyValue(255.asInstanceOf[Byte]))) // FIXME: replace last byte with 255
      
  def limit(maxColumnCount: Int) = this.copy(maxColumnCount = maxColumnCount)
  def reversed() = this.copy(rev = true)
}