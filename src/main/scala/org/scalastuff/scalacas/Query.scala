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

import org.scalastuff.scalacas.keys._

case class Query(keys: Seq[KeyValue],
  fromColumnName: KeyValue = KeyValue.empty,
  toColumnName: KeyValue = KeyValue.empty,
  maxColumnCount: Int = Integer.MAX_VALUE,
  rev: Boolean = false) {
  
  def from(key: Key[_]) = this.copy(fromColumnName = key.value)
  def from(keyPath: KeyPath[_]) = this.copy(fromColumnName = keyPath.startRange)
  
  def to(key: Key[_]) = this.copy(toColumnName = key.value)
  def to(keyPath: KeyPath[_]) = this.copy(toColumnName = keyPath.endRange)
  
  def having(keyPath: KeyPath[_]) =
    this.copy(fromColumnName = keyPath.startRange,
      toColumnName = keyPath.endRange) 
      
  def limit(maxColumnCount: Int) = this.copy(maxColumnCount = maxColumnCount)
  def reversed() = this.copy(rev = true)
}