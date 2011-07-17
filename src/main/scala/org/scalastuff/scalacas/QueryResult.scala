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

import scala.collection.JavaConversions._
import me.prettyprint.hector.api.beans.HColumn
import org.scalastuff.scalacas.keys._

class QueryResult(row: Iterable[HColumn[KeyValue, Array[Byte]]]) {
  def filter[A <: AnyRef](implicit mapper: Mapper[A], keyPath: KeyPath1[A]): Iterable[A] = {
    for (col <- row if keyPath.isPathOf(col.getName) )
      yield mapper.columnToObject(col)
  }
  
  def filter[A <: AnyRef](keyPath: KeyPath1[A])(implicit mapper: Mapper[A]): Iterable[A] = filter(mapper, keyPath)
  
  def find[A <: AnyRef](implicit mapper: Mapper[A], keyPath: KeyPath1[A]): Option[A] = {
    for (col <- row find { col => keyPath.isPathOf(col.getName) } )
      yield mapper.columnToObject(col)
  }
  
  def find[A <: AnyRef](keyPath: KeyPath1[A])(implicit mapper: Mapper[A]): Option[A] = find(mapper, keyPath)
}