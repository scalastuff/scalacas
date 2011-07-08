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

class QueryResult(row: Iterable[HColumn[Array[Byte], Array[Byte]]]) {
  def filter[O <: AnyRef](implicit mapper: Mapper[O], keyPath: KeyPath1[O]): Iterable[O] = {
    for (col <- row if col.getName startsWith keyPath.prefix.bytes)
      yield mapper.columnToObject(col)
  }
  
  def find[O <: AnyRef](implicit mapper: Mapper[O], keyPath: KeyPath1[O]): Option[O] = {
    for (col <- row find { col => col.getName startsWith keyPath.prefix.bytes } )
      yield mapper.columnToObject(col)
  }
}