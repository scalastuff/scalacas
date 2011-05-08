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

class QueryResult(row: Iterable[HColumn[String, Array[Byte]]]) {
  def filter[O <: AnyRef](implicit mapper: Mapper[O]): Iterable[O] = {
    filterPrefix(mapper.fullPrefix, mapper)
  }
  
  def filter[O <: AnyRef, P <: AnyRef](parent:P)(implicit mapper: Mapper[O], mapperP:Mapper[P]): Iterable[O] = {
    filterPrefix(mapper.fullPrefix(parent), mapper)
  }
  
  def find[O <: AnyRef](implicit mapper: Mapper[O]): Option[O] = {
    findPrefix(mapper.fullPrefix, mapper)
  }
  
  def find[O <: AnyRef, P <: AnyRef](parent:P)(implicit mapper: Mapper[O], mapperP:Mapper[P]): Option[O] = {
    findPrefix(mapper.fullPrefix(parent), mapper)
  }
  
  private def filterPrefix[O <: AnyRef](fullPrefix:String, mapper: Mapper[O]) = {
	for (col <- row if col.getName startsWith fullPrefix)
      yield mapper.columnToObject(col)  
  }
  
  private def findPrefix[O <: AnyRef](fullPrefix:String, mapper: Mapper[O]) = {
	for (col <- row find { col => col.getName startsWith fullPrefix } )
      yield mapper.columnToObject(col)
  }
}