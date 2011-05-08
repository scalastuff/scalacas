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

import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.Keyspace
import me.prettyprint.cassandra.serializers.{ BytesArraySerializer, StringSerializer }
import scala.collection.JavaConversions._

class Query(val keys: Seq[String]) {
  var fromColumnName: Option[String] = None
  var toColumnName: Option[String] = None
  var rev = false
  var maxColumnCount = Integer.MAX_VALUE

  def startWith[O <: AnyRef](obj: O)(implicit mapperO: Mapper[O]): Query = {
    fromColumnName = Some(mapperO.name(obj))
    this
  }

  def startWith[O <: AnyRef, P <: AnyRef](obj: O, parent: P)(implicit mapperO: Mapper[O], mapperP: Mapper[P]): Query = {
    fromColumnName = Some(mapperO.name(obj, parent))
    this
  }

  def startWithClass[O <: AnyRef]()(implicit mapperO: Mapper[O]): Query = {
    fromColumnName = Some(mapperO.fullPrefix)
    this
  }

  def endWith[O <: AnyRef](obj: O)(implicit mapperO: Mapper[O]): Query = {
    toColumnName = Some(mapperO.name(obj))
    this
  }

  def endWith[O <: AnyRef, P <: AnyRef](obj: O, parent: P)(implicit mapperO: Mapper[O], mapperP: Mapper[P]): Query = {
    toColumnName = Some(mapperO.name(obj, parent))
    this
  }

  def endWithClass[O <: AnyRef]()(implicit mapperO: Mapper[O]): Query = {
    toColumnName = Some(mapperO.fullPrefix + "~")
    this
  }

  def objectsOfClass[O <: AnyRef]()(implicit mapperO: Mapper[O]): Query = {
    fromColumnName = Some(mapperO.fullPrefix)
    toColumnName = Some(mapperO.fullPrefix + "~")
    this
  }

  def reversed() = {
    rev = true
    this
  }

  def limit(maxColumnCount: Int) = {
    this.maxColumnCount = maxColumnCount
    this
  }

  protected[scalacas] def execute(keyspace: Keyspace, columnFamilyName: String) = {
    import Serializers._

    keys match {
      case key :: Nil =>
        val sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, bytesSerializer)
        sliceQuery.setColumnFamily(columnFamilyName)
        sliceQuery.setKey(key)
        sliceQuery.setRange(
          fromColumnName getOrElse "",
          toColumnName getOrElse "",
          rev,
          maxColumnCount)
        val columns = sliceQuery.execute().get().getColumns()
        Iterable(new QueryResult(columns))
        
      case _ =>
        val multigetSliceQuery = HFactory.createMultigetSliceQuery(keyspace, stringSerializer, stringSerializer, bytesSerializer)
        multigetSliceQuery.setColumnFamily(columnFamilyName)
        multigetSliceQuery.setKeys(keys)
        multigetSliceQuery.setRange(
          fromColumnName getOrElse "",
          toColumnName getOrElse "",
          rev,
          maxColumnCount)
        val rows = multigetSliceQuery.execute().get()
        rows map { row =>
          new QueryResult(row.getColumnSlice().getColumns())
        }
    }
  }
}