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
import me.prettyprint.hector.api.mutation.{ Mutator => HectorMutator }
import me.prettyprint.hector.api.factory.HFactory

trait Mutators { self: ColumnFamily =>
  import Serializers._

  type Mutator = HectorMutator[String]
  
  protected def createMutator() = HFactory.createMutator(self.keyspace, stringSerializer)

  def write[O <: AnyRef](key: String, obj: O)(implicit mapperO: Mapper[O]) = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addInsertion(key, cf.columnFamilyName, mapperO.objectToColumn(obj))
  }

  def write[O <: AnyRef, P <: AnyRef](key: String, obj: O, parent: P)(implicit mapperO: Mapper[O], mapperP: Mapper[P]) = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addInsertion(key, cf.columnFamilyName, mapperO.objectToColumn(obj))
  }

  def writeAll[O <: AnyRef](key: String, objs: Iterable[O])(implicit mapperO: Mapper[O]) = (mutator: Mutator, cf: ColumnFamily) => {
    objs foreach { obj => write(key, obj) }
  }

  def writeAll[O <: AnyRef, P <: AnyRef](key: String, objs: Iterable[O], parent: P)(implicit mapperO: Mapper[O], mapperP: Mapper[P]) = (mutator: Mutator, cf: ColumnFamily) => {
    objs foreach { obj => write(key, obj, parent) }
  }

  def delete[O <: AnyRef](key: String, obj: O)(implicit mapperO: Mapper[O]) = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addDeletion(key, cf.columnFamilyName, mapperO.name(obj), stringSerializer)
  }

  def delete[O <: AnyRef, P <: AnyRef](key: String, obj: O, parent: P)(implicit mapperO: Mapper[O], mapperP: Mapper[P]) = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addDeletion(key, cf.columnFamilyName, mapperO.name(obj, parent), stringSerializer)
  }

  def deleteAll[O <: AnyRef](key: String, objs: Iterable[O])(implicit mapperO: Mapper[O]) = (mutator: Mutator, cf: ColumnFamily) => {
    objs foreach { obj => delete(key, obj) }
  }

  def deleteAll[O <: AnyRef, P <: AnyRef](key: String, objs: Iterable[O], parent: P)(implicit mapperO: Mapper[O], mapperP: Mapper[P]) = (mutator: Mutator, cf: ColumnFamily) => {
    objs foreach { obj => delete(key, obj, parent) }
  }

  def deleteRow(key: String) = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addDeletion(key, cf.columnFamilyName)
  }

  def deleteRows(keys: String*) = (mutator: Mutator, cf: ColumnFamily) => {
    keys foreach deleteRow
  }
}