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

trait Mutators extends Serializers { self: ColumnFamily =>

  type Mutator = HectorMutator[KeyValue]  

  protected def createMutator(): Mutator = HFactory.createMutator(self.keyspace, keyValueSerializer)

  def write[A <: AnyRef](rowKey: KeyValue, obj: A)(implicit mapper: Mapper[A], keyPath: KeyPath1[A]) = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addInsertion(rowKey, cf.columnFamilyName, mapper.objectToColumn(keyPath(obj), obj))
  }
  
  def write[A <: AnyRef](rowKey: KeyValue, columnKey: Key[A], obj: A)(implicit mapper: Mapper[A]) = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addInsertion(rowKey, cf.columnFamilyName, mapper.objectToColumn(columnKey, obj))
  }

  def writeAll[A <: AnyRef](rowKey: KeyValue, objs: Iterable[A])(implicit mapper: Mapper[A], keyPath: KeyPath1[A]) = (mutator: Mutator, cf: ColumnFamily) => {
    for (obj <- objs)
      mutator.addInsertion(rowKey, cf.columnFamilyName, mapper.objectToColumn(keyPath(obj), obj))
  }

  def delete[A <: AnyRef](rowKey: KeyValue, obj: A)(implicit keyPath: KeyPath1[A]) = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addDeletion(rowKey, cf.columnFamilyName, keyPath(obj), keyValueSerializer)
  }
  
  def delete[A <: AnyRef](rowKey: KeyValue, columnKey: Key[A]) = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addDeletion(rowKey, cf.columnFamilyName, columnKey, keyValueSerializer)
  }

  def deleteAll[A <: AnyRef](rowKey: KeyValue, objs: Iterable[A])(implicit keyPath: KeyPath1[A]) = (mutator: Mutator, cf: ColumnFamily) => {
    for (obj <- objs)
      mutator.addDeletion(rowKey, cf.columnFamilyName, keyPath(obj), keyValueSerializer)
  }
  
  def deleteAll[A <: AnyRef](rowKey: KeyValue, columnKeys: Iterable[Key[A]]) = (mutator: Mutator, cf: ColumnFamily) => {
    for (columnKey <- columnKeys)
      mutator.addDeletion(rowKey, cf.columnFamilyName, columnKey, keyValueSerializer)
  }

  def deleteRow(rowKey: KeyValue) = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addDeletion(rowKey, cf.columnFamilyName)
  }

  def deleteRows(keys: KeyValue*) = (mutator: Mutator, cf: ColumnFamily) => {
    for (key <- keys)
      mutator.addDeletion(key, cf.columnFamilyName)
  }
}