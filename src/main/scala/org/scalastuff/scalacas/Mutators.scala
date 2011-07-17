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
import org.scalastuff.scalacas.keys._
import Serializers._

trait Mutators { self: ColumnFamily =>

  type Mutator = HectorMutator[KeyValue]
  type Mutation = (Mutator, ColumnFamily) => Unit

  protected def createMutator(): Mutator = HFactory.createMutator(self.keyspace, keyValueSerializer)

  def write[A <: AnyRef](rowKey: KeyValue, obj: A)(implicit mapper: Mapper[A], columnKeyPath: KeyPath1[A]): Mutation = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addInsertion(rowKey, cf.columnFamilyName, mapper.objectToColumn(columnKeyPath(obj), obj))
  }
  
  @inline
  def write[A <: AnyRef](rowKey: KeyValue, columnKeyPath: KeyPath1[A], obj: A)(implicit mapper: Mapper[A]): Mutation = {
    write(rowKey, obj)(mapper, columnKeyPath)
  }
  
  def write[A <: AnyRef](rowKey: KeyValue, columnKey: Key[A], obj: A)(implicit mapper: Mapper[A]): Mutation = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addInsertion(rowKey, cf.columnFamilyName, mapper.objectToColumn(columnKey, obj))
  }

  def writeAll[A <: AnyRef](rowKey: KeyValue, objs: Traversable[A])(implicit mapper: Mapper[A], columnKeyPath: KeyPath1[A]): Mutation = (mutator: Mutator, cf: ColumnFamily) => {
    for (obj <- objs)
      mutator.addInsertion(rowKey, cf.columnFamilyName, mapper.objectToColumn(columnKeyPath(obj), obj))
  }
  
  @inline
  def writeAll[A <: AnyRef](rowKey: KeyValue, columnKeyPath: KeyPath1[A], objs: Traversable[A])(implicit mapper: Mapper[A]): Mutation = {
    writeAll(rowKey, objs)(mapper, columnKeyPath)
  }

  def delete[A <: AnyRef](rowKey: KeyValue, obj: A)(implicit columnKeyPath: KeyPath1[A]): Mutation = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addDeletion(rowKey, cf.columnFamilyName, columnKeyPath(obj).value, keyValueSerializer)
  }
  
  @inline
  def delete[A <: AnyRef](rowKey: KeyValue, columnKeyPath: KeyPath1[A], obj: A): Mutation = {
    delete(rowKey, obj)(columnKeyPath)
  }
  
  def delete[A <: AnyRef](rowKey: KeyValue, columnKey: Key[A]): Mutation = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addDeletion(rowKey, cf.columnFamilyName, columnKey.value, keyValueSerializer)
  }

  def deleteAll[A <: AnyRef](rowKey: KeyValue, objs: Traversable[A])(implicit columnKeyPath: KeyPath1[A]): Mutation = (mutator: Mutator, cf: ColumnFamily) => {
    for (obj <- objs)
      mutator.addDeletion(rowKey, cf.columnFamilyName, columnKeyPath(obj).value, keyValueSerializer)
  }
  
  @inline
  def deleteAll[A <: AnyRef](rowKey: KeyValue, columnKeyPath: KeyPath1[A], objs: Traversable[A]): Mutation = {
    deleteAll(rowKey, objs)(columnKeyPath)
  }
  
  def deleteAll[A <: AnyRef](rowKey: KeyValue, columnKeys: Traversable[Key[A]]): Mutation = (mutator: Mutator, cf: ColumnFamily) => {
    for (columnKey <- columnKeys)
      mutator.addDeletion(rowKey, cf.columnFamilyName, columnKey.value, keyValueSerializer)
  }

  def deleteRow(rowKey: KeyValue): Mutation = (mutator: Mutator, cf: ColumnFamily) => {
    mutator.addDeletion(rowKey, cf.columnFamilyName)
  }

  def deleteRows(keys: KeyValue*): Mutation = (mutator: Mutator, cf: ColumnFamily) => {
    for (key <- keys)
      mutator.addDeletion(key, cf.columnFamilyName)
  }
}