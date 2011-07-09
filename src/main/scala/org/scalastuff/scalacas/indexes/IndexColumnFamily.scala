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
package indexes
import org.scalastuff.scalacas.keys._

class IndexColumnFamily(_db: Database, _columnFamilyName: String) extends ColumnFamily(_db, _columnFamilyName) {
  implicit val mapper = StringMapper
  val rowKey = path[String]
  implicit val columnPath: KeyPath1[String] = path[String]

  def findByKey(keyValue: String): Set[String] = {
    for {
      result <- select(rowKey(keyValue)).execute()
      ref <- result.filter[String]
    } yield ref
  } toSet
  
  def saveRef(keyValue: String, ref: String) {
    mutate(write(rowKey(keyValue), ref))
  }
  
  def deleteRef(keyValue: String, ref: String) {
    mutate(delete(rowKey(keyValue), ref))
  }
  
  def deleteKey(keyValue: String) {
    mutate(deleteRow(rowKey(keyValue)))
  }
}