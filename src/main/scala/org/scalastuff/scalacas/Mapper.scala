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

import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.factory.HFactory

/**
 * Maps objects to Column.
 *
 * Subclass to provide your own mapping to your domain objects of given class.
 * Must be thread-safe.
 *
 * @author Alexander Dvorkovyy
 */
abstract class Mapper[A <: AnyRef] extends Serializers with Keys {
  type Column = HColumn[KeyValue, Array[Byte]]

  def objectToColumn(columnKey: Key[A], obj: A) = 
    HFactory.createColumn(columnKey, objectToBytes(obj), keyValueSerializer, bytesSerializer)
    
  def columnToObject(column: Column): A  
  def objectToBytes(obj: A): Array[Byte]  
}