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

/**
 * Implement this trait in your domain object if you want SuperColumn name be generated automatically.
 * Do not forget to mixin {@link HasIdSupport} into your {@link Mapper}.
 * 
 * @author Alexander Dvorkovyy
 *
 */
trait HasId {
	def id : String
}

/**
 * Mixin trait to provide automatic SuperColumn name generation for objects implementing {@link HasId} trait.
 * 
 * @author Alexander Dvorkovyy
 *
 */
trait HasIdSupport[A <: HasId] extends Mapper[A] {
	def id(obj:A) = obj.id
}