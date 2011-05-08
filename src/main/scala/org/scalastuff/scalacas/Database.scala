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
import me.prettyprint.hector.api.Cluster
import me.prettyprint.hector.api.ddl.{ KeyspaceDefinition, ColumnFamilyDefinition }
import me.prettyprint.hector.api.factory.HFactory

class Database(val cluster: Cluster, val keyspaceName: String) {
  def keyspaceDef = cluster.describeKeyspace(keyspaceName) match {
    case null =>
      val ksDef = HFactory.createKeyspaceDefinition(keyspaceName, "org.apache.cassandra.locator.SimpleStrategy", 1, List[ColumnFamilyDefinition]());
      cluster.addKeyspace(ksDef)
      ksDef
    case ksDef =>
      ksDef
  }
}