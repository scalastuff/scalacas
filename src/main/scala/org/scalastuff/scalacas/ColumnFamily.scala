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
import me.prettyprint.cassandra.service.ThriftCfDef
import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.mutation.Mutator

class ColumnFamily(val db: Database, val columnFamilyName: String) extends Mutators {
  if (!db.keyspaceDef.getCfDefs.exists(_.getName == columnFamilyName)) {
    val cfDef = new ThriftCfDef(db.keyspaceName, columnFamilyName)
    db.cluster.addColumnFamily(cfDef)
  }
  
  val consistencyLevelPolicy = new QuorumAllConsistencyLevelPolicy()
  
  lazy val keyspace = HFactory.createKeyspace(db.keyspaceName, db.cluster, consistencyLevelPolicy)

  def mutate(mutators: ((Mutators#Mutator, ColumnFamily) => _)*) = {
    val mutator = createMutator()
    for (m <- mutators)
      m(mutator, this)
    mutator.execute()
  }  

  def truncate() {
    db.cluster.truncate(db.keyspaceName, columnFamilyName)
  }

  def key(k: String) = new Query(Seq(k))
  def keys(ks: String*) = new Query(ks)

  def query(qry: Query): Iterable[QueryResult] = qry.execute(keyspace, columnFamilyName)
}