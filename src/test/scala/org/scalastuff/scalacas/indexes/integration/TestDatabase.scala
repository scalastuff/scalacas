package org.scalastuff.scalacas
package indexes.integration

import me.prettyprint.hector.api.factory.HFactory

object TestDatabase {
  val cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160")

  val keyspace = new Database(cluster, "Test")
}