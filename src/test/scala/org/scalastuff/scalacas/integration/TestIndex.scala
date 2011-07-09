package org.scalastuff.scalacas.indexes
package integration

object TestIndex extends IndexColumnFamily(TestDatabase.keyspace, "TestIndex") 