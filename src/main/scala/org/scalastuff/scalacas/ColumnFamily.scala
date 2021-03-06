package org.scalastuff.scalacas

import org.scale7.cassandra.pelops._
import org.apache.cassandra.thrift._
import scala.collection.JavaConversions._

class ColumnFamily(val db: Database, val columnFamilyName: String) {
  if (!db.keyspaceDef.getCf_defs.exists(_.getName == columnFamilyName)) {
    val columnFamilyManager = new ColumnFamilyManager(db.cluster, db.keyspaceName)
    val cfDef = new CfDef(db.keyspaceName, columnFamilyName)
    cfDef.setColumn_type("Super")
    cfDef.setComparator_type("UTF8Type")
    columnFamilyManager.addColumnFamily(cfDef)
  }

  def mutate(cl: ConsistencyLevel, mutators: ((Mutator, ColumnFamily) => _)*) = {
    val mutator = Pelops.createMutator("pool")
    for (m <- mutators)
      m(mutator, this)
    mutator.execute(cl)
  }

  def deleteRow(key: String, cl: ConsistencyLevel = ConsistencyLevel.QUORUM) {
    val rowDeletor = Pelops.createRowDeletor("pool")
    rowDeletor.deleteRow(columnFamilyName, key, cl)
  }

  def deleteRows(cl: ConsistencyLevel, keys: String*) {
    val rowDeletor = Pelops.createRowDeletor("pool")
    for (key <- keys)
      rowDeletor.deleteRow(columnFamilyName, key, cl)
  }

  def truncate() {
    val columnFamilyManager = new ColumnFamilyManager(db.cluster, db.keyspaceName)
    columnFamilyManager.truncateColumnFamily(columnFamilyName)
  }

  def key(k: String) = new Query(Seq(k))
  def keys(ks: String*) = new Query(ks)

  def query(qry: Query): Iterable[QueryResult] = {
    val selector = Pelops.createSelector("pool")
    qry.keys match {
      case key :: Nil => Iterable(new QueryResult(selector.getSuperColumnsFromRow(columnFamilyName, key, qry.toSlicePredicate, qry.cl)))
      case _ => selector.getSuperColumnsFromRowsUtf8Keys(columnFamilyName, qry.keys, qry.toSlicePredicate, qry.cl) values () map (new QueryResult(_))
    }
  }
}