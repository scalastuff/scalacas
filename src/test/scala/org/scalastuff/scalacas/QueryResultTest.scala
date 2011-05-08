package org.scalastuff.scalacas

import org.junit.Test
import org.junit.Assert._

class QueryResultTest {
  import TestClasses._
  import Serializers._
  
  val qr = new QueryResult(List(
      scmA.createColumn("A 1", toBytes(1)),
      scmB.createColumn("B 2", toBytes(2)),
      scmC.createColumn("B 3", toBytes(3))))

  @Test
  def testFilter() {    
    val a = qr.filter[A]
    assertEquals(1, a.size)
    assertEquals(1, a.head.id)

    val b = qr.filter[B] toList;
    assertEquals(2, b.size)
    assertEquals(B(2), b(0))
    assertEquals(B(3), b(1))
    
    val c = qr.filter[C]
    assertEquals(0, c.size)
  }
  
  @Test
  def testFind {    
    val a = qr.find[A]
    assertEquals(Some(A(1)), a)

    val b = qr.find[B]
    assertEquals(Some(B(2)), b)
    
    val c = qr.find[C]
    assertEquals(None, c)
  }
  
  // TODO: parent
}