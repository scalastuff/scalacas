package org.scalastuff.scalacas

import org.junit.Test
import org.junit.Assert._

class QueryResultTest {
  import TestClasses._
  import Serializers._
  import Keys._

  val qr = new QueryResult(List(
    scmA.objectToColumn(pathA(A(1)), A(1)),
    scmB.objectToColumn(pathB(B(2)), B(2)),
    scmB.objectToColumn(pathB(B(3)), B(3)),
    scmC.objectToColumn(pathC(B(3), C(4)), C(4)),
    scmC.objectToColumn(pathC(B(3), C(5)), C(5))))

  @Test
  def testFilter() {
    val a = qr.filter[A]
    assertEquals(1, a.size)
    assertEquals(1, a.head.id)

    val b = qr.filter[B] toList;
    assertEquals(2, b.size)
    assertEquals(B(2), b(0))
    assertEquals(B(3), b(1))

    val c = qr.filter[C](scmC, pathC(B(3)))
    assertEquals(2, c.size)
  }

  @Test
  def testFind {
    //    val a = qr.find[A]
    //    assertEquals(Some(A(1)), a)
    //
    //    val b = qr.find[B]
    //    assertEquals(Some(B(2)), b)
    //    
    //    val c = qr.find[C]
    //    assertEquals(None, c)
  }

  // TODO: parent
}