package org.scalastuff.scalacas

import org.junit.Test
import org.junit.Assert._

import keys._
import Keys._

class QueryResultTest {
  import TestClasses._
  import TestClassMappers._

  val qr = new QueryResult(List(
    scmA.objectToColumn(pathA(BeanA(1)), BeanA(1)),
    scmB.objectToColumn(pathB(BeanB("2")), BeanB("2")),
    scmB.objectToColumn(pathB(BeanB("3")), BeanB("3")),
    scmC.objectToColumn(pathC(BeanB("3"), BeanC("4")), BeanC("4")),
    scmC.objectToColumn(pathC(BeanB("3"), BeanC("5")), BeanC("5"))))

  @Test
  def testFilter() {
    val a = qr.filter[BeanA]
    assertEquals(1, a.size)
    assertEquals(1, a.head.id)

    val b = qr.filter[BeanB] toList;
    assertEquals(2, b.size)
    assertEquals(BeanB("2"), b(0))
    assertEquals(BeanB("3"), b(1))

    val c = qr.filter[BeanC](scmC, pathC(BeanB("3")))
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