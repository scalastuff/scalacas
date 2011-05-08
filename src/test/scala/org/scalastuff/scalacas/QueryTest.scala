package org.scalastuff.scalacas

import org.junit.Test
import org.junit.Assert._

class QueryTest {
  import TestClasses._

  @Test
  def testObjectOfClass() {
    val qry = new Query(List("1")).objectsOfClass[A]
    assertEquals(Some("A "), qry.fromColumnName)
    assertEquals(Some("A ~"), qry.toColumnName)
  }

  @Test
  def testStartWithClass() {
    val qry = new Query(List("1")).startWithClass[A]
    assertEquals(Some("A "), qry.fromColumnName)
  }

  @Test
  def testStartWith() {
    val qry = new Query(List("1")).startWith(A(1))
    assertEquals(Some("A 1"), qry.fromColumnName)
  }

  @Test
  def testStartWithParent() {
    val qry = new Query(List("1")).startWith(A(1), C(1))
    assertEquals(Some("C 1/A 1"), qry.fromColumnName)
  }

  @Test
  def testEndWithClass() {
    val qry = new Query(List("1")).endWithClass[A]
    assertEquals(Some("A ~"), qry.toColumnName)
  }

  @Test
  def testEndWith() {
    val qry = new Query(List("1")).endWith(A(10))
    assertEquals(Some("A 10"), qry.toColumnName)
  }

  @Test
  def testEndWithParent() {
    val qry = new Query(List("1")).endWith(A(10), C(1))
    assertEquals(Some("C 1/A 10"), qry.toColumnName)
  }

  @Test
  def testLimit() {
    val qry = new Query(List("1")) limit 100
    assertEquals(100, qry.maxColumnCount)
  }
}