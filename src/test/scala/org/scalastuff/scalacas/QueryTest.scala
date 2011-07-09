package org.scalastuff.scalacas

import org.junit.Test
import org.junit.Assert._
import keys._
import Keys._

class QueryTest {
  import TestClasses._
  
  val rowKey = path[Int]
  val columnKeyA = "A" :: path[BeanA]
  val columnKeyC = columnKeyA :: "C" :: path[BeanC]

  @Test
  def testObjectOfClass() {
    val qry = Query(List(rowKey(1))).havingPath(columnKeyA)
    assertEquals(Some("A "), qry.fromColumnName)
    assertEquals(Some("A ~"), qry.toColumnName)
  }

  @Test
  def testStartWithClass() {
    val qry = Query(List(rowKey(1))) from columnKeyA
    assertEquals(Some("A "), qry.fromColumnName)
  }

  @Test
  def testStartWith() {
    val qry = Query(List(rowKey(1))) from columnKeyA(BeanA(1))
    assertEquals(Some("A 1"), qry.fromColumnName)
  }

  @Test
  def testStartWithParent() {
    val qry = Query(List(rowKey(1))) from columnKeyC(BeanA(1), BeanC("1"))
    assertEquals(Some("C 1/A 1"), qry.fromColumnName)
  }

  @Test
  def testEndWithClass() {
    val qry = Query(List(rowKey(1))) to columnKeyA
    assertEquals(Some("A ~"), qry.toColumnName)
  }

  @Test
  def testEndWith() {
    val qry = Query(List(rowKey(1))) to columnKeyA(BeanA(10))
    assertEquals(Some("A 10"), qry.toColumnName)
  }

  @Test
  def testEndWithParent() {
    val qry = Query(List(rowKey(1))) to columnKeyC(BeanA(10), BeanC("1"))
    assertEquals(Some("C 1/A 10"), qry.toColumnName)
  }

  @Test
  def testLimit() {
    val qry = Query(List(rowKey(1))) limit 100
    assertEquals(100, qry.maxColumnCount)
  }
}