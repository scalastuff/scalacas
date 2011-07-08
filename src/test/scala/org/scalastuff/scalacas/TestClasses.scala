package org.scalastuff.scalacas

import scala.collection.JavaConversions._

object TestClasses {
  import Keys._
  import Serializers._
  
  case class A(id: Int) extends HasIntKey
  case class B(id: Int) extends HasIntKey
  case class C(id: Int) extends HasIntKey

  implicit val pathA = "A" :: path[A]
  implicit val scmA = new Mapper[A] {
	  def objectToBytes(obj:A) = toBytes(obj.id)
	  def columnToObject(column: Column) = A(fromBytes[Int](column.getValue))
  }
  
  implicit val pathB = "B" :: path[B]
  implicit val scmB = new Mapper[B] {
	  def objectToBytes(obj:B) = toBytes(obj.id)
	  def columnToObject(column: Column) = B(fromBytes[Int](column.getValue))
  }
  
  implicit val pathC = pathB :: "C" :: path[C]
  implicit val scmC = new Mapper[C] {
	  def objectToBytes(obj:C) = toBytes(obj.id)
	  def columnToObject(column: Column) = C(fromBytes[Int](column.getValue))
  }
}