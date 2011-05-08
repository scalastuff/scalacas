package org.scalastuff.scalacas

import scala.collection.JavaConversions._

object TestClasses {
  import Serializers._
  
  case class A(id: Int)
  case class B(id: Int)
  case class C(id: Int)

  implicit val scmA = new Mapper[A]("A") {
	  def objectToColumn(obj:A) = throw new UnsupportedOperationException
	  def columnToObject(column: Column) = A(fromBytes[Int](column.getValue))
	  def id(obj:A) = obj.id.toString
  }
  
  implicit val scmB = new Mapper[B]("B") {
	  def objectToColumn(obj:B) = throw new UnsupportedOperationException
	  def columnToObject(column: Column) = B(fromBytes[Int](column.getValue))
	  def id(obj:B) = obj.id.toString
  }
  
  implicit val scmC = new Mapper[C]("C") {
	  def objectToColumn(obj:C) = throw new UnsupportedOperationException
	  def columnToObject(column: Column) = C(fromBytes[Int](column.getValue))
	  def id(obj:C) = obj.id.toString
  }
}