package org.scalastuff.scalacas

import scala.collection.JavaConversions._
import keys._
import Keys._
import Serializers._

object TestClasses {
  
  case class BeanA(id: Int)
  case class BeanB(id: String)
  case class BeanC(id: String)
  
  implicit val IdentifyBeanA = Identify[BeanA, Int](_.id)
  implicit val IdentifyBeanB = Identify[BeanB, String](_.id)
  implicit val IdentifyBeanC = Identify[BeanC, String](_.id)
}

object TestClassMappers {
  import TestClasses._
  
  implicit val pathA = "A" :: path[BeanA]
  implicit val scmA = new Mapper[BeanA] {
	  def objectToBytes(obj:BeanA) = toBytes(obj.id)
	  def columnToObject(column: Column) = BeanA(fromBytes[Int](column.getValue))
  }
  
  implicit val pathB = "B" :: path[BeanB]
  implicit val scmB = new Mapper[BeanB] {
	  def objectToBytes(obj:BeanB) = toBytes(obj.id)
	  def columnToObject(column: Column) = BeanB(fromBytes[String](column.getValue))
  }
  
  implicit val pathC = pathB :: "C" :: path[BeanC]
  implicit val scmC = new Mapper[BeanC] {
	  def objectToBytes(obj:BeanC) = toBytes(obj.id)
	  def columnToObject(column: Column) = BeanC(fromBytes[String](column.getValue))
  }
}