package org.scalastuff.scalacas

object Keys extends Application {

  ///////////////////////////////
  // Key Extractors
  ///////////////////////////////
	
  trait KeyExtractor[A] {
    def apply(a: A): Key[A]
  }

  object KeyExtractor {
    def apply[A <: HasId]() = new KeyExtractor[A] {
      def apply(a: A) = new Key[A](a.id)
    }
  }

  implicit def keyExtractor2KeyPath[H](ke: KeyExtractor[H]) = new OneKeyPath(ke)
  implicit def hasId2KeyExtractor[A <: HasId] = KeyExtractor[A]()
  def keyOf[A](implicit keyExtractor: KeyExtractor[A]) = keyExtractor

  //////////////////////////////////////
  // Key Paths - describe the key path
  //////////////////////////////////////

  sealed class KeyPath[H](val head: KeyExtractor[H])

  final class OneKeyPath[H](_head: KeyExtractor[H]) extends KeyPath[H](_head) {
    def ::[H0](v: KeyExtractor[H0]) = new CompositePath[H0, H, OneKeyPath[H]](v, this)

    def applyKeyOf(h: H) = new Key[H](head(h).value)
    val <# = applyKeyOf _
  }

  final class CompositePath[H, H2, T <: KeyPath[H2]](_head: KeyExtractor[H], val tail: T) extends KeyPath(_head) {
    def ::[H0](v: KeyExtractor[H0]) = new CompositePath[H0, H, CompositePath[H, H2, T]](v, this)

    def applyKeyOf[B, PB <: KeyPath[B], K <: KeyPart[B]](h: H)(implicit builder: BuildKey[H, B, CompositePath[H, H2, T], PB, K]): K =
      new CompositeKey[H, CompositePath[H, H2, T]]("", this).applyKeyOf(h)

    def <#[B, PB <: KeyPath[B], K <: KeyPart[B]](h: H)(implicit builder: BuildKey[H, B, CompositePath[H, H2, T], PB, K]): K = applyKeyOf(h)
  }

  ///////////////////////////////
  // Keys themselves
  ///////////////////////////////

  trait KeyPart[T]

  class Key[T](val value: String) extends KeyPart[T]

  class CompositeKey[A, P <: KeyPath[A]](prefix: String, keyPath: P) extends KeyPart[A] {
    def applyKeyOf[B, PB <: KeyPath[B], K <: KeyPart[B]](a: A)(implicit builder: BuildKey[A, B, P, PB, K]): K = {
      builder(prefix, a, keyPath)
    }

    def <#[B, PB <: KeyPath[B], K <: KeyPart[B]](a: A)(implicit builder: BuildKey[A, B, P, PB, K]): K = applyKeyOf(a)
  }

  ///////////////////////////////
  // Key Builders (KeyPath => Key)
  ///////////////////////////////

  trait BuildKey[A, B, PA <: KeyPath[A], PB <: KeyPath[B], K <: KeyPart[B]] {
    def apply(prefix: String, a: A, keyPath: PA): K
  }

  implicit def oneKey[A] =
    new BuildKey[A, A, OneKeyPath[A], OneKeyPath[A], Key[A]] {
      def apply(prefix: String, a: A, keyPath: OneKeyPath[A]) = new Key(prefix + keyPath.head(a).value)
    }

  implicit def compositeKey[A, B, P <: KeyPath[B]] =
    new BuildKey[A, B, CompositePath[A, B, P], P, CompositeKey[B, P]] {
      def apply(prefix: String, a: A, keyPath: CompositePath[A, B, P]) = new CompositeKey[B, P](prefix + keyPath.head(a).value, keyPath.tail)
    }

  ///////////////////////////////
  // Tests
  ///////////////////////////////


  case class BeanA(id: String) extends HasId
  case class BeanB(id: String) extends HasId
  case class BeanC(id: String) extends HasId

  val keyPathA = keyOf[BeanA]
  val aKey = keyPathA <# BeanA("1")
  printKey[BeanA](aKey)

  val keyPathB = keyOf[BeanA] :: keyOf[BeanB]
  val bKey = keyPathB <# BeanA("1") <# BeanB("2")
  printKey[BeanB](bKey)

  val keyPathC = keyOf[BeanA] :: keyOf[BeanB] :: keyOf[BeanC]
  val cKey = keyPathC <# BeanA("1") <# BeanB("2") <# BeanC("3")
  printKey[BeanC](cKey)

  def printKey[X](key: Key[X]) = println(key.value)
}