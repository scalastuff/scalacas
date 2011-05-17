package org.scalastuff.scalacas

object Keys extends Application {

  trait Key[T] {
    def value: Seq[Any]
  }

  ///////////////////////////////
  // Key Extractors
  ///////////////////////////////

  trait KeyExtractor[A] {
    def apply(a: A): Key[A]
    def apply(prefix: Seq[Any], a: A): Key[A]
  }

  object KeyExtractor {
    def apply[A <: HasId]() = new KeyExtractor[A] {
      def apply(a: A) = new Key[A] { def value = Seq[Any](a.id) }
      def apply(prefix: Seq[Any], a: A) = new Key[A] { def value = prefix :+ a.id }
    }
  }

  implicit def keyExtractor2KeyPath[H](ke: KeyExtractor[H]) = new KeyPath1("", ke)
  implicit def hasId2KeyExtractor[A <: HasId] = KeyExtractor[A]()
  def path[A](implicit keyExtractor: KeyExtractor[A]) = keyExtractor

  //////////////////////////////////////
  // Key Paths - describe the key path
  //////////////////////////////////////

  sealed abstract class KeyPath[H](val prefix: Seq[Any], val head: KeyExtractor[H]) {
    type This <: KeyPath[H]
    protected[scalacas] def prepend(prefix: Seq[Any]): This
  }

  final class KeyPath1[H](_prefix: Seq[Any], _head: KeyExtractor[H]) extends KeyPath[H](_prefix, _head) {
    type This = KeyPath1[H]
    def ::[H0](v: KeyExtractor[H0]) = new CompositePath(Seq.empty, v, this)
    def ::(_prefix: String) = new KeyPath1(_prefix +: prefix, head)
    protected[scalacas] def prepend(_prefix: Seq[Any]) = new KeyPath1(_prefix ++ prefix, head)

    def apply(h: H) = head(prefix, h)
  }

  final class CompositePath[H, T <: KeyPath[_]](_prefix: Seq[Any], _head: KeyExtractor[H], val tail: T) extends KeyPath(_prefix, _head) {
    type This = CompositePath[H, T]
    def ::[H0](v: KeyExtractor[H0]) = new CompositePath(Seq.empty, v, this)
    def ::(_prefix: String) = new CompositePath(_prefix +: prefix, head, tail)
    protected[scalacas] def prepend(_prefix: Seq[Any]) = new CompositePath(_prefix ++ prefix, head, tail)

    def apply(h: H) = tail.prepend(head(prefix, h).value)
  }

  ///////////////////////////////
  // Shortcuts for up to 5 beans
  ///////////////////////////////

  type KeyPath2[A, B] = CompositePath[A, KeyPath1[B]]
  type KeyPath3[A, B, C] = CompositePath[A, KeyPath2[B, C]]
  type KeyPath4[A, B, C, D] = CompositePath[A, KeyPath3[B, C, D]]
  type KeyPath5[A, B, C, D, E] = CompositePath[A, KeyPath4[B, C, D, E]]

  implicit def applyKeyPath2[A, B](path: KeyPath2[A, B]) = { (a: A, b: B) => path(a)(b) }
  implicit def applyKeyPath3[A, B, C](path: KeyPath3[A, B, C]) = { (a: A, b: B, c: C) => path(a)(b)(c) }
  implicit def applyKeyPath4[A, B, C, D](path: KeyPath4[A, B, C, D]) = { (a: A, b: B, c: C, d: D) => path(a)(b)(c)(d) }
  implicit def applyKeyPath5[A, B, C, D, E](path: KeyPath5[A, B, C, D, E]) = { (a: A, b: B, c: C, d: D, e: E) => path(a)(b)(c)(d)(e) }

  ///////////////////////////////
  // Tests
  ///////////////////////////////

  case class BeanA(id: String) extends HasId
  case class BeanB(id: String) extends HasId
  case class BeanC(id: String) extends HasId

  val keyPathA = path[BeanA]
  val aKey = keyPathA(BeanA("1"))
  printKey[BeanA](aKey)

  val keyPathB = path[BeanA] :: "b" :: path[BeanB]
  val bKey = keyPathB(BeanA("1"), BeanB("2"))
  printKey[BeanB](bKey)

  val keyPathC = path[BeanA] :: "b" :: path[BeanB] :: "c" :: path[BeanC]
  val cKey = keyPathC(BeanA("1"), BeanB("2"), BeanC("3"))
  printKey[BeanC](cKey)

  def printKey[X](key: Key[X]) = println(key.value)
}