package org.scalastuff.scalacas

class Key[-T](val value: String)

trait KeyPath[C] {
  def apply(c: C): Key[C]
}

trait AncestorKeyPath[A, C] {
  def apply(a: A): KeyPath[C]
  def label(l: String) = new AncestorKeyPath[A, C] {
    def apply(a: A) = new LabelKeyPath(AncestorKeyPath.this.apply(a), l)
    def keyOf[CC <: HasId] = {
      val self = this
      new Ancestor2KeyPath[A, C, CC] {
        def apply(a: A) = {
		    new AncestorKeyPath[C, CC] {
		      def apply(c: C) = new KeyPath[CC] {
		        def apply(cc: CC) = new Key[CC](self(a)(c).value + cc.id)
		      }
		    }
        }
      }
    }
  }
}

trait Ancestor2KeyPath[A, C, CC] {
	def apply(a: A): AncestorKeyPath[C, CC]
}

class LabelKeyPath[C](parent: KeyPath[C], label: String) extends KeyPath[C] {
  def apply(c: C) = new Key[Any](parent.apply(c).value + " " + label)
  def keyOf[CC <: HasId] = {
    val self = this
    new AncestorKeyPath[C, CC] {
      def apply(c: C) = new KeyPath[CC] {
        def apply(cc: CC) = new Key[CC](self.apply(c).value + cc.id)
      }
    }
  }
}

trait KeyPathBuilder {
  def prefix: String
  def keyOf[T <: HasId] = new KeyPath[T] {
    def apply(c: T) = new Key[T](prefix + c.id)
    def label(l: String) = new LabelKeyPath(this, l)
  }
}

object EmptyPath {
  def label(l: String) = new Key[Any](l) with KeyPathBuilder {
    def prefix = l
  }
}

object TestKeys extends Application {
  import EmptyPath._

  case class BeanA(id: String) extends HasId
  case class BeanB(id: String) extends HasId
  case class BeanC(id: String) extends HasId

  printKey[BeanA](label("a"))
  printKey[BeanB](label("b"))
  val keyPath1 = label("A").keyOf[BeanA]
  printKey[BeanA](keyPath1(BeanA("1")))
  // printKey[BeanB](keyPath1(BeanA("1"))) // <- doesn't compile, check it!

  val keyPath2 = label("A").keyOf[BeanA].label("B")
  printKey[BeanA](keyPath2(BeanA("2")))
  printKey[BeanB](keyPath2(BeanA("3")))

  val keyPath3 = label("A").keyOf[BeanA].label("B").keyOf[BeanB]
  printKey[BeanB](keyPath3(BeanA("4"))(BeanB("5")))

  val keyPath4 = label("A").keyOf[BeanA].label("B").keyOf[BeanB].label("C")
  printKey[BeanB](keyPath4(BeanA("6"))(BeanB("7")))

  val keyPath5 = label("A").keyOf[BeanA].label("B").keyOf[BeanB].label("C").keyOf[BeanC]
  printKey[BeanC](keyPath5(BeanA("8"))(BeanB("9"))(BeanC("0")))

  def printKey[X](key: Key[X]) = println(key.value)
}