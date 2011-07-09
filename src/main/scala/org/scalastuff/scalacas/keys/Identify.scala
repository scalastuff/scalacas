package org.scalastuff.scalacas.keys

/**
 * provides id of given entity
 */
trait Identify[E, I] extends (E => I)

object Identify {
  def apply[E, I](identify: E => I) = new Identify[E, I] {
    def apply(entity: E): I = identify(entity)
  }
}