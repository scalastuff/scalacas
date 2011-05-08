package org.scalastuff.scalacas

import org.junit.Test
import org.junit.Assert._

class MapperTest {

  import TestClasses._

  @Test
  def testName  {
	assertEquals("A 1", scmA.name(A(1)))
	assertEquals("B 2", scmB.name(B(2)))
	assertEquals("C 4/B 3", scmB.name(B(3), C(4)))
  }
  
  @Test
  def testFullPrefix {
	assertEquals("A ", scmA.fullPrefix)
	assertEquals("C 4/B ", scmB.fullPrefix(C(4)))
  }

}