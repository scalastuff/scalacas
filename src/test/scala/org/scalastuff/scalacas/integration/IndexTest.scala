package org.scalastuff.scalacas.integration

import org.junit.Assert._
import org.junit.{ Test, Ignore }

class IndexTest {

  @Test
  def testSimple() {
    TestIndex.truncate()
    
    assertEquals(0, TestIndex.findByKey("A").size)
    
    TestIndex.saveRef("A", "1")
    assertEquals(1, TestIndex.findByKey("A").size)
    
    TestIndex.truncate()
  }

}