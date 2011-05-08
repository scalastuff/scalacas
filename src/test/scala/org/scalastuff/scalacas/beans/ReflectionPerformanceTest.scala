package org.scalastuff.scalacas
package beans

import org.junit.Test
import org.junit.Assert._

class ReflectionTest extends HasId {
  var i: Int = 1
  var i2: Integer = 2
  var s: String = "whatever"
  var l: Long = 3
  var bd: BigDecimal = 2.33
  def id = i.toString
}

class ReflectionPerformanceTest {

  @Test
  def testMappingPerformance() {

    val protoMapper = new ProtobufMapper[ReflectionTest]("")

    val rt = new ReflectionTest
    rt.i = 10
    rt.i2 = 20
    rt.s = "xxx aaa7"
    rt.l = 30
    rt.bd = 45.78

    val pscl = protoMapper.objectToColumn(rt)
    val obj = protoMapper.columnToObject(pscl)

    assertEquals(rt.i, obj.i)
    assertEquals(rt.i2, obj.i2)
    assertEquals(rt.s, obj.s)
    assertEquals(rt.l, obj.l)
    assertEquals(rt.bd, obj.bd)

    for (i <- 1 to 5) {
      testMapperPerformance("proto mapper", protoMapper, pscl)
    }
  }

  private def testMapperPerformance[A <: AnyRef](mapperName: String, mapper: Mapper[A], cl: ProtobufMapper[ReflectionTest]#Column) {
    System.gc()
    val start = System.nanoTime
    for (i <- 1 to 1000000) {
      //    val scl = mapper.toSubColumnsList(mutator, rt)
      val obj = mapper.columnToObject(cl)
    }

    val end = System.nanoTime
    println("Mapping columns to object with %s 1000000 times: %d ms".format(mapperName, (end - start) / 1000000))
  }
}