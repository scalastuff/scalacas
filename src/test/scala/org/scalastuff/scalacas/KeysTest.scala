package org.scalastuff.scalacas
//package keytest

import org.junit.Test
import org.junit.Assert._
import keys._
import Keys._

class KeysTest {
  import TestClasses._  
  
  @Test
  def testKeyPath1() {
    val keyPathA = path[BeanA]
    val aKey = keyPathA(BeanA(1))
    checkKey[BeanA](Array[Byte](0, 0, 0, 1), aKey)
  }
  
  @Test
  def testKeyPath2Simple() {
    val keyPathC = path[BeanB] :: path[BeanC]
    val key1 = keyPathC(BeanB("a"), BeanC("bc"))
    val key2 = keyPathC(BeanB("ab"), BeanC("c"))
    
    checkNotEqual(key1, key2)
  }
  
  @Test
  def testKeyPath1Delimited() {
    val keyPath1 = "a" :: path[BeanB]
    val keyPath2 = path[BeanB]
    
    checkNotEqual(keyPath1.prefix, keyPath2.prefix)
    
    val key1 = keyPath1(BeanB("bc"))
    val key2 = keyPath2(BeanB("bc"))

    checkNotEqual(key1, key2)
  }
  
  @Test
  def testKeyPath2Delimited() {
    val keyPathA = "a" :: path[BeanA]
    
    val keyPathB1 = keyPathA :: "b" :: "c" :: path[BeanB]
    val keyPathB2 = path[BeanA] :: "b" :: "c" :: path[BeanB]
    
    checkNotEqual(keyPathB1.prefix, keyPathB2.prefix)
    
    val key1 = keyPathB1(BeanA(1), BeanB("2"))
    val key2 = keyPathB2(BeanA(1), BeanB("2"))
    
    checkNotEqual(key1, key2)
  }
  
  @Test
  def testKeyPath3Delimited() {
    val keyPathC = path[BeanA] :: "b" :: path[BeanB] :: "c" :: path[BeanC]
    val cKey = keyPathC(BeanA(1), BeanB("2"), BeanC("3"))
    
    checkKey[BeanC](Array[Byte](0, 0, 0, 1, 0, 'b', 0, '2', 0, 'c', 0, '3'), cKey)
  }
  
  def checkKey[A](bytes: Array[Byte], key: Key[A]) {
    checkKeyValue(bytes, key)
  }
  
  def checkKeyValue(bytes: Array[Byte], key: KeyValue) {
    val keyBytes = KeyValue.Serializer.toBytes(key)
    assertArrayEquals(bytes, keyBytes)
  }
  
  def checkNotEqual(key1: KeyValue, key2: KeyValue) {
    if (key1 == key2) {
      fail("Both keys are the same: " + key1 + " and " + key2)
    }
  }
}