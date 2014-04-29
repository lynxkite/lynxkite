package com.lynxanalytics.biggraph.graph_api.attributes

import org.scalatest.FunSuite

class AttributesTest extends FunSuite {
  class Parent
  class Child extends Parent

  val SignatureExtension(testSig, testCloner) = AttributeSignature
    .empty
    .addAttribute[String]("string_attr")
    .addAttribute[Int]("int_attr")
    .addAttribute[Parent]("parent_attr")
    .addAttribute[Child]("child_attr")

  val SignatureExtension(extendedSig, extensionCloner) = testSig
    .addAttribute[String]("string_attr2")

  test("Setting and reading an attribute should work") {
    val writeIdx = testSig.getWriteIndexFor[String]("string_attr")
    val readIdx = testSig.getReadIndexFor[String]("string_attr")
    val da = DenseAttributes(testSig)
    da.set(writeIdx, "Hello")
    assert(da(readIdx) == "Hello")
  }

  test("Read/write of non-existant attribute should cause NoSuchElementException") {
    intercept[NoSuchElementException] {
      testSig.getReadIndexFor[Int]("bla")
    }
    intercept[NoSuchElementException] {
      testSig.getWriteIndexFor[String]("blu")
    }
  }

  test("Read/write an unrelated type should cause ClassCastException") {
    intercept[ClassCastException] {
      testSig.getReadIndexFor[String]("int_attr")
    }
    intercept[ClassCastException] {
      testSig.getWriteIndexFor[Int]("string_attr")
    }
  }

  test("We can write subclass to a superclass slot") {
    testSig.getWriteIndexFor[Child]("parent_attr")
  }

  test("We cannot read subclass from a superclass slot") {
    intercept[ClassCastException] {
      testSig.getReadIndexFor[Child]("parent_attr")
    }
  }

  test("We cannot write superclass to a subclass slot") {
    intercept[ClassCastException] {
      testSig.getWriteIndexFor[Parent]("child_attr")
    }
  }

  test("We can read superclass from a subclass slot") {
    testSig.getReadIndexFor[Parent]("child_attr")
  }

  test("Cloner keeps old attribute value") {
    val oldDa = DenseAttributes(testSig)
    oldDa.set(testSig.getWriteIndexFor[String]("string_attr"), "hi")
    val newDa = extensionCloner.clone(oldDa)
    newDa.set(extendedSig.getWriteIndexFor[String]("string_attr2"), "hoho")
    val values = extendedSig.getAttributeValues[String](newDa)
    assert(values.size == 2)
    assert(values("string_attr") == "hi")
    assert(values("string_attr2") == "hoho")
  }
}
