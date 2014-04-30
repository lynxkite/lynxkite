package com.lynxanalytics.biggraph.graph_api.attributes

import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import org.scalatest.FunSuite

class AttributesTest extends FunSuite {
  class Parent
  class Child extends Parent

  val testSig = AttributeSignature
    .empty
    .addAttribute[String]("string_attr")
    .addAttribute[Int]("int_attr")
    .addAttribute[Parent]("parent_attr")
    .addAttribute[Child]("child_attr")
    .signature

  val SignatureExtension(extendedSig, extensionCloner) = testSig
    .addAttribute[String]("string_attr2")

  test("Setting and reading an attribute should work") {
    val writeIdx = testSig.writeIndex[String]("string_attr")
    val readIdx = testSig.readIndex[String]("string_attr")
    val da = testSig.maker.make
    da.set(writeIdx, "Hello")
    assert(da(readIdx) == "Hello")
  }

  test("Read/write of non-existant attribute should cause NoSuchElementException") {
    intercept[NoSuchElementException] {
      testSig.readIndex[Int]("bla")
    }
    intercept[NoSuchElementException] {
      testSig.writeIndex[String]("blu")
    }
  }

  test("Read/write an unrelated type should cause ClassCastException") {
    intercept[ClassCastException] {
      testSig.readIndex[String]("int_attr")
    }
    intercept[ClassCastException] {
      testSig.writeIndex[Int]("string_attr")
    }
  }

  test("We can write subclass to a superclass slot") {
    testSig.writeIndex[Child]("parent_attr")
  }

  test("We cannot read subclass from a superclass slot") {
    intercept[ClassCastException] {
      testSig.readIndex[Child]("parent_attr")
    }
  }

  test("We cannot write superclass to a subclass slot") {
    intercept[ClassCastException] {
      testSig.writeIndex[Parent]("child_attr")
    }
  }

  test("We can read superclass from a subclass slot") {
    testSig.readIndex[Parent]("child_attr")
  }

  test("Cloner keeps old attribute value") {
    val oldDa = testSig.maker.make
    oldDa.set(testSig.writeIndex[String]("string_attr"), "hi")
    val newDa = extensionCloner.clone(oldDa)
    newDa.set(extendedSig.writeIndex[String]("string_attr2"), "hoho")
    val values = AttributeUtil.getAttributeValues[String](extendedSig, newDa)
    assert(values.size == 2)
    assert(values("string_attr") == "hi")
    assert(values("string_attr2") == "hoho")
  }

  test("Size returns size....") {
    assert(testSig.size == 4)
    assert(extendedSig.size == 5)
  }

  test("Signatures are not serializable") {
    intercept[java.io.NotSerializableException] {
      val buffer = new ByteArrayOutputStream
      val objectStream = new ObjectOutputStream(buffer)
      objectStream.writeObject(testSig)
    }
  }

  test("We can serialize things that should be serializable") {
    val buffer = new ByteArrayOutputStream
    val objectStream = new ObjectOutputStream(buffer)
    objectStream.writeObject(testSig.maker)
    objectStream.writeObject(testSig.readIndex[String]("string_attr"))
    objectStream.writeObject(testSig.writeIndex[Parent]("parent_attr"))
    objectStream.writeObject(extensionCloner)
  }
}
