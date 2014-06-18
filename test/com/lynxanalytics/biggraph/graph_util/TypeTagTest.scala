package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite
import scala.reflect.runtime.universe._

class A[T] {
}
class TypeTagTest extends FunSuite {
  def bla[T: TypeTag](a: A[T]) {
    println("TT", typeTag[T])
    //    println("ATT", a.typeTag)
    //    println("EQ", a.typeTag.tpe =:= typeTag[T].tpe)
  }

  test("We can export attributes") {
    val c: A[T] forSome { type T } = new A[Int]
    bla(c)
  }
}

