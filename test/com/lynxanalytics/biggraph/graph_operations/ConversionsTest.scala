package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.controllers.FEVertexAttributeFilter
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

import scala.reflect.runtime.universe._

class ConversionsTest extends FunSuite with TestGraphOp {
  test("vertex attribute to string") {
    val graph = ExampleGraph()().result
    val string = {
      val op = VertexAttributeToString[Double]()
      op(op.attr, graph.age).result.attr
    }
    assert(string.rdd.collect.toMap
      == Map(0 -> "20.3", 1 -> "18.2", 2 -> "50.3", 3 -> "2.0"))
  }

  test("vertex attribute to double") {
    val graph = ExampleGraph()().result
    val string = {
      val op = VertexAttributeToString[Double]()
      op(op.attr, graph.age).result.attr
    }
    val double = {
      val op = VertexAttributeToDouble()
      op(op.attr, string).result.attr
    }
    assert(double.rdd.collect.toMap
      == Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
  }

  test("Double formatting") {
    assert(DynamicValue.convert(0.0).string == "0")
    assert(DynamicValue.convert(1.0).string == "1")
    assert(DynamicValue.convert(1.1).string == "1.1")
    assert(DynamicValue.convert(1.0001).string == "1.0001")
    assert(DynamicValue.convert(1.000100001).string == "1.0001")
  }

  case class TagTester[T: TypeTag](value: T) {
    val typeTag = implicitly[TypeTag[T]]
  }

  def testTag[T](t: TagTester[T]) = {
    val jsonRepr = TypeTagToFormat.typeTagToFormat(t.typeTag).writes(t.value)
    val readBack = TypeTagToFormat.typeTagToFormat(t.typeTag).reads(jsonRepr)
    assert(readBack.get == t.value)
  }

  test("TypeTag -> json conversions (simple types)") {
    // Simple types:
    testTag(TagTester("hello"))
    testTag(TagTester(42L))
    testTag(TagTester(42))
    testTag(TagTester(3.1415))
  }

  test("TypeTag -> json conversions (options)") {
    testTag(TagTester(Option(1.0)))
    testTag(TagTester[Option[Int]](None))
    testTag(TagTester(Option(100L)))
    testTag(TagTester[Option[Option[String]]](Some(Some("embedded option"))))
  }

  test("TypeTag -> json conversions (ToJson traits)") {
    val filter = VertexAttributeFilter[Double](DoubleGT(2))
    testTag(TagTester[VertexAttributeFilter[Double]](filter))
    val edge = Edge(10L, 20L)
    testTag(TagTester(edge))
    val dv = DynamicValue("name", true, Some(1.2), Some(1.3), None)
    testTag(TagTester(dv))
  }

  test("TypeTag -> json conversions (Tuples)") {
    testTag(TagTester((1, 2)))
    testTag(TagTester((3.14, "SomeString")))
  }

  test("TypeTag -> json conversions (Scala collections)") {
    val seqOfTuples = (1 to 10).zip(11 to 20).seq
    testTag(TagTester(seqOfTuples))

    val mapOfIntString = Map(1 -> "one", 2 -> "two", 3 -> "three")
    testTag(TagTester(mapOfIntString))

    val mapOfStringDouble = Map("one" -> 1.0, "twenty" -> 20.0, "minuspi" -> -3.1415)
    testTag((TagTester(mapOfStringDouble)))
  }
}
