package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_operations._

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
    val vs = graph.vertices
    val string = {
      val op = VertexAttributeToString[Double]()
      op(op.attr, graph.age).result.attr
    }
    val doubleFromString = {
      val op = VertexAttributeToDouble()
      op(op.attr, string).result.attr
    }
    val int = {
      val op = AddConstantIntAttribute(1)
      op(op.vs, vs).result.attr
    }
    val doubleFromInt = {
      val op = IntAttributeToDouble()
      op(op.attr, int).result.attr
    }
    assert(doubleFromString.rdd.collect.toMap
      == Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
    assert(doubleFromInt.rdd.collect.toMap
      == Map(0 -> 1.0, 1 -> 1.0, 2 -> 1.0, 3 -> 1.0))
  }

  test("Double formatting") {
    assert(DynamicValue.convert(0.0).string == "0")
    assert(DynamicValue.convert(1.0).string == "1")
    assert(DynamicValue.convert(1.1).string == "1.1")
    assert(DynamicValue.convert(1.0001).string == "1.0001")
    assert(DynamicValue.convert(1.000100001).string == "1.0001")
  }

  test("bundle vertex attributes", com.lynxanalytics.biggraph.SphynxOnly) {
    val graph = ExampleGraph()().result
    val v1 = DeriveScala.deriveAndInferReturnType(
      "Vector(age, 1)", Seq("age" -> graph.age), graph.vertices).runtimeSafeCast[Vector[Double]]
    val v2 = DeriveScala.deriveAndInferReturnType(
      "Vector(0.2)", Seq(), graph.vertices).runtimeSafeCast[Vector[Double]]
    val vectors = Seq(v1, v2)
    val doubles: Seq[Attribute[Double]] = Seq(graph.age, graph.income)
    val res = {
      val op = BundleVertexAttributesIntoVector(2, 2)
      op(op.vs, graph.vertices)(
        op.doubleElements, doubles)(
          op.vectorElements, vectors).result.vectorAttr
    }
    assert(res.rdd.collect.toMap
      == Map(0 -> Vector(20.3, 1000, 20.3, 1, 0.2), 2 -> Vector(50.3, 2000, 50.3, 1, 0.2)))
  }
}
