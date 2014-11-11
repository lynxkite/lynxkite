package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

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
}
