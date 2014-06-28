package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class ConversionsTest extends FunSuite with TestGraphOperation {
  test("edge attribute to string") {
    val graph = helper.apply(ExampleGraph())
    val string = helper.apply(EdgeAttributeToString(), 'attr -> graph('weight))
    assert(helper.localData(string.edgeAttributes('string))
      == Map((0 -> 1) -> "1.0", (1 -> 0) -> "2.0", (2 -> 1) -> "4.0", (2 -> 0) -> "3.0"))
  }

  test("edge attribute to double") {
    val graph = helper.apply(ExampleGraph())
    val string = helper.apply(EdgeAttributeToString(), 'attr -> graph('weight))
    val double = helper.apply(EdgeAttributeToDouble(), 'attr -> string('string))
    assert(helper.localData(double.edgeAttributes('double))
      == Map((0 -> 1) -> 1.0, (1 -> 0) -> 2.0, (2 -> 1) -> 4.0, (2 -> 0) -> 3.0))
  }

  test("vertex attribute to string") {
    val graph = helper.apply(ExampleGraph())
    val string = helper.apply(VertexAttributeToString(), 'attr -> graph('age))
    assert(helper.localData(string.vertexAttributes('string))
      == Map(0 -> "20.3", 1 -> "18.2", 2 -> "50.3", 3 -> "2.0"))
  }

  test("vertex attribute to double") {
    val graph = helper.apply(ExampleGraph())
    val string = helper.apply(VertexAttributeToString(), 'attr -> graph('age))
    val double = helper.apply(VertexAttributeToDouble(), 'attr -> string('string))
    assert(helper.localData(double.vertexAttributes('double))
      == Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
  }
}
