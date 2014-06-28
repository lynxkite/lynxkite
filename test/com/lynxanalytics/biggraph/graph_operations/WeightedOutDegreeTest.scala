package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class WeightedOutDegreeTest extends FunSuite with TestGraphOperation {
  test("example graph") {
    val g = helper.apply(ExampleGraph())
    val out = helper.apply(WeightedOutDegree(), 'weights -> g.edgeAttributes('weight))
    assert(helper.localData(out.vertexAttributes('outdegrees)) ===
      Map(0 -> 1.0, 1 -> 2.0, 2 -> 7.0, 3 -> 0.0))
  }
}
