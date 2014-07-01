package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class AddReversedEdgesTest extends FunSuite with TestGraphOperation {
  test("example graph") {
    val g = helper.apply(ExampleGraph())
    val out = helper.apply(AddReversedEdges(), 'es -> g('edges))
    assert(helper.localData(out.edgeBundles('esPlus)) === Set(0 -> 1, 1 -> 0, 0 -> 2, 2 -> 0, 1 -> 2, 2 -> 1))
  }
}
