package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class ReverseEdgesTest extends FunSuite with TestGraphOperation {
  test("example graph") {
    val g = helper.apply(ExampleGraph())
    val out = helper.apply(ReverseEdges(), 'esAB -> g('edges))
    assert(helper.localData(out.edgeBundles('esBA)) == Set(0 -> 1, 1 -> 0, 0 -> 2, 1 -> 2))
  }
}
