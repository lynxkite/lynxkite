package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class AddConstantEdgeAttributeTest extends FunSuite with TestGraphOperation {
  test("triangle") {
    val (vs, es) = helper.smallGraph(Map(0 -> Seq(1), 1 -> Seq(2), 2 -> Seq(0)))
    val out = helper.apply(AddConstantDoubleEdgeAttribute(100.0), 'edges -> es)
    assert(helper.localData(out.edgeAttributes('attr)) ==
      Map((0l, 1l) -> 100.0, (1l, 2l) -> 100.0, (2l, 0l) -> 100.0))
  }
}
