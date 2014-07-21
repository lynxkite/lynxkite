package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class WeightedOutDegreeTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = WeightedOutDegree()
    val out = op(op.attr, g.weight).result.outDegree
    assert(out.rdd.collect.toMap ==
      Map(0 -> 1.0, 1 -> 2.0, 2 -> 7.0, 3 -> 0.0))
  }
}
