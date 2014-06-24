package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class WeightedOutDegreeTest extends FunSuite with TestGraphOperation {
  test("triangle") {
    val (vs, es, ws) = helper.weightedGraph(Map(0 -> Seq((1, 1.0)), 1 -> Seq((2, 1.0)), 2 -> Seq((0, 1.0))))
    val out = helper.apply(WeightedOutDegree(), 'vsA -> vs, 'vsB -> vs, 'edges -> es, 'weights -> ws)
    assert(helper.localData(out.vertexAttributes('outdegrees)) ===
      Map(0l -> 1.0, 1l -> 1.0, 2l -> 1.0))
  }

  test("triangle with different weights") {
    val (vs, es, ws) = helper.weightedGraph(Map(0 -> Seq((1, 1.0), (2, 1.5)), 1 -> Seq((2, 1.0)), 2 -> Seq((0, 3.0))))
    val out = helper.apply(WeightedOutDegree(), 'vsA -> vs, 'vsB -> vs, 'edges -> es, 'weights -> ws)
    assert(helper.localData(out.vertexAttributes('outdegrees)) ===
      Map(0l -> 2.5, 1l -> 1.0, 2l -> 3.0))
  }
}
