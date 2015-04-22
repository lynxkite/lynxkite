package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class CentralityTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = Centrality()
    val out = op(op.es, g.edges).result.harmonicCentrality
    assert(out.rdd.collect.toMap ==
      Map(0 -> 2.0, 1 -> 2.0, 2 -> 0.0, 3 -> 0.0))
  }
}
