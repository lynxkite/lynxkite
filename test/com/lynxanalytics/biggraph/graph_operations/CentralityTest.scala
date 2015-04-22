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
      Map(0 -> 3.0, 1 -> 3.0, 2 -> 1.0, 3 -> 1.0))
  }
}
