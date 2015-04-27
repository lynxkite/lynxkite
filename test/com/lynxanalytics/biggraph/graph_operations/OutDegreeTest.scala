package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class OutDegreeTest extends FunSuite with TestGraphOp {
  test("example graph - unweighted") {
    val g = ExampleGraph()().result
    val op = OutDegree()
    val out = op(op.es, g.edges).result.outDegree
    assert(out.rdd.collect.toMap ==
      Map(0 -> 1.0, 1 -> 1.0, 2 -> 2.0, 3 -> 0.0))
  }
}
