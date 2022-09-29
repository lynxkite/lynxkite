package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class OutDegreeTest extends AnyFunSuite with TestGraphOp {
  test("example graph - unweighted") {
    val g = ExampleGraph()().result
    val op = OutDegree()
    val out = op(op.es, g.edges).result.outDegree
    assert(out.rdd.collect.toMap ==
      Map(0 -> 1.0, 1 -> 1.0, 2 -> 2.0, 3 -> 0.0))
  }
}
