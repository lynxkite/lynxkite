package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class FindMaxCliquesTest extends FunSuite with TestGraphOp {
  test("triangle") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1))).result
    val op = FindMaxCliques(3)
    val fmcOut = op(op.vs, g.vs)(op.es, g.es).result
    assert(fmcOut.segments.rdd.count == 1)
  }

  //TODO: some more creative tests
}
