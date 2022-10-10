package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class ClusteringCoefficientTest extends AnyFunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = ClusteringCoefficient()
    val out = op(op.vs, g.vertices)(op.es, g.edges).result
    assert(out.clustering.rdd.collect.toMap == Map(0 -> 0.5, 1 -> 0.5, 2 -> 1.0, 3 -> 1.0))
  }
}
