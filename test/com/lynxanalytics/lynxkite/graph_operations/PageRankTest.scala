package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class PageRankTest extends AnyFunSuite with TestGraphOp {
  test("example graph") {
    val eg = ExampleGraph()().result
    val op = PageRank(0.5, 3)
    val res = op(op.es, eg.edges)(op.weights, eg.weight).result
    val ranks = res.pagerank.rdd.collect.toMap
    assert(1.0 < ranks(0) && ranks(0) < 2.0, s"Bad rank: ${ranks(0)}")
    assert(1.0 < ranks(1) && ranks(1) < 2.0, s"Bad rank: ${ranks(1)}")
    assert(0.0 < ranks(2) && ranks(2) < 1.0, s"Bad rank: ${ranks(2)}")
    assert(0.0 < ranks(3) && ranks(3) < 1.0, s"Bad rank: ${ranks(3)}")
  }
}
