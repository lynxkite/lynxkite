package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class PageRankTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val eg = ExampleGraph().builder.result
    val op = PageRank(0.5, 3)
    val res = op.set(op.weights, eg.weight).result
    val ranks = res.pagerank.rdd.collect.toMap
    assert(1.0 < ranks(0) && ranks(0) < 2.0, s"Bad rank: ${ranks(0)}")
    assert(1.0 < ranks(1) && ranks(1) < 2.0, s"Bad rank: ${ranks(1)}")
    assert(0.0 < ranks(2) && ranks(2) < 1.0, s"Bad rank: ${ranks(2)}")
    assert(0.0 < ranks(3) && ranks(3) < 1.0, s"Bad rank: ${ranks(3)}")
  }
}
