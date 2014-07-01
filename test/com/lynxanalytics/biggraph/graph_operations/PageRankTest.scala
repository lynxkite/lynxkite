package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class PageRankTest extends FunSuite with TestGraphOperation {
  test("example graph") {
    val g = helper.apply(ExampleGraph())
    val out = helper.apply(PageRank(0.5, 3), 'weights -> g('weight))
    val ranks = helper.localData[Double](out.vertexAttributes('pagerank))
    assert(1.0 < ranks(0) && ranks(0) < 2.0, s"Bad rank: ${ranks(0)}")
    assert(1.0 < ranks(1) && ranks(1) < 2.0, s"Bad rank: ${ranks(1)}")
    assert(0.0 < ranks(2) && ranks(2) < 1.0, s"Bad rank: ${ranks(2)}")
    assert(0.0 < ranks(3) && ranks(3) < 1.0, s"Bad rank: ${ranks(3)}")
  }
}
