package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class RemoveNonSymmetricEdgesTest extends FunSuite with TestGraphOperation {
  test("example graph") {
    val g = helper.apply(ExampleGraph())
    val out = helper.apply(RemoveNonSymmetricEdges(), 'es -> g('edges))
    assert(helper.localData(out.edgeBundles('symmetric)) == Set(0 -> 1, 1 -> 0))
  }
}
