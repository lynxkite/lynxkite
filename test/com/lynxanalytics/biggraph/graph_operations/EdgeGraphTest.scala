package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._

class EdgeGraphTest extends FunSuite with TestGraphOperation {
  test("example graph") {
    val g = helper.apply(ExampleGraph())
    val out = helper.apply(EdgeGraph(), 'es -> g('edges))
    assert(helper.localData(out.vertexSets('newVS)) === Set(0, 1, 2, 3))
    assert(helper.localData(out.edgeBundles('newES)) === Set(1 -> 0, 3 -> 1, 2 -> 0, 0 -> 1))
  }
}
