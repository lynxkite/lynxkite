package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class ClusteringCoefficientTest extends FunSuite with TestGraphOperation {
  test("example graph") {
    val g = helper.apply(ExampleGraph())
    val out = helper.apply(ClusteringCoefficient(), 'vs -> g.vertexSets('vertices), 'es -> g.edgeBundles('edges))
    assert(helper.localData(out.vertexAttributes('clustering)) ===
      Map(0 -> 0.5, 1 -> 0.5, 2 -> 1.0, 3 -> 1.0))
  }
}
