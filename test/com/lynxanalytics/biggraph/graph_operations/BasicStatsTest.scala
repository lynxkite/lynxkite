package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class BasicStatsTest extends FunSuite with TestGraphOperation {
  test("compute basic stats") {
    val graph = helper.apply(ExampleGraph())
    val count = helper.apply(CountVertices(), graph.mapNames('vertices -> 'vertices))
    val minmax = helper.apply(ComputeMinMaxDouble(), graph.mapNames('age -> 'attribute))
    assert(helper.localData(count.scalars('count)) == 4)
    assert(helper.localData(minmax.scalars('min)) == 2.0)
    assert(helper.localData(minmax.scalars('max)) == 50.3)
  }
}
