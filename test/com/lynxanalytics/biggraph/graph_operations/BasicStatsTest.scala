package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class BasicStatsTest extends FunSuite with TestGraphOperation {
  test("compute basic stats") {
    val graph = helper.apply(CreateExampleGraphOperation())
    val count = helper.apply(CountVertices(), graph.mapNames('vertices -> 'vertices))
    val minmax = helper.apply(ComputeMinMaxDouble(), graph.mapNames('age -> 'attribute))
    assert(helper.localData(count.scalars('count)) == 3)
    assert(helper.localData(minmax.scalars('min)) == 18.2)
    assert(helper.localData(minmax.scalars('max)) == 50.3)
  }
}
