package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._

class UpperBoundFilterTest extends FunSuite with TestGraphOperation {
  test("vertex filtering works") {
    val graph = helper.apply(ExampleGraph())
    val filtered = helper.apply(UpperBoundFilter(21), graph.mapNames('age -> 'attr, 'vertices -> 'vs))
    assert(helper.localData(filtered.vertexSets('fvs)) == Set(0L, 1L))
  }
  test("edge filtering works") {
    val graph = helper.apply(ExampleGraph())
    val filtered = helper.apply(UpperBoundFilter(21), graph.mapNames('age -> 'attr, 'vertices -> 'vs))
    val joined = helper.apply(
      InducedEdgeBundle(),
      graph.mapNames('edges -> 'input, 'vertices -> 'src, 'vertices -> 'dst)
        ++ filtered.mapNames('fvs -> 'srcSubset, 'fvs -> 'dstSubset))
    assert(helper.localData(joined.edgeBundles('induced)) == Set(0L -> 1L, 1L -> 0L))
  }
}
