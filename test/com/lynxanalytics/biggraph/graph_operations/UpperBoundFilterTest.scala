package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._

class UpperBoundFilterTest extends FunSuite with TestGraphOperation {
  test("vertex filtering works") {
    val graph = helper.apply(CreateExampleGraphOperation())
    val filtered = helper.apply(UpperBoundFilter(21), graph.mapNames('age -> 'attr, 'vertices -> 'vs))
    assert(helper.localData(filtered.vertexSets('fvs)) == Set(0L, 1L))
  }
  test("edge filtering works") {
    val graph = helper.apply(CreateExampleGraphOperation())
    val filtered = helper.apply(UpperBoundFilter(21), graph.mapNames('age -> 'attr, 'vertices -> 'vs))
    val joined = helper.apply(
      EdgesToSubset(),
      graph.mapNames('edges -> 'es, 'vertices -> 'vs, 'vertices -> 'dst)
        ++ filtered.mapNames('fvs -> 'vsSubset, 'projection -> 'projection))
    assert(helper.localData(joined.edgeBundles('esSubset)) == Set(0L -> 1L, 1L -> 0L))
  }
}
