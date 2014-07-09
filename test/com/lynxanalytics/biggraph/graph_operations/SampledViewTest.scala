package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.controllers.FEVertex

class SampledViewTest extends FunSuite with TestGraphOperation {
  test("example graph, center set, no edges, no size, no label") {
    val graph = helper.apply(ExampleGraph())
    val view = helper.apply(
      SampledView(center = "1", radius = 0, hasEdges = false, hasSizes = false, hasLabels = false),
      graph.mapNames('vertices -> 'vertices))
    assert(helper.localData(view.scalars('feVertices)) == Seq(FEVertex(1.0, 0, 0, 1, "")))
    assert(helper.localData(view.vertexSets('sample)) == Set(1))
    assert(helper.localData[Int](view.vertexAttributes('feIdxs)) == Map(1 -> 0))
  }

  test("example graph, center set, radius 1, age as size, name as label") {
    val graph = helper.apply(ExampleGraph())
    val view = helper.apply(
      SampledView(center = "1", radius = 1, hasEdges = true, hasSizes = true, hasLabels = true),
      graph.mapNames('vertices -> 'vertices, 'edges -> 'edges, 'age -> 'sizeAttr, 'name -> 'labelAttr))
    assert(helper.localData(view.scalars('feVertices)) == Seq(
      FEVertex(20.3, 0, 0, 0, "Adam"),
      FEVertex(18.2, 0, 0, 1, "Eve"),
      FEVertex(50.3, 0, 0, 2, "Bob")))
    assert(helper.localData(view.vertexSets('sample)) == Set(0, 1, 2))
    assert(helper.localData[Int](view.vertexAttributes('feIdxs)) == Map(0 -> 0, 1 -> 1, 2 -> 2))
  }
}
