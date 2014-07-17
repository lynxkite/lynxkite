package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import org.apache.spark.SparkContext._
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class SampledViewTest extends FunSuite with TestGraphOp {
  test("example graph, center set, no edges, no size, no label") {
    val eg = ExampleGraph()().result
    val op = SampledView(center = "1", radius = 0, hasEdges = false, hasSizes = false, hasLabels = false)
    val view = op(op.vertices, eg.vertices).result
    assert(view.svVertices.value == Seq(SampledViewVertex(1, 1.0, "")))
    assert(view.sample.rdd.keys.collect.toSet == Set(1))
    assert(view.feIdxs.rdd.collect.toMap == Map(1 -> 0))
  }

  test("example graph, center set, radius 1, age as size, name as label") {
    val eg = ExampleGraph()().result
    val op = SampledView(center = "1", radius = 1, hasEdges = true, hasSizes = true, hasLabels = true)
    val view = op(op.vertices, eg.vertices)(op.edges, eg.edges)(op.sizeAttr, eg.age)(op.labelAttr, eg.name).result
    assert(view.svVertices.value == Seq(
      SampledViewVertex(0, 20.3, "Adam"),
      SampledViewVertex(1, 18.2, "Eve"),
      SampledViewVertex(2, 50.3, "Bob")))
    assert(view.sample.rdd.keys.collect.toSet == Set(0, 1, 2))
    assert(view.feIdxs.rdd.collect.toMap == Map(0 -> 0, 1 -> 1, 2 -> 2))
  }

  test("sample center has no edges") {
    val eg = ExampleGraph()().result
    val op = SampledView(center = "3", radius = 1, hasEdges = true, hasSizes = false, hasLabels = true)
    val view = op(op.vertices, eg.vertices)(op.edges, eg.edges)(op.labelAttr, eg.name).result
    assert(view.svVertices.value == Seq(SampledViewVertex(3, 1.0, "Isolated Joe")))
    assert(view.sample.rdd.keys.collect.toSet == Set(3))
    assert(view.feIdxs.rdd.collect.toMap == Map(3 -> 0))
  }
}
