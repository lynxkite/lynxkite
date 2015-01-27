package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class BasicStatsTest extends FunSuite with TestGraphOp {
  val g = ExampleGraph()().result

  test("compute basic stats - vertex count") {
    val op = CountVertices()
    val out = op(op.vertices, g.vertices).result
    assert(out.count.value === 4)
  }
  test("compute basic stats - edge count") {
    val op = CountEdges()
    val out = op(op.edges, g.edges).result
    assert(out.count.value === 4)
  }
  test("compute basic stats - min max values") {
    val op = ComputeMinMaxDouble()
    val out = op(op.attribute, g.age).result
    assert(out.min.value === 2.0)
    assert(out.max.value === 50.3)
  }
  test("compute basic stats - top values") {
    val op = ComputeTopValues[String](2)
    val out = op(op.attribute, g.name).result
    assert(out.topValues.value.toSet === Set("Adam" -> 1, "Eve" -> 1))
  }
}
