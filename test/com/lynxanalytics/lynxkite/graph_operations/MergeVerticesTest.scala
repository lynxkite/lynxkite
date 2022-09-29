package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class MergeVerticesTest extends AnyFunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = MergeVertices[String]()
    val res = op(op.attr, g.gender).result
    val s = res.segments.rdd.keys.collect
    assert(s.size == 2)
    assert(res.belongsTo.rdd.values.collect.toSet == Set(Edge(0, s(0)), Edge(1, s(1)), Edge(2, s(0)), Edge(3, s(0))))
  }

  test("vertices where attribute is undefined are discarded") {
    val g = ExampleGraph()().result
    val op = MergeVertices[Double]()
    val res = op(op.attr, g.income).result
    val s = res.segments.rdd.keys.collect
    assert(s.size == 2)
    assert(res.belongsTo.rdd.values.collect.toSet == Set(Edge(0, s(0)), Edge(2, s(1))))
  }
}
