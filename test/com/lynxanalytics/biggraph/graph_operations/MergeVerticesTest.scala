package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class MergeVerticesTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = MergeVertices[String]()
    val res = op(op.attr, g.gender).result
    val segments = res.segments.rdd.keys.collect
    assert(segments.size == 2)
    val s0 = segments(0)
    val s1 = segments(1)
    assert(res.belongsTo.rdd.values.collect.toSet == Set(Edge(0, s0), Edge(1, s1), Edge(2, s0), Edge(3, s0)))
  }
}
