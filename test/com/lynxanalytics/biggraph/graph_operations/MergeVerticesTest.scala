package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class MergeVerticesTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = MergeVertices[String]()
    val res = op(op.attr, g.gender).result
    val s = res.segments.rdd.keys.collect
    assert(s.size == 2)
    assert(res.belongsTo.rdd.values.collect.toSet == Set(Edge(0, s(0)), Edge(1, s(1)), Edge(2, s(0)), Edge(3, s(0))))
  }
}
