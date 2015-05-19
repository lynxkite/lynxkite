package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class MergeVerticesTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = MergeVertices[String]()
    val res = op(op.attr, g.gender).result
    assert(res.segments.rdd.keys.collect.toSet == Set(0, 1))
    assert(res.belongsTo.rdd.values.collect.toSet == Set(Edge(0, 0), Edge(1, 1), Edge(2, 0), Edge(3, 0)))
  }
}
