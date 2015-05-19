package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class IdAsAttributeTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = IdAsAttribute()
    val attr = op(op.vertices, g.vertices).result.vertexIds
    assert(attr.rdd.collect.toMap == Map(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3))
  }
}
