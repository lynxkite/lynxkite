package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite
import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class IdAsAttributeTest extends AnyFunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = IdAsAttribute()
    val attr = op(op.vertices, g.vertices).result.vertexIds
    assert(attr.rdd.collect.toMap == Map(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3))
  }
}
