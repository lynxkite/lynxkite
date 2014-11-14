package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class CollectAttributeTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = CollectAttribute[String](Set(0L, 3L))
    val res = op(op.attr, g.name).result
    assert(res.attr.value == Map(0L -> "Adam", 3L -> "Isolated Joe"))
  }
}
