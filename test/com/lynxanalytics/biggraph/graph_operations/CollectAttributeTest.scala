package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class CollectAttributeTest extends AnyFunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = CollectAttribute[String](Set(0L, 3L))
    val res = op(op.attr, g.name).result
    assert(res.idToAttr.value == Map(0L -> "Adam", 3L -> "Isolated Joe"))
  }
}
