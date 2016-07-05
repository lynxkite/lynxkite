package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class HashVertexAttributeTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val eg = ExampleGraph()().result
    val op = HashVertexAttribute("Dennis Bergkamp")
    val res = op(op.vs, eg.vertices)(op.attr, eg.name).result.hashed
    val hash = res.rdd.collect.toSeq.sorted
    assert(hash == Seq(0 -> "29863D72F28233796FD2C420", 1 -> "2F020DD4972D253730990906",
      2 -> "363EA594AD8BFEEEB0EAC85C", 3 -> "A08B9B3BAC49A60C67F6F458"))
  }
}
