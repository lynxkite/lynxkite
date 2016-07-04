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
    assert(hash == Seq(0 -> "99263F16589D60", 1 -> "AB72E85EFFE1DE", 2 -> "EF6AE3AE06DA2C", 3 -> "919EDDE6FCBBF2"))
  }
}
