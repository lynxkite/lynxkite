package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class PartitionAttributeTest extends FunSuite with TestGraphOp {
  test("example graph - random role") {
    val g = ExampleGraph()().result
    val roles = {
      val op = CreateRole(0.5, 0)
      op(op.vertices, g.vertices).result.role
    }
    val r = roles.rdd.collect.toMap
    assert(r == Map(0 -> "train", 1 -> "train", 2 -> "test", 3 -> "test"))

    val op = PartitionAttribute[Double]()
    val out = op(op.attr, g.age)(op.role, roles).result

    val te = out.test.rdd.collect
    val tr = out.train.rdd.collect

    te.foreach { case (a, b) => assert(r(a) == "test") }
    tr.foreach { case (a, b) => assert(r(a) == "train") }
    assert(te.size + tr.size == r.size)
  }
}
