package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class AddConstantAttributeTest extends AnyFunSuite with TestGraphOp {
  test("triangle vertex attribute") {
    val g = SmallTestGraph(Map(0 -> Seq(1), 1 -> Seq(2), 2 -> Seq(0))).result
    val op = AddConstantDoubleAttribute(100.0)
    val out = op(op.vs, g.vs).result

    val res = g.vs.rdd.join(out.attr.rdd).mapValues(_._2).collect.toMap

    assert(res == Map(0l -> 100.0, 1l -> 100.0, 2l -> 100.0))
  }

  test("triangle edge attribute") {
    val g = SmallTestGraph(Map(0 -> Seq(1), 1 -> Seq(2), 2 -> Seq(0))).result
    val eAttr = AddConstantAttribute.run(g.es.idSet, 100.0)

    // join edge bundle and weight data to make an output that is easy to read
    val res = g.es.rdd.join(eAttr.rdd).map {
      case (id, (edge, value)) =>
        (edge.src.toInt, edge.dst.toInt) -> value
    }.collect.toMap

    assert(res == Map((0l, 1l) -> 100.0, (1l, 2l) -> 100.0, (2l, 0l) -> 100.0))
  }
}
