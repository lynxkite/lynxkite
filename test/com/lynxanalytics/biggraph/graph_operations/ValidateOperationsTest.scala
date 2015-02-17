package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.SmallTestGraph._

class ValidateOperationsTest extends FunSuite with TestGraphOp {
  val g1 = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2))).result
  val g2 = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1))).result
  val gw1 = AddConstantAttribute.run(g1.es.idSet, 1.0)
  val gw2 = AddConstantAttribute.run(g2.es.idSet, 1.0)
  val op = ConcatenateBundles()

  test("all good") {
    op(op.edgesAB, g1.es)(op.edgesBC, g1.es)(
      op.weightsAB, gw1)(op.weightsBC, gw1).result
  }
  test("missing input") {
    val e = intercept[java.util.NoSuchElementException] {
      op(op.edgesAB, g1.es)(op.edgesBC, g1.es)(
        op.weightsAB, gw1).result
    }
    assert(e.getMessage.contains("weightsBC"))
  }
  test("collision") {
    val e = intercept[java.lang.AssertionError] {
      op(op.edgesAB, g1.es)(op.edgesBC, g2.es)(
        op.weightsAB, gw1)(op.weightsBC, gw2).result
    }
    assert(e.getMessage.contains("Collision"))
  }
}
