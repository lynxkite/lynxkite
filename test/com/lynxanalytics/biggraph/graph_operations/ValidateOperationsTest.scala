package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.SmallTestGraph._

class ValidateOperationsTest extends FunSuite with TestGraphOp {
  val g1 = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2))).result
  val g2 = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1))).result
  val addWeights = AddConstantDoubleEdgeAttribute(1.0)
  val gw1 = addWeights(addWeights.edges, g1.es).result
  val gw2 = addWeights(addWeights.edges, g2.es).result
  val op = ConcatenateBundles()

  test("validate input structure - missing input") {
    intercept[java.lang.AssertionError] {
      op(op.weightsAB, gw1.attr)(op.edgesBC, g2.es).result
    }
  }
  test("validate input structure - collision") {
    intercept[java.lang.AssertionError] {
      op(op.weightsAB, gw1.attr)(op.weightsBC, gw2.attr).result
    }
  }
}
