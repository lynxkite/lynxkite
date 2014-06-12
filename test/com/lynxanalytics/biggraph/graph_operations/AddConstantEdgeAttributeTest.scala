package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class AddConstantEdgeAttributeTest extends FunSuite {
  test("triangle") {
    val input = TestWizard.run(SmallGraph(Map(0 -> Seq(1), 1 -> Seq(2), 2 -> Seq(0))), DataSet())
    val op = TestWizard.run(AddConstantDoubleEdgeAttribute(100.0), DataSet(
      edgeBundles = Map('edges -> input.edgeBundles('es))))
    val attrs = op.edgeAttributes('attr)
    assert(attrs.rdd.collect.toMap == Map(0 -> 100.0, 1 -> 100.0, 2 -> 100.0))
  }
}
