package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class FillEdgeAttributeWithConstantDefaultValueOperationTest extends OperationsTestBase {
  test("Fill edge attribute with constant default value") {
    run("Example Graph")
    run("Derived edge attribute",
      Map("type" -> "double", "output" -> "income_edge", "expr" -> "src$income"))
    run("Fill edge attribute with constant default value",
      Map("attr" -> "income_edge", "def" -> "-1.0"))
    val filledIncome = project.edgeAttributes("income_edge").runtimeSafeCast[Double]
    assert(filledIncome.rdd.values.collect.toSeq.sorted == Seq(-1.0, 1000.0, 2000.0, 2000.0))
  }
}

