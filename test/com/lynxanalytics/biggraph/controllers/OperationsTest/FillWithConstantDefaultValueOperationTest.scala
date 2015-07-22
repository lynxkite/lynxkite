package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class FillWithConstantDefaultValueOperationTest extends OperationsTestBase {
  test("Fill with constant default value") {
    run("Example Graph")
    run("Fill with constant default value",
      Map("attr" -> "income", "def" -> "-1.0"))
    val filledIncome = project.vertexAttributes("income").runtimeSafeCast[Double]
    assert(filledIncome.rdd.values.collect.toSeq.sorted == Seq(-1.0, -1.0, 1000.0, 2000.0))
  }
}

