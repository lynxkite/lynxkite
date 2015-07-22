package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class AggregateEdgeAttributeOperationTest extends OperationsTestBase {
  test("Aggregate edge attribute") {
    run("Example Graph")
    run("Aggregate edge attribute globally",
      Map("prefix" -> "", "aggregate-weight" -> "sum", "aggregate-comment" -> ""))
    assert(project.scalars("weight_sum").value == 10.0)
  }
}
