package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class MergeTwoEdgeAttributesOperationTest extends OperationsTestBase {
  test("Merge two edge attributes") {
    run("Example Graph")
    run("Derived edge attribute",
      Map("type" -> "double", "output" -> "income_edge", "expr" -> "src$income"))
    run("Merge two edge attributes",
      Map("name" -> "merged", "attr1" -> "income_edge", "attr2" -> "weight"))
    val merged = project.edgeAttributes("merged").runtimeSafeCast[Double]
    assert(merged.rdd.values.collect.toSeq.sorted == Seq(2.0, 1000.0, 2000.0, 2000.0))
  }
}

