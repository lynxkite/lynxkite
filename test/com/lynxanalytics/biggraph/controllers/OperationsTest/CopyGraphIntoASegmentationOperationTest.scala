package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class CopyGraphIntoASegmentationOperationTest extends OperationsTestBase {
  test("Copy graph into a segmentation") {
    run("Example Graph")
    run("Copy graph into a segmentation", Map("name" -> "seg"))
    val seg = project.segmentation("seg")
    assert(seg.belongsTo.toIdPairSeq == Seq((0, (0, 0)), (1, (1, 1)), (2, (2, 2)), (3, (3, 3))))
    val name = seg.project.vertexAttributes("name").runtimeSafeCast[String]
    assert(name.rdd.values.collect.toSeq.sorted == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
  }

}

