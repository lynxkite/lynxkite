// These tests check some lower level infrastructure beneath
// various operations. So, there is no single operation this class
// is revolving around.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class AuxiliaryOperationTest extends OperationsTestBase {

  test("Optional and mandatory parameters work") {
    val firstPart = box("Create example graph")
      .box("Aggregate edge attribute to vertices", Map(
        "prefix" -> "incoming",
        "direction" -> "incoming edges",
        // "aggregate_comment" -> "", This is now optional
        "aggregate_weight" -> "sum"))
    firstPart.box("Aggregate edge attribute to vertices", Map(
      "prefix" -> "incoming",
      // "direction" -> "incoming edges", This is not optional, but it has
      // a default so it should still work.
      "aggregate_comment" -> "",
      "aggregate_weight" -> "sum")).project
  }

  test("Default parameters work") {
    val base = box("Create enhanced example graph")
    val resultIfNoParams = base
      .box("Compute PageRank", Map())
      .project
      .vertexAttributes("page_rank")
      .rdd.collect.toMap
    val resultIfYesParams = base
      .box("Compute PageRank", Map("iterations" -> "5", "damping" -> "0.85"))
      .project
      .vertexAttributes("page_rank")
      .rdd.collect.toMap
    assert(resultIfNoParams == resultIfYesParams)
  }

  test("Parametric parameters work") {
    val fiftyFives = box("Create example graph")
      .box("Add constant vertex attribute",
        Map("name" -> "const55", "type" -> "Double"),
        Seq(),
        Map("value" -> "${ (1 to 10).sum }"))
      .project
      .vertexAttributes("const55")
      .rdd
      .asInstanceOf[com.lynxanalytics.biggraph.graph_api.AttributeRDD[Double]]
      .values.collect().toSeq
    assert(fiftyFives == Seq(55.0, 55.0, 55.0, 55.0))
  }
}

