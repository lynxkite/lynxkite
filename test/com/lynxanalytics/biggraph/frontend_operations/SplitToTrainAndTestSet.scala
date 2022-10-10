package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class SplitToTrainAndTestSet extends OperationsTestBase {
  test("Split to train and test sets") {
    val source = "age"
    val project = box("Create example graph")
      .box(
        "Split to train and test set",
        Map("source" -> s"$source", "test_set_ratio" -> "0.25", "seed" -> "16"))
      .project
    val trainRDD = project.vertexAttributes(s"${source}_train").rdd
    val testRDD = project.vertexAttributes(s"${source}_test").rdd
    // train and test should be disjoint
    assert(trainRDD.join(testRDD).isEmpty)
    // these results are determined by the random seed
    assert(testRDD.count == 1)
    assert(trainRDD.count == 3)
  }
}
