package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class SplitToTrainAndTestSet extends OperationsTestBase {
  test("Split to train and test sets") {
    val source = "age"
    run("Create example graph")
    run("Split to train and test set",
      Map("source" -> s"$source", "test_set_ratio" -> "0.25", "seed" -> "16"))
    val trainRDD = project.vertexAttributes(s"${source}_train").rdd
    val testRDD = project.vertexAttributes(s"${source}_test").rdd
    // train and test should be disjoint
    assert(trainRDD.join(testRDD).isEmpty)
    // these results are determined by the random seed
    assert(testRDD.count == 1)
    assert(trainRDD.count == 3)
  }
}
