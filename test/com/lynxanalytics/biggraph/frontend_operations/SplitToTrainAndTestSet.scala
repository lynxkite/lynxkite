package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class SplitToTrainAndTestSet extends OperationsTestBase {
  test("Split to train and test sets") {
    val target = "age"
    run("Example Graph")
    run("Split to train and test set",
      Map("target" -> s"$target", "test_set_ratio" -> "0.25", "seed" -> "16"))
    val trainCount = project.vertexAttributes(s"${target}_train").rdd.count
    val testCount = project.vertexAttributes(s"${target}_test").rdd.count
    // these results are determined by the random seed
    assert(testCount == 1)
    assert(trainCount == 3)
  }
}
