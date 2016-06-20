package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.AttributeRDD
import com.lynxanalytics.biggraph.graph_api.Scripting._

class SplitToTrainAndTestSet extends OperationsTestBase {
  test("Split to train and test sets") {
    val target = "age"
    run("Example Graph")
    run("Split to train and test set",
      Map("source" -> s"$target", "test_set_ratio" -> "0.25", "seed" -> "16"))
    val trainRDD = project.vertexAttributes(s"${target}_train").rdd
    val testRDD = project.vertexAttributes(s"${target}_test").rdd
    // train and test should be disjoint
    assert(trainRDD.join(testRDD).isEmpty)
    // these results are determined by the random seed
    assert(testRDD.count == 1)
    assert(trainRDD.count == 3)
  }
}
