package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class TriadicClosureTest extends OperationsTestBase {
  test("Triadic closure") {
    run("Create example graph")
    run("Replace edges with triadic closure")

    val ab_weights = project.edgeAttributes("ab_weight").runtimeSafeCast[Double]
    assert(ab_weights.rdd.values.collect.toSeq.sorted == Seq(1.0, 2.0, 3.0, 4.0))

    val bc_weights = project.edgeAttributes("bc_weight").runtimeSafeCast[Double]
    assert(bc_weights.rdd.values.collect.toSeq.sorted == Seq(1.0, 1.0, 2.0, 2.0))

    val ab_comments = project.edgeAttributes("ab_comment").runtimeSafeCast[String]
    assert(ab_comments.rdd.values.collect.toSeq.sorted == Seq(
      "Adam loves Eve",
      "Bob envies Adam",
      "Bob loves Eve",
      "Eve loves Adam"))

    val bc_comments = project.edgeAttributes("bc_comment").runtimeSafeCast[String]
    assert(bc_comments.rdd.values.collect.toSeq.sorted == Seq(
      "Adam loves Eve",
      "Adam loves Eve",
      "Eve loves Adam",
      "Eve loves Adam"))

  }
}
