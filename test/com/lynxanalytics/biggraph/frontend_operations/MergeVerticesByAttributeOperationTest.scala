package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class MergeVerticesByAttributeOperationTest extends OperationsTestBase {
  test("Merge vertices by attribute") {
    run("Create example graph")
    run("Merge vertices by attribute",
      Map("key" -> "gender", "aggregate_age" -> "average", "aggregate_name" -> "count",
        "aggregate_id" -> "", "aggregate_location" -> "", "aggregate_gender" -> "", "aggregate_income" -> ""))
    val age = project.vertexAttributes("age_average").runtimeSafeCast[Double]
    assert(age.rdd.collect.toMap.values.toSet == Set(24.2, 18.2))
    val count = project.vertexAttributes("name_count").runtimeSafeCast[Double]
    assert(count.rdd.collect.toMap.values.toSet == Set(3.0, 1.0))
    val gender = project.vertexAttributes("gender").runtimeSafeCast[String]
    assert(gender.rdd.collect.toMap.values.toSet == Set("Male", "Female"))
    val v = project.vertexSet.rdd.keys.collect.toSeq.sorted
    val edges = project.edgeBundle
    assert(edges.rdd.values.collect.toSeq.sorted ==
      Seq(Edge(v(0), v(0)), Edge(v(0), v(1)), Edge(v(0), v(1)), Edge(v(1), v(0))))
  }

  test("Merge vertices by attribute, no edge bundle") {
    run("Create example graph")
    run("Discard edges")
    assert(project.edgeBundle == null)
    run("Merge vertices by attribute",
      Map("key" -> "gender", "aggregate_age" -> "average", "aggregate_id" -> "", "aggregate_name" -> "",
        "aggregate_location" -> "", "aggregate_gender" -> "", "aggregate_income" -> ""))
    val age = project.vertexAttributes("age_average").runtimeSafeCast[Double]
    assert(age.rdd.collect.toMap.values.toSet == Set(24.2, 18.2))
    assert(project.edgeBundle == null)
  }

  test("Merge vertices by attribute, segmentation") {
    run("Create example graph")
    run("Segment by string attribute", Map("name" -> "bucketing", "attr" -> "gender"))
    run("Add constant vertex attribute", Map(
      "name" -> "constant",
      "value" -> "1",
      "type" -> "Double",
      "apply_to" -> "|bucketing"))
    run("Merge vertices by attribute", Map(
      "key" -> "constant",
      "aggregate_gender" -> "",
      "aggregate_id" -> "",
      "aggregate_size" -> "",
      "apply_to" -> "|bucketing"))
    val bucketing = project.segmentation("bucketing")
    assert(bucketing.scalars("!coverage").value == 4)
    assert(bucketing.scalars("!belongsToEdges").value == 4)
    assert(bucketing.scalars("!nonEmpty").value == 1)
  }
}
