package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class MergeVerticesByAttributeOperationTest extends OperationsTestBase {
  test("Merge vertices by attribute") {
    val project = box("Create example graph")
      .box("Merge vertices by attribute",
        Map("key" -> "gender", "aggregate_age" -> "average", "aggregate_name" -> "count",
          "aggregate_id" -> "", "aggregate_location" -> "", "aggregate_gender" -> "", "aggregate_income" -> ""))
      .project
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
    val base = box("Create example graph")
      .box("Discard edges")
    assert(base.project.edgeBundle == null)
    val project = base.box("Merge vertices by attribute",
      Map("key" -> "gender", "aggregate_age" -> "average", "aggregate_id" -> "", "aggregate_name" -> "",
        "aggregate_location" -> "", "aggregate_gender" -> "", "aggregate_income" -> "")).project
    val age = project.vertexAttributes("age_average").runtimeSafeCast[Double]
    assert(age.rdd.collect.toMap.values.toSet == Set(24.2, 18.2))
    assert(project.edgeBundle == null)
  }

  test("Merge vertices by attribute, segmentation") {
    val project = box("Create example graph")
      .box("Segment by String attribute", Map("name" -> "bucketing", "attr" -> "gender"))
      .box("Add constant vertex attribute", Map(
        "name" -> "constant",
        "value" -> "1",
        "type" -> "Double",
        "apply_to_project" -> "|bucketing"))
      .box("Merge vertices by attribute", Map(
        "key" -> "constant",
        "aggregate_gender" -> "",
        "aggregate_id" -> "",
        "aggregate_size" -> "",
        "apply_to_project" -> "|bucketing")).project
    val bucketing = project.segmentation("bucketing")
    assert(bucketing.scalars("!coverage").value == 4)
    assert(bucketing.scalars("!belongsToEdges").value == 4)
    assert(bucketing.scalars("!nonEmpty").value == 1)
  }
}
