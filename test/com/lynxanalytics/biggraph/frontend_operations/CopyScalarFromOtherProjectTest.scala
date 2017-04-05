package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class CopyScalarFromOtherProjectTest extends OperationsTestBase {
  test("Take scalar from other project scalar value ok") {
    val other = box("Create example graph")
      .box("Derive scalar", Map(
        "output" -> "scalar_val",
        "type" -> "double",
        "expr" -> "42.0"))
    val project = box("Create example graph")
      .box("Copy scalar from other project", Map(
        "scalar" -> "scalar_val",
        "save_as" -> "my_scalar"), Seq(other))
      .project

    assert(project.scalars("my_scalar").value == 42.0)
  }

  test("Take scalar from segmentation") {
    val other = box("Create example graph")
      .box("Add random vertex attribute", Map(
        "dist" -> "Standard Normal",
        "name" -> "rnd",
        "seed" -> "1474343267"))
      .box("Segment by double attribute", Map(
        "attr" -> "rnd",
        "interval_size" -> "0.1",
        "name" -> "seg",
        "overlap" -> "no"))
      .box("Derive scalar", Map(
        "apply_to_project" -> "|seg",
        "output" -> "scalar_val",
        "type" -> "string",
        "expr" -> "'myvalue'"))
    val project = box("Create example graph")
      .box("Copy scalar from other project", Map(
        "apply_to_scalar" -> "|seg",
        "scalar" -> "scalar_val",
        "save_as" -> "my_scalar_2"), Seq(other))
      .project

    assert(project.scalars("my_scalar_2").value == "myvalue")
  }

  test("Take scalar from segmentation of segmentation") {
    val other = box("Create example graph")
      .box("Add random vertex attribute", Map(
        "dist" -> "Standard Normal",
        "name" -> "rnd",
        "seed" -> "1474343267"))
      .box("Segment by double attribute", Map(
        "attr" -> "rnd",
        "interval_size" -> "0.1",
        "name" -> "seg",
        "overlap" -> "no"))
      .box("Add random vertex attribute", Map(
        "apply_to_project" -> "|seg",
        "dist" -> "Standard Normal",
        "name" -> "rnd2",
        "seed" -> "1474343267"))
      .box("Segment by double attribute", Map(
        "apply_to_project" -> "|seg",
        "attr" -> "rnd2",
        "interval_size" -> "0.1",
        "name" -> "seg2",
        "overlap" -> "no"))
      .box("Derive scalar", Map(
        "apply_to_project" -> "|seg|seg2",
        "output" -> "deep_scalar",
        "type" -> "string",
        "expr" -> "'deep value'"))
    val project = box("Create example graph")
      .box("Copy scalar from other project", Map(
        "apply_to_scalar" -> s"|seg|seg2",
        "scalar" -> "deep_scalar",
        "save_as" -> "my_scalar_3"), Seq(other))
      .project

    assert(project.scalars("my_scalar_3").value == "deep value")
  }
}
