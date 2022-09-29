package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.controllers._
import com.lynxanalytics.lynxkite.graph_api.Scripting._

class CopyScalarFromOtherProjectTest extends OperationsTestBase {
  test("Take scalar from other project scalar value ok") {
    val other = box("Create example graph")
      .box(
        "Derive graph attribute",
        Map(
          "output" -> "scalar_val",
          "expr" -> "42.0"))
    val project = box("Create example graph")
      .box(
        "Copy graph attribute from other graph",
        Map(
          "graph_attribute" -> "scalar_val",
          "save_as" -> "my_scalar"),
        Seq(other))
      .project

    assert(project.scalars("my_scalar").value == 42.0)
  }

  test("Take graph attribute from segmentation") {
    val other = box("Create example graph")
      .box(
        "Add random vertex attribute",
        Map(
          "dist" -> "Standard Normal",
          "name" -> "rnd",
          "seed" -> "1474343267"))
      .box(
        "Segment by numeric attribute",
        Map(
          "attr" -> "rnd",
          "interval_size" -> "0.1",
          "name" -> "seg",
          "overlap" -> "no"))
      .box(
        "Derive graph attribute",
        Map(
          "apply_to_graph" -> ".seg",
          "output" -> "scalar_val",
          "expr" -> "\"myvalue\""))
    val project = box("Create example graph")
      .box(
        "Copy graph attribute from other graph",
        Map(
          "apply_to_source" -> ".seg",
          "graph_attribute" -> "scalar_val",
          "save_as" -> "my_scalar_2"),
        Seq(other))
      .project

    assert(project.scalars("my_scalar_2").value == "myvalue")
  }

  test("Take scalar from segmentation of segmentation") {
    val other = box("Create example graph")
      .box(
        "Add random vertex attribute",
        Map(
          "dist" -> "Standard Normal",
          "name" -> "rnd",
          "seed" -> "1474343267"))
      .box(
        "Segment by numeric attribute",
        Map(
          "attr" -> "rnd",
          "interval_size" -> "0.1",
          "name" -> "seg",
          "overlap" -> "no"))
      .box(
        "Add random vertex attribute",
        Map(
          "apply_to_graph" -> ".seg",
          "dist" -> "Standard Normal",
          "name" -> "rnd2",
          "seed" -> "1474343267"))
      .box(
        "Segment by numeric attribute",
        Map(
          "apply_to_graph" -> ".seg",
          "attr" -> "rnd2",
          "interval_size" -> "0.1",
          "name" -> "seg2",
          "overlap" -> "no"))
      .box(
        "Derive graph attribute",
        Map(
          "apply_to_graph" -> ".seg.seg2",
          "output" -> "deep_scalar",
          "expr" -> "\"deep value\""))
    val project = box("Create example graph")
      .box(
        "Copy graph attribute from other graph",
        Map(
          "apply_to_source" -> s".seg.seg2",
          "graph_attribute" -> "deep_scalar",
          "save_as" -> "my_scalar_3"),
        Seq(other))
      .project

    assert(project.scalars("my_scalar_3").value == "deep value")
  }
}
