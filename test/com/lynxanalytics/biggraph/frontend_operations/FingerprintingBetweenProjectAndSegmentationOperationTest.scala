package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers._

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class FingerprintingBetweenProjectAndSegmentationOperationTest extends OperationsTestBase {
  test("Fingerprinting between project and segmentation") {
    val project = box("Create example graph")
      .box("Use other project as segmentation", Map(
        "name" -> "eg2"), Seq(box("Create example graph")))
      .box("Use table as segmentation links", Map(
        "apply_to_project" -> ".eg2",
        "base_id_attr" -> "name",
        "base_id_column" -> "src",
        "seg_id_attr" -> "name",
        "seg_id_column" -> "dst"), Seq(importCSV("fingerprint-example-connections.csv")))
      .box("Link project and segmentation by fingerprint", Map(
        "apply_to_project" -> ".eg2",
        "mo" -> "1",
        "ms" -> "0.5"))
      .box("Aggregate from segmentation", Map(
        "apply_to_project" -> ".eg2",
        "prefix" -> "seg",
        "aggregate_age" -> "average"))
      .project
    val newAge = project.vertexAttributes("seg_age_average")
      .runtimeSafeCast[Double].rdd.collect.toSeq.sorted
    // Two mappings.
    assert(newAge == Seq(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
    val oldAge = project.vertexAttributes("age")
      .runtimeSafeCast[Double].rdd.collect.toMap
    // They map Adam to Adam, Eve to Eve.
    for ((k, v) <- newAge) {
      assert(v == oldAge(k))
    }
  }

  test("Fingerprinting between project and segmentation by attribute") {
    val other = box("Use table as graph", Map(
      "src" -> "src",
      "dst" -> "dst"), Seq(importCSV("fingerprint-edges-2.csv")))
      .box("Aggregate edge attribute to vertices", Map(
        "prefix" -> "",
        "direction" -> "outgoing edges",
        "aggregate_src_link" -> "most_common"))
      .box("Rename vertex attributes", Map(
        "change_src_link_most_common" -> "link"))
    val golden = box("Use table as graph", Map(
      "src" -> "src",
      "dst" -> "dst"), Seq(importCSV("fingerprint-edges-1.csv")))
      .box("Use other project as segmentation", Map(
        "name" -> "other"), Seq(other))
      .box("Define segmentation links from matching attributes", Map(
        "apply_to_project" -> ".other",
        "base_id_attr" -> "stringId",
        "seg_id_attr" -> "link"))
    def seg(box: TestBox) = box.project.segmentation("other")
    def belongsTo(box: TestBox) = seg(box).belongsTo.toPairSeq
    assert(belongsTo(golden).size == 6)
    val fingerprinted = golden.box("Link project and segmentation by fingerprint", Map(
      "apply_to_project" -> ".other",
      "mo" -> "0",
      "ms" -> "0"))
    assert(belongsTo(fingerprinted).size == 6)
    val similarity = seg(fingerprinted).vertexAttributes("fingerprinting_similarity_score")
      .runtimeSafeCast[Double].rdd.values.collect
    assert(similarity.size == 6)
    assert(similarity.filter(_ > 0).size == 6)
  }
}
