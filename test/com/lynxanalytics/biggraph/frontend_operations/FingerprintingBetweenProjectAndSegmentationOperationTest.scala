package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers._

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class FingerprintingBetweenProjectAndSegmentationOperationTest extends OperationsTestBase {
  test("Fingerprinting between project and segmentation") {
    run("Example Graph")
    saveAsFrame("ExampleGraph2")
    run("Import project as segmentation", Map(
      "them" -> "ExampleGraph2"))
    val seg = project.segmentation("ExampleGraph2")
    run("Load segmentation links from CSV", Map(
      "files" -> "OPERATIONSTEST$/fingerprint-example-connections.csv",
      "header" -> "src,dst",
      "delimiter" -> ",",
      "omitted" -> "",
      "filter" -> "",
      "allowCorruptLines" -> "yes",
      "base-id-attr" -> "name",
      "base-id-field" -> "src",
      "seg-id-attr" -> "name",
      "seg-id-field" -> "dst"),
      on = seg)
    run("Fingerprinting between project and segmentation", Map(
      "mrew" -> "0.0",
      "mo" -> "1",
      "ms" -> "0.5"),
      on = seg)
    run("Aggregate from segmentation",
      Map("prefix" -> "seg",
        "aggregate-age" -> "average",
        "aggregate-id" -> "",
        "aggregate-name" -> "",
        "aggregate-location" -> "",
        "aggregate-gender" -> "",
        "aggregate-fingerprinting_similarity_score" -> "",
        "aggregate-income" -> ""),
      on = seg)
    val newAge = project.vertexAttributes("seg_age_average")
      .runtimeSafeCast[Double].rdd.collect.toSeq.sorted
    // Two mappings.
    assert(newAge == Seq(0 -> 20.3, 1 -> 18.2))
    val oldAge = project.vertexAttributes("age")
      .runtimeSafeCast[Double].rdd.collect.toMap
    // They map Adam to Adam, Eve to Eve.
    for ((k, v) <- newAge) {
      assert(v == oldAge(k))
    }
  }

  test("Fingerprinting between project and segmentation by attribute") {
    run("Import vertices and edges from single CSV fileset", Map(
      "files" -> "OPERATIONSTEST$/fingerprint-edges-2.csv",
      "header" -> "src,dst,src_link",
      "delimiter" -> ",",
      "src" -> "src",
      "dst" -> "dst",
      "omitted" -> "",
      "allowCorruptLines" -> "yes",
      "filter" -> ""))
    run("Aggregate edge attribute to vertices", Map(
      "prefix" -> "",
      "direction" -> "outgoing edges",
      "aggregate-src_link" -> "most_common",
      "aggregate-dst" -> "",
      "aggregate-src" -> ""))
    run("Rename vertex attribute", Map("from" -> "src_link_most_common", "to" -> "link"))
    saveAsFrame("other")
    run("Import vertices and edges from single CSV fileset", Map(
      "files" -> "OPERATIONSTEST$/fingerprint-edges-1.csv",
      "header" -> "src,dst",
      "delimiter" -> ",",
      "src" -> "src",
      "dst" -> "dst",
      "omitted" -> "",
      "allowCorruptLines" -> "yes",
      "filter" -> ""))
    run("Import project as segmentation", Map(
      "them" -> "other"))
    val seg = project.segmentation("other")
    run("Define segmentation links from matching attributes", Map(
      "base-id-attr" -> "stringID",
      "seg-id-attr" -> "link"),
      on = seg)
    def belongsTo = seg.belongsTo.toPairSeq
    assert(belongsTo.size == 6)
    run("Fingerprinting between project and segmentation", Map(
      "mrew" -> "0",
      "mo" -> "0",
      "ms" -> "0"),
      on = seg)
    assert(belongsTo.size == 5)
    val similarity = seg.vertexAttributes("fingerprinting_similarity_score")
      .runtimeSafeCast[Double].rdd.values.collect
    assert(similarity.size == 5)
    assert(similarity.filter(_ > 0).size == 2)
  }

}
