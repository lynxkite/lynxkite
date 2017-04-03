package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers.DirectoryEntry
import com.lynxanalytics.biggraph.graph_api.Edge
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ExportImportOperationTest extends OperationsTestBase {
  test("Imports from implicit tables") {
    val eg = box("Create example graph")
      .box("Find connected components", Map(
        "name" -> "cc",
        "directions" -> "ignore directions"))
      .box("Convert vertex attribute to string", Map(
        "apply_to_project" -> "|cc",
        "attr" -> "id"))

    // Import vertices as vertices
    {
      val project = eg
        .box("Import vertices", Map(
          "id_attr" -> "new_id"))
        .project
      assert(project.vertexSet.rdd.count == 4)
      assert(project.edgeBundle == null)
      val vAttrs = project.vertexAttributes.toMap
      // 6 original +1 new_id
      assert(vAttrs.size == 7)
      assert(vAttrs("id") == vAttrs("new_id"))
      assert(
        vAttrs("name").rdd.map(_._2).collect.toSet == Set("Adam", "Eve", "Bob", "Isolated Joe"))
    }

    // Import edges as vertices
    {
      val project = eg
        .box("Take edges as vertices")
        .box("Import vertices", Map(
          "id_attr" -> "id"))
        .project
      assert(project.vertexSet.rdd.count == 4)
      assert(project.edgeBundle == null)
      val vAttrs = project.vertexAttributes.toMap
      // 2 original edge attr, 2 * 6 src and dst attributes + 1 id
      assert(vAttrs.size == 15)
      assert(vAttrs("edge_weight").rdd.map(_._2).collect.toSet == Set(1.0, 2.0, 3.0, 4.0))
      assert(
        vAttrs("src_name").runtimeSafeCast[String].rdd.map(_._2).collect.toSeq.sorted ==
          Seq("Adam", "Bob", "Bob", "Eve"))
      assert(
        vAttrs("dst_name").runtimeSafeCast[String].rdd.map(_._2).collect.toSeq.sorted ==
          Seq("Adam", "Adam", "Eve", "Eve"))
    }

    // Import belongs to as edges
    {
      val project = eg
        .box("Take segmentation links as base project", Map(
          "apply_to_project" -> "|cc"))
        .box("Import vertices and edges from a single table", Map(
          "src" -> "base_name",
          "dst" -> "segment_id"))
        .project
      // 4 nodes in 2 segments
      assert(project.vertexSet.rdd.count == 6)
      assert(project.edgeBundle.rdd.count == 4)
      val vAttrs = project.vertexAttributes.toMap
      val eAttrs = project.edgeAttributes.toMap
      // id, stringId
      assert(vAttrs.size == 2)
      // 6 from base vertices, 2 from connected components
      assert(eAttrs.size == 8)

      val baseName = eAttrs("base_name").runtimeSafeCast[String].rdd
      val segmentId = eAttrs("segment_id").runtimeSafeCast[String].rdd
      val componentMap = baseName.sortedJoin(segmentId).values.collect.toMap
      assert(componentMap.size == 4)
      assert(componentMap("Adam") == componentMap("Eve"))
      assert(componentMap("Adam") == componentMap("Bob"))
      assert(componentMap("Adam") != componentMap("Isolated Joe"))
      assert(
        vAttrs("stringID").rdd.map(_._2).collect.toSet ==
          Set("Adam", "Eve", "Bob", "Isolated Joe", "0", "3"))
    }
  }

  test("Import edges for existing vertices") {
    val edges = importSeq(Seq("src", "dst", "value"), Seq(
      ("Adam", "Eve", "value1"),
      ("Isolated Joe", "Bob", "value2"),
      ("Eve", "Alice", "value3")))
    // The string "Alice" in the last row does not match any vertices in the example graph.
    // Therefore we expect it to be discarded.
    val project = box("Create example graph")
      .box("Import edges for existing vertices", Map(
        "attr" -> "name",
        "src" -> "src",
        "dst" -> "dst"), Seq(edges))
      .project
    assert(Seq(Edge(0, 1), Edge(3, 2)) ==
      project.edgeBundle.rdd.collect.toSeq.map(_._2))
    val valueAttr = project.edgeBundle.rdd
      .join(project.edgeAttributes("value").runtimeSafeCast[String].rdd)
      .values
    assert(Seq((Edge(0, 1), "value1"), (Edge(3, 2), "value2")) ==
      valueAttr.collect.toSeq.sorted)
  }

  test("Import vertex attributes") {
    val attrs = importSeq(Seq("row_id", "value"), Seq(
      ("Adam", "value1"),
      ("Isolated Joe", "value2"),
      ("Alice", "value3")))
    // The string "Alice" in the last row does not match any vertices in the example graph.
    // Therefore we expect it to be discarded.
    val project = box("Create example graph")
      .box("Import vertex attributes", Map(
        "id_attr" -> "name",
        "id_column" -> "row_id",
        "prefix" -> "imported"), Seq(attrs))
      .project
    val valueAttr = project.vertexAttributes("imported_value").runtimeSafeCast[String].rdd
    assert(Seq((0, "value1"), (3, "value2")) == valueAttr.collect.toSeq.sorted)
  }

  test("Import edge attributes") {
    val attrs = importSeq(Seq("row_id", "value"), Seq(
      ("Adam loves Eve", "value1"),
      ("Bob envies Adam", "value2"),
      ("Squirrell loves Peanuts", "value3")))
    // The last row does not match any edges in the example graph.
    // Therefore we expect it to be discarded.
    val project = box("Create example graph")
      .box("Import edge attributes", Map(
        "id_attr" -> "comment",
        "id_column" -> "row_id",
        "prefix" -> "imported"), Seq(attrs))
      .project
    val valueAttr = project.edgeAttributes("imported_value").runtimeSafeCast[String].rdd
    assert(Seq((0, "value1"), (2, "value2")) == valueAttr.collect.toSeq.sorted)
  }

  test("Attributes are not overwritten during import") {
    val table = importSeq(Seq("id", "name", "age"), Seq(
      ("1", "Bob", "12"),
      ("2", "Eve", "23"),
      ("3", "Chris", "34")))
    val edgeTable = importSeq(Seq("new_comment", "weight"), Seq(
      ("Adam loves Eve", "1.0")))

    val ex = intercept[java.lang.AssertionError] {
      box("Create example graph")
        .box("Convert vertex attribute to string", Map(
          "attr" -> "id"))
        .box("Import vertex attributes", Map(
          "id_attr" -> "id",
          "id_column" -> "id"), Seq(table))
        .project
    }
    assert(ex.getMessage.contains(
      "Cannot import column `id`. Attribute already exists."))

    val ex2 = intercept[java.lang.AssertionError] {
      box("Create example graph")
        .box("Import edge attributes", Map(
          "id_attr" -> "comment",
          "id_column" -> "new_comment"), Seq(edgeTable))
        .project
    }
    assert(ex2.getMessage.contains(
      "Cannot import column `weight`. Attribute already exists."))
  }
}
