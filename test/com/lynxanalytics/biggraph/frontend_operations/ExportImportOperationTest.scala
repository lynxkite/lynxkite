package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers.DirectoryEntry
import com.lynxanalytics.biggraph.graph_api.Edge
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class ExportImportOperationTest extends OperationsTestBase {
  test("Imports from implicit tables") {
    val eg = box("Create example graph")
      .box(
        "Find connected components",
        Map(
          "name" -> "cc",
          "directions" -> "ignore directions"))
      .box(
        "Convert vertex attribute to String",
        Map(
          "apply_to_graph" -> ".cc",
          "attr" -> "id"))

    // Import vertices as vertices
    {
      val project = eg
        .box("Use table as vertices")
        .project
      assert(project.vertexSet.rdd.count == 4)
      assert(project.edgeBundle == null)
      val vAttrs = project.vertexAttributes.toMap
      assert(vAttrs.size == 6)
      assert(
        vAttrs("name").rdd.map(_._2).collect.toSet == Set("Adam", "Eve", "Bob", "Isolated Joe"))
    }

    // Import edges as vertices
    {
      val project = eg
        .box("Take edges as vertices")
        .box("Use table as vertices")
        .project
      assert(project.vertexSet.rdd.count == 4)
      assert(project.edgeBundle == null)
      val vAttrs = project.vertexAttributes.toMap
      // 2 original edge attr, 2 * 6 src and dst attributes
      assert(vAttrs.size == 14)
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
        .box(
          "Take segmentation links as base graph",
          Map(
            "apply_to_graph" -> ".cc"))
        .box(
          "Use table as graph",
          Map(
            "src" -> "base_name",
            "dst" -> "segment_id"))
        .project
      // 4 nodes in 2 segments
      assert(project.vertexSet.rdd.count == 6)
      assert(project.edgeBundle.rdd.count == 4)
      val vAttrs = project.vertexAttributes.toMap
      val eAttrs = project.edgeAttributes.toMap
      // stringId
      assert(vAttrs.size == 1)
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
        vAttrs("stringId").rdd.map(_._2).collect.toSet ==
          Set("Adam", "Eve", "Bob", "Isolated Joe", "0", "3"))
    }
  }

  test("Use table as edges") {
    val edges = importSeq(
      Seq("src", "dst", "string", "number"),
      Seq(
        ("Adam", "Eve", "value1", 1L),
        ("Isolated Joe", "Bob", "value2", 2L),
        ("Eve", "Alice", "value3", 3L)))
    // The string "Alice" in the last row does not match any vertices in the example graph.
    // Therefore we expect it to be discarded.
    val project = box("Create example graph")
      .box(
        "Use table as edges",
        Map(
          "attr" -> "name",
          "src" -> "src",
          "dst" -> "dst"),
        Seq(edges))
      .project
    assert(Seq(Edge(0, 1), Edge(3, 2)) ==
      project.edgeBundle.rdd.collect.toSeq.map(_._2))
    val stringAttr = project.edgeBundle.rdd
      .join(project.edgeAttributes("string").runtimeSafeCast[String].rdd)
      .values
    assert(Seq((Edge(0, 1), "value1"), (Edge(3, 2), "value2")) ==
      stringAttr.collect.toSeq.sorted)
    val numberAttr = project.edgeBundle.rdd
      .join(project.edgeAttributes("number").runtimeSafeCast[Double].rdd)
      .values
    assert(Seq((Edge(0, 1), 1.0), (Edge(3, 2), 2.0)) ==
      numberAttr.collect.toSeq.sorted)
  }

  test("Use table as graph with Ints") {
    val edges = importSeq(Seq("src", "dst"), Seq((1, 2), (2, 3), (3, 1)))
    val project = edges.box("Use table as graph", Map("src" -> "src", "dst" -> "dst")).project
    assert(3 == project.edgeBundle.rdd.collect.toSeq.length)
  }

  test("Use table as vertex attributes") {
    val attrs = importSeq(
      Seq("row_id", "value"),
      Seq(
        ("Adam", "value1"),
        ("Isolated Joe", "value2"),
        ("Alice", "value3")))
    // The string "Alice" in the last row does not match any vertices in the example graph.
    // Therefore we expect it to be discarded.
    val project = box("Create example graph")
      .box(
        "Use table as vertex attributes",
        Map(
          "id_attr" -> "name",
          "id_column" -> "row_id",
          "prefix" -> "imported"),
        Seq(attrs))
      .project
    val valueAttr = project.vertexAttributes("imported_value").runtimeSafeCast[String].rdd
    assert(Seq((0, "value1"), (3, "value2")) == valueAttr.collect.toSeq.sorted)
  }

  test("Use table as edge attributes") {
    val attrs = importSeq(
      Seq("row_id", "value"),
      Seq(
        ("Adam loves Eve", "value1"),
        ("Bob envies Adam", "value2"),
        ("Squirrell loves Peanuts", "value3")))
    // The last row does not match any edges in the example graph.
    // Therefore we expect it to be discarded.
    val project = box("Create example graph")
      .box(
        "Use table as edge attributes",
        Map(
          "id_attr" -> "comment",
          "id_column" -> "row_id",
          "prefix" -> "imported"),
        Seq(attrs))
      .project
    val valueAttr = project.edgeAttributes("imported_value").runtimeSafeCast[String].rdd
    assert(Seq((0, "value1"), (2, "value2")) == valueAttr.collect.toSeq.sorted)
  }

  test("Attributes are not overwritten during import") {
    val table = importSeq(
      Seq("id", "name", "age"),
      Seq(
        ("1", "Bob", "12"),
        ("2", "Eve", "23"),
        ("3", "Chris", "34")))
    val edgeTable = importSeq(
      Seq("new_comment", "weight"),
      Seq(
        ("Adam loves Eve", "1.0")))

    val ex = intercept[java.lang.AssertionError] {
      box("Create example graph")
        .box(
          "Use table as vertex attributes",
          Map(
            "if_exists" -> "Disallow this",
            "id_attr" -> "id",
            "id_column" -> "id"),
          Seq(table))
        .project
    }
    assert(ex.getMessage.contains(
      "Cannot import column `name`. Attribute already exists."))

    val ex2 = intercept[java.lang.AssertionError] {
      box("Create example graph")
        .box(
          "Use table as edge attributes",
          Map(
            "if_exists" -> "Disallow this",
            "id_attr" -> "comment",
            "id_column" -> "new_comment"),
          Seq(edgeTable))
        .project
    }
    assert(ex2.getMessage.contains(
      "Cannot import column `weight`. Attribute already exists."))
  }
}
