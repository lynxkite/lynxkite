package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import org.apache.spark.sql.Row

class JoinTest extends OperationsTestBase {
  test("Simple vertex attribute join works") {
    val root = box("Create example graph")
    val source = root
      .box(
        "Add constant vertex attribute",
        Map("name" -> "seven", "value" -> "7", "type" -> "number"))
    val target = root
    val project = box("Graph rejoin", Map("attrs" -> "seven"), Seq(target, source)).project

    val values = project.vertexAttributes("seven").rdd.collect.toMap.values.toSeq

    assert(values == Seq(7, 7, 7, 7))
  }

  test("Simple edge attribute join works") {
    val root = box("Create example graph")
    val source = root
      .box(
        "Add constant edge attribute",
        Map("name" -> "eight", "value" -> "8", "type" -> "number"))
    val target = root
    val project = box(
      "Graph rejoin",
      Map(
        "apply_to_target" -> "!edges",
        "apply_to_source" -> "!edges",
        "attrs" -> "eight"), Seq(target, source)).project

    val values = project.edgeAttributes("eight").rdd.collect.toMap.values.toSeq

    assert(values == Seq(8, 8, 8, 8))
  }

  test("Segmentations can be joined") {
    val root = box("Create example graph")
    val target = root
    val source = root
      .box(
        "Segment by numeric attribute",
        Map(
          "name" -> "bucketing",
          "attr" -> "age",
          "interval_size" -> "1",
          "overlap" -> "no"))
      .box(
        "Create random edges",
        Map(
          "apply_to_graph" -> ".bucketing",
          "degree" -> "10",
          "seed" -> "31415"))
      .box(
        "Add constant edge attribute",
        Map(
          "apply_to_graph" -> ".bucketing",
          "name" -> "ten",
          "value" -> "10",
          "type" -> "number"))
    val project = box(
      "Graph rejoin",
      Map(
        "apply_to_target" -> "",
        "apply_to_source" -> "",
        "segs" -> "bucketing"), Seq(target, source)).project

    val segm = project.existingSegmentation("bucketing")
    val values = segm.edgeAttributes("ten").rdd.collect.toMap.values.toSeq
    val tens = values.count(_ == 10.0)
    assert(tens > 0 && tens == values.size)
  }

  test("Vertex attributes joined to edge attributes") {
    val root = box("Create example graph")
    val target = root
    val source = root
      .box(
        "Take edges as vertices",
        Map())

    val project = box(
      "Graph rejoin",
      Map(
        "apply_to_target" -> "!edges",
        "apply_to_source" -> "",
        "attrs" -> "dst_name,dst_gender"), Seq(target, source)).project

    val names = project.edgeAttributes("dst_name")
      .rdd.collect.toMap.values.toList.map(_.asInstanceOf[String]).sorted
    val genders = project.edgeAttributes("dst_gender")
      .rdd.collect.toMap.values.toList.map(_.asInstanceOf[String]).sorted
    assert(names == List("Adam", "Adam", "Eve", "Eve"))
    assert(genders == List("Female", "Female", "Male", "Male"))
  }

  test("Filtering vertex attributes") {
    val root = box("Create example graph")
    val target = root
    val source = root
      .box(
        "Filter by attributes",
        Map("filterva_age" -> "> -10")) // Dummy segmentation

      .box(
        "Filter by attributes",
        Map("filterva_age" -> "> 40")) // Keep only Bob
      .box(
        "Add constant vertex attribute",
        Map("name" -> "ten", "value" -> "10", "type" -> "number"))

    val project = box(
      "Graph rejoin",
      Map(
        "attrs" -> "name,ten"),
      Seq(target, source)).project

    assert(project.vertexAttributes("name").rdd.collect.toMap.values.toSeq == Seq("Bob"))
    assert(project.vertexAttributes("ten").rdd.collect.toMap.values.toSeq == Seq(10))
    assert(project.vertexAttributes("age").rdd.collect.toMap.values.toSeq
      .asInstanceOf[Seq[Double]].sorted == Seq(2.0, 18.2, 20.3, 50.3))
  }

  test("Filtering edge attributes") {
    val root = box("Create example graph")
    val target = root
    val source = root
      .box(
        "Filter by attributes",
        Map("filterea_weight" -> ">-1"))
      .box(
        "Filter by attributes",
        Map("filterva_age" -> "", "filterea_weight" -> ">1"))
      .box(
        "Add constant edge attribute",
        Map("name" -> "ten", "value" -> "10", "type" -> "number"))

    val project = box(
      "Graph rejoin",
      Map(
        "apply_to_target" -> "!edges",
        "apply_to_source" -> "!edges",
        "attrs" -> "ten"),
      Seq(target, source)).project

    assert(project.edgeAttributes("ten").rdd.collect.toMap.values.toSeq.toList == List(10.0, 10, 10.0))
  }

  test("Bring attributes from broader to narrower set") {
    val root = box("Create example graph")
    val source =
      root.box("Add constant edge attribute")
        .box(
          "Add constant edge attribute",
          Map("name" -> "const1", "value" -> "1", "type" -> "number"))
    val target =
      root.box(
        "Filter by attributes",
        Map("filterea_weight" -> ">2"))
    val project = box(
      "Graph rejoin",
      Map(
        "apply_to_target" -> "!edges",
        "apply_to_source" -> "!edges",
        "attrs" -> "const1"), Seq(target, source)).project

    val joinedAttributes = project.edgeAttributes("const1")
      .rdd.collect.toMap.values.toList.map(_.asInstanceOf[Double])
    assert(joinedAttributes == List(1.0, 1.0))
  }

  test("Segmentation edges can be joined back") {
    val numVertices = 50
    val numEdges = numVertices * (numVertices - 1)
    val root = box("Create vertices", Map("size" -> s"$numVertices"))
      .box(
        "Segment by numeric attribute",
        Map("name" -> "seg", "attr" -> "ordinal", "interval_size" -> "1", "overlap" -> "no"))
      .box(
        "Add constant vertex attribute",
        Map("name" -> "const1", "value" -> "1", "type" -> "number", "apply_to_graph" -> ".seg"))
      .box(
        "Connect vertices on attribute",
        Map("fromAttr" -> "const1", "toAttr" -> "const1", "apply_to_graph" -> ".seg"))
    val target =
      root.box(
        "Add random edge attribute",
        Map(
          "name" -> "random",
          "apply_to_graph" -> ".seg",
          "dist" -> "Standard Uniform",
          "seed" -> "32421341"))
        .box(
          "Filter by attributes",
          Map(
            "filterea_random" -> ">0.5",
            "apply_to_graph" -> ".seg"))
    val source = root.box("Take segmentation as base graph", Map("apply_to_graph" -> ".seg"))
      .box(
        "Add random edge attribute",
        Map(
          "name" -> "random2",
          "dist" -> "Standard Uniform",
          "seed" -> "10101221"))
      .box(
        "Filter by attributes",
        Map("filterea_random2" -> ">0.5"))

    val join = box(
      "Graph rejoin",
      Map(
        "apply_to_target" -> ".seg!edges",
        "apply_to_source" -> "!edges",
        "attrs" -> "random2"), Seq(target, source))

    val random2Defined =
      join.box(
        "SQL1",
        Map("sql" -> "select COUNT(*) from `seg.edges` where edge_random2 is not null"))
        .table.df.collect.take(1).head.getLong(0)

    import org.scalactic.TolerantNumerics
    implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(0.01)
    assert(random2Defined.toDouble / numEdges.toDouble === 0.25)

    val random2NotDefined =
      join.box(
        "SQL1",
        Map("sql" -> "select COUNT(*) from `seg.edges` where edge_random2 is null"))
        .table.df.collect.take(1).head.getLong(0)

    assert(random2NotDefined.toDouble / numEdges.toDouble === 0.25)
  }

  test("Complex test") {
    val root =
      box("Create vertices", Map("size" -> "10"))
        .box(
          "Add constant vertex attribute",
          Map("name" -> "const1", "value" -> "1", "type" -> "number"))
        .box("Connect vertices on attribute", Map("fromAttr" -> "const1", "toAttr" -> "const1"))
    // Now split, filter, edges to vertices, and then filter again.
    val source = root
      .box(
        "Derive edge attribute",
        Map(
          "output" -> "keep1",
          "expr" -> "if (src$ordinal % 2 == dst$ordinal % 2) 1.0 else 0.0"))
      .box(
        "Filter by attributes",
        Map("filterea_keep1" -> ">0.5"))
      .box("Take edges as vertices")
      .box(
        "Derive vertex attribute",
        Map(
          "output" -> "keep2",
          "expr" -> "if (dst_ordinal < src_ordinal) 1.0 else 0.0"))
      .box(
        "Filter by attributes",
        Map("filterva_keep2" -> ">0.5"))
      .box(
        "Derive vertex attribute",
        Map(
          "output" -> "newattr",
          "expr" -> "dst_ordinal.toString + \"_\" + src_ordinal.toString"))
    // The target should also undergo some filtering:
    val target = root
      .box(
        "Filter by attributes",
        Map("filterva_ordinal" -> "< 8"))
      .box(
        "Filter by attributes",
        Map("filterva_ordinal" -> "> 2"))
    val project = box(
      "Graph rejoin",
      Map(
        "apply_to_target" -> "!edges",
        "apply_to_source" -> "",
        "attrs" -> "newattr"), Seq(target, source)).project

    val newEdgeAttributes = project.edgeAttributes("newattr")
      .rdd.collect.toMap.values.toList.map(_.asInstanceOf[String]).sorted
    assert(newEdgeAttributes == List("3.0_5.0", "3.0_7.0", "4.0_6.0", "5.0_7.0"))

  }

  // target has no Bob, source has no Isolated Joe
  def getTargetSource(): (TestBox, TestBox) = {
    val root = box("Create example graph")
    val target = root.box(
      "Filter by attributes",
      Map("filterva_age" -> "> -1")) // Make chain longer
      .box("Filter by attributes", Map("filterva_age" -> "<40")) // Discard Bob
    val source = root.box(
      "Filter by attributes",
      Map("filterva_age" -> "> -2")) // Make chain longer
      .box("Filter by attributes", Map("filterva_age" -> ">10")) // Discard Joe
    (target, source)
  }

  test("Segmentations work with filters") {
    val (target, sourceRoot) = getTargetSource()
    val source = sourceRoot.box(
      "Segment by numeric attribute",
      Map(
        "name" -> "bucketing",
        "attr" -> "age",
        "interval_size" -> "1",
        "overlap" -> "no"))
      .box(
        "Aggregate to segmentation",
        Map(
          "apply_to_graph" -> ".bucketing",
          "aggregate_name" -> "first"))
      .box(
        "Rename vertex attributes",
        Map(
          "apply_to_graph" -> ".bucketing",
          "change_name_first" -> "name"))
    val join = box(
      "Graph rejoin",
      Map(
        "segs" -> "bucketing"), Seq(target, source))

    val result =
      join.box(
        "Aggregate from segmentation",
        Map(
          "apply_to_graph" -> ".bucketing",
          "aggregate_name" -> "first"))
        .box(
          "SQL1",
          Map("sql" -> "select name,bucketing_name_first from `vertices`"))
        .table.df.collect.toList.sortBy(_.getString(0))
    assert(result == List(Row("Adam", "Adam"), Row("Eve", "Eve"), Row("Isolated Joe", null)))
  }

  test("Edges can be copied over") {
    val (target, sourceRoot) = getTargetSource()
    val source = sourceRoot.box("Replace edges with triadic closure", Map())
      .box(
        "Derive edge attribute",
        Map(
          "output" -> "edge_attr",
          "expr" -> "src$name + '_' + dst$name"))
    val result = box(
      "Graph rejoin",
      Map(
        "edge" -> "yes"), Seq(target, source))
      .box(
        "SQL1",
        Map("sql" -> "select edge_edge_attr from `edges`"))
      .table.df.collect.toList.sortBy(_.getString(0))
    assert(result == List(Row("Adam_Adam"), Row("Eve_Eve")))
  }

}
