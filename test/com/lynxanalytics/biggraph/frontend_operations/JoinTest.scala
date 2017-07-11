package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class JoinTest extends OperationsTestBase {
  test("Simple vertex attribute join works") {
    val root = box("Create example graph")
    val source = root
      .box(
        "Add constant vertex attribute",
        Map("name" -> "seven", "value" -> "7", "type" -> "Double"))
    val target = root
    val project = box("Project rejoin", Map("attrs" -> "seven"), Seq(target, source)).project

    val values = project.vertexAttributes("seven").rdd.collect.toMap.values.toSeq

    assert(values == Seq(7, 7, 7, 7))
  }

  test("Simple edge attribute join works") {
    val root = box("Create example graph")
    val source = root
      .box(
        "Add constant edge attribute",
        Map("name" -> "eight", "value" -> "8", "type" -> "Double"))
    val target = root
    val project = box(
      "Project rejoin",
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
        "Segment by Double attribute",
        Map(
          "name" -> "bucketing",
          "attr" -> "age",
          "interval_size" -> "1",
          "overlap" -> "no"
        ))
      .box(
        "Create random edges",
        Map(
          "apply_to_project" -> "|bucketing",
          "degree" -> "10",
          "seed" -> "31415"
        ))
      .box(
        "Add constant edge attribute",
        Map(
          "apply_to_project" -> "|bucketing",
          "name" -> "ten",
          "value" -> "10",
          "type" -> "Double"))
    val project = box("Project rejoin",
      Map(
        "apply_to_target" -> "",
        "apply_to_source" -> "",
        "segs" -> "bucketing"
      ), Seq(target, source)
    ).project

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

    val project = box("Project rejoin",
      Map(
        "apply_to_target" -> "!edges",
        "apply_to_source" -> "",
        "attrs" -> "dst_name,dst_gender"
      ), Seq(target, source)
    ).project

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
      .box("Filter by attributes",
        Map("filterva_age" -> "> -10", // Dummy segmentation
          "filterva_gender" -> "", "filterva_id" -> "", "filterva_income" -> "",
          "filterva_location" -> "", "filterva_name" -> "", "filterea_comment" -> "",
          "filterea_weight" -> ""))

      .box("Filter by attributes",
        Map("filterva_age" -> "> 40", // Keep only Bob
          "filterva_gender" -> "", "filterva_id" -> "", "filterva_income" -> "",
          "filterva_location" -> "", "filterva_name" -> "", "filterea_comment" -> "",
          "filterea_weight" -> ""))
      .box(
        "Add constant vertex attribute",
        Map("name" -> "ten", "value" -> "10", "type" -> "Double"))

    val project = box(
      "Project rejoin",
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
      .box("Filter by attributes",
        Map("filterva_age" -> "",
          "filterva_gender" -> "", "filterva_id" -> "", "filterva_income" -> "",
          "filterva_location" -> "", "filterva_name" -> "", "filterea_comment" -> "",
          "filterea_weight" -> ">-1"))

      .box("Filter by attributes",
        Map("filterva_age" -> "",
          "filterva_gender" -> "", "filterva_id" -> "", "filterva_income" -> "",
          "filterva_location" -> "", "filterva_name" -> "", "filterea_comment" -> "",
          "filterea_weight" -> ">1"))
      .box(
        "Add constant edge attribute",
        Map("name" -> "ten", "value" -> "10", "type" -> "Double"))

    val project = box(
      "Project rejoin",
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
        .box("Add constant edge attribute",
          Map("name" -> "const1", "value" -> "1", "type" -> "Double"))
    val target =
      root.box("Filter by attributes",
        Map("filterva_age" -> "",
          "filterva_gender" -> "", "filterva_id" -> "", "filterva_income" -> "",
          "filterva_location" -> "", "filterva_name" -> "", "filterea_comment" -> "",
          "filterea_weight" -> ">2"))
    val project = box("Project rejoin",
      Map(
        "apply_to_target" -> "!edges",
        "apply_to_source" -> "!edges",
        "attrs" -> "const1"
      ), Seq(target, source)
    ).project

    val joinedAttributes = project.edgeAttributes("const1")
      .rdd.collect.toMap.values.toList.map(_.asInstanceOf[Double])
    assert(joinedAttributes == List(1.0, 1.0))
  }

  test("Segmentation edges can be joined back") {
    val numVertices = 50
    val numEdges = numVertices * (numVertices - 1)
    val root = box("Create vertices", Map("size" -> s"$numVertices"))
      .box("Convert vertex attribute to Double",
        Map("attr" -> "ordinal"))
      .box("Segment by Double attribute",
        Map("name" -> "seg", "attr" -> "ordinal", "interval_size" -> "1", "overlap" -> "no"))
      .box("Add constant vertex attribute",
        Map("name" -> "const1", "value" -> "1", "type" -> "Double", "apply_to_project" -> "|seg"))
      .box("Connect vertices on attribute",
        Map("fromAttr" -> "const1", "toAttr" -> "const1", "apply_to_project" -> "|seg"))
    val target =
      root.box("Add random edge attribute",
        Map("name" -> "random",
          "apply_to_project" -> "|seg",
          "dist" -> "Standard Uniform",
          "seed" -> "32421341"))
        .box("Filter by attributes",
          Map(
            "filterva_id" -> "",
            "filterea_random" -> ">0.5",
            "apply_to_project" -> "|seg"))
    val source = root.box("Take segmentation as base project", Map("apply_to_project" -> "|seg"))
      .box("Add random edge attribute",
        Map("name" -> "random2",
          "dist" -> "Standard Uniform",
          "seed" -> "10101221"))
      .box("Filter by attributes",
        Map(
          "filterva_bottom" -> "",
          "filterva_id" -> "",
          "filterva_const1" -> "",
          "filterva_size" -> "",
          "filterva_top" -> "",
          "filterea_random2" -> ">0.5"))

    val join = box("Project rejoin",
      Map(
        "apply_to_target" -> "|seg!edges",
        "apply_to_source" -> "!edges",
        "attrs" -> "random2"
      ), Seq(target, source))

    val random2Defined =
      join.box("SQL1",
        Map("sql" -> "select COUNT(*) from `seg|edges` where edge_random2 is not null"))
        .table.df.collect.take(1).head.getLong(0)

    import org.scalactic.TolerantNumerics
    implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(0.01)
    assert(random2Defined.toDouble / numEdges.toDouble === 0.25)

    val random2NotDefined =
      join.box("SQL1",
        Map("sql" -> "select COUNT(*) from `seg|edges` where edge_random2 is null"))
        .table.df.collect.take(1).head.getLong(0)

    assert(random2NotDefined.toDouble / numEdges.toDouble === 0.25)
  }

  test("Complex test") {

    val root =
      box("Create vertices", Map("size" -> "10"))
        .box("Add constant vertex attribute",
          Map("name" -> "const1", "value" -> "1", "type" -> "Double"))
        .box("Connect vertices on attribute", Map("fromAttr" -> "const1", "toAttr" -> "const1"))
        .box("Convert vertex attribute to Double",
          Map("attr" -> "ordinal"))
    // Now split, filter, edges to vertices, and then filter again.
    val source = root
      .box("Derive edge attribute",
        Map("type" -> "Double", "output" -> "keep1",
          "expr" -> "src$ordinal % 2 === dst$ordinal % 2"))
      .box("Filter by attributes",
        Map("filterva_const1" -> "",
          "filterva_ordinal" -> "",
          "filterea_keep1" -> ">0.5"))
      .box("Take edges as vertices")
      .box("Derive vertex attribute",
        Map("type" -> "Double", "output" -> "keep2",
          "expr" -> "dst_ordinal < src_ordinal"))
      .box("Filter by attributes",
        Map(
          "filterva_dst_ordinal" -> "",
          "filterva_src_ordinal" -> "",
          "filterva_dst_const1" -> "",
          "filterva_src_const1" -> "",
          "filterva_edge_keep1" -> "",
          "filterva_keep2" -> ">0.5"
        )
      )
      .box("Derive vertex attribute",
        Map("type" -> "String", "output" -> "newattr",
          "expr" -> "'' + dst_ordinal + '_' + src_ordinal"))
    // The target should also undergo some filtering:
    val target = root
      .box("Filter by attributes",
        Map("filterva_const1" -> "",
          "filterva_ordinal" -> "< 8"))
      .box("Filter by attributes",
        Map("filterva_const1" -> "",
          "filterva_ordinal" -> "> 2"))
    val project = box("Project rejoin",
      Map(
        "apply_to_target" -> "!edges",
        "apply_to_source" -> "",
        "attrs" -> "newattr"
      ), Seq(target, source)
    ).project

    val newEdgeAttributes = project.edgeAttributes("newattr")
      .rdd.collect.toMap.values.toList.map(_.asInstanceOf[String]).sorted
    assert(newEdgeAttributes == List("3_5", "3_7", "4_6", "5_7"))

  }
}

