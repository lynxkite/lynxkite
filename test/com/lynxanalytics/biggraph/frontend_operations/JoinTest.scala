package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class JoinTest extends OperationsTestBase {
  test("Simple vertex attribute join works") {
    val root = box("Create example graph")
    val right = root
      .box(
        "Add constant vertex attribute",
        Map("name" -> "seven", "value" -> "7", "type" -> "Double"))
    val left = root
    val project = box("Join projects", Map("attrs" -> "seven"), Seq(left, right)).project

    val values = project.vertexAttributes("seven").rdd.collect.toMap.values.toSeq

    assert(values == Seq(7, 7, 7, 7))
  }

  test("Simple edge attribute join works") {
    val root = box("Create example graph")
    val right = root
      .box(
        "Add constant edge attribute",
        Map("name" -> "eight", "value" -> "8", "type" -> "Double"))
    val left = root
    val project = box(
      "Join projects",
      Map(
        "apply_to_a" -> "!edges",
        "apply_to_b" -> "!edges",
        "attrs" -> "eight"), Seq(left, right)).project

    val values = project.edgeAttributes("eight").rdd.collect.toMap.values.toSeq

    assert(values == Seq(8, 8, 8, 8))
  }

  test("Segmentations can be joined") {
    val root = box("Create example graph")
    val left = root
    val right = root
      .box(
        "Segment by Double attribute",
        Map(
          "name" -> "bucketing",
          "attr" -> "age",
          "interval_size" -> "1",
          "overlap" -> "no"
        ))
      .box(
        "Create random edge bundle",
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
    val project = box("Join projects",
      Map(
        "apply_to_a" -> "",
        "apply_to_b" -> "",
        "segs" -> "bucketing"
      ), Seq(left, right)
    ).project

    val segm = project.existingSegmentation("bucketing")
    val values = segm.edgeAttributes("ten").rdd.collect.toMap.values.toSeq
    val tens = values.count(_ == 10.0)
    assert(tens > 0 && tens == values.size)
  }

  test("Vertex attributes joined to edge attributes") {
    val root = box("Create example graph")
    val left = root
    val right = root
      .box(
        "Take edges as vertices",
        Map())

    val project = box("Join projects",
      Map(
        "apply_to_a" -> "!edges",
        "apply_to_b" -> "",
        "attrs" -> "dst_name,dst_gender"
      ), Seq(left, right)
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
    val left = root
    val right = root
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
      "Join projects",
      Map(
        "attrs" -> "name,ten"),
      Seq(left, right)).project

    assert(project.vertexAttributes("name").rdd.collect.toMap.values.toSeq == Seq("Bob"))
    assert(project.vertexAttributes("ten").rdd.collect.toMap.values.toSeq == Seq(10))
    assert(project.vertexAttributes("age").rdd.collect.toMap.values.toSeq
      .asInstanceOf[Seq[Double]].sorted == Seq(2.0, 18.2, 20.3, 50.3))
  }

}

