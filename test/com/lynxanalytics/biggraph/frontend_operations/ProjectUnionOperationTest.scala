package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class ProjectUnionOperationTest extends OperationsTestBase {
  test("Project union") {
    val a = box("Create example graph")
    val b = box("Create example graph")
      .box("Rename vertex attributes", Map("change_age" -> "newage"))
      .box("Rename edge attributes", Map("change_comment" -> "newcomment"))
    val union = box("Project union", Map("id_attr" -> "new_id"), Seq(a, b))
    val project = union.project

    assert(project.vertexSet.rdd.count == 8)

    assert(project.edgeBundle.rdd.count == 8)

    val vAttrs = project.vertexAttributes.toMap
    // 6 original +1 renamed +1 new_id
    assert(vAttrs.size == 8)
    val eAttrs = project.edgeAttributes.toMap
    // 2 original +1 renamed
    assert(eAttrs.size == 3)

    // Not renamed vertex attr is defined on all.
    assert(vAttrs("name").rdd.count == 8)
    // Renamed vertex attr is defined on half.
    assert(vAttrs("age").rdd.count == 4)
    assert(vAttrs("newage").rdd.count == 4)

    // Not renamed edge attr is defined on all.
    assert(eAttrs("weight").rdd.count == 8)
    // Renamed edge attr is defined on half.
    assert(eAttrs("comment").rdd.count == 4)
    assert(eAttrs("newcomment").rdd.count == 4)
  }

  test("Project union on vertex sets") {
    val a = box("Create vertices", Map("size" -> "10"))
    val b = box("Create vertices", Map("size" -> "10"))
    val union = box(
      "Project union",
      Map("id_attr" -> "new_id"), Seq(a, b))
    val project = union.project

    assert(project.vertexSet.rdd.count == 20)
    assert(project.edgeBundle == null)
  }

  test("Project union - useful error message (#1611)") {
    val a = box("Create example graph")
    val b = box("Create example graph")
      .box("Rename vertex attributes", Map("change_age" -> "newage"))
      .box("Add constant vertex attribute",
        Map("name" -> "age", "value" -> "dummy", "type" -> "String"))
    val ex = intercept[java.lang.AssertionError] {
      val union = box("Project union", Map("id_attr" -> "new_id"), Seq(a, b))
      union.project
    }
    assert(ex.getMessage.contains(
      "Attribute 'age' has conflicting types in the two projects: (Double and String)"))
  }
}
