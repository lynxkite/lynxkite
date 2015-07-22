package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api.Scripting._

class ProjectUnionOperationTest extends OperationsTestBase {
  test("Project union") {
    run("Example Graph")
    val other = Project.fromPath("ExampleGraph2")
    project.copy(other)
    run("Rename vertex attribute", Map("from" -> "age", "to" -> "newage"), on = other)
    run("Rename edge attribute", Map("from" -> "comment", "to" -> "newcomment"), on = other)
    run("Union with another project", Map("other" -> "ExampleGraph2", "id-attr" -> "new_id"))

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
    run("New vertex set", Map("size" -> "10"))
    val other = Project.fromPath("Copy")
    project.copy(other)
    run("Union with another project", Map("other" -> "Copy", "id-attr" -> "new_id"))

    assert(project.vertexSet.rdd.count == 20)
    assert(project.edgeBundle == null)
  }

  test("Project union - useful error message (#1611)") {
    run("Example Graph")
    val other = Project.fromPath("ExampleGraph2")
    project.copy(other)
    run("Rename vertex attribute",
      Map("from" -> "age", "to" -> "newage"), on = other)
    run("Add constant vertex attribute",
      Map("name" -> "age", "value" -> "dummy", "type" -> "String"), on = other)

    val ex = intercept[java.lang.AssertionError] {
      run("Union with another project",
        Map("other" -> "ExampleGraph2", "id-attr" -> "new_id"))
    }
    assert(ex.getMessage.contains(
      "Attribute 'age' has conflicting types in the two projects: (Double and String)"))
  }
}
