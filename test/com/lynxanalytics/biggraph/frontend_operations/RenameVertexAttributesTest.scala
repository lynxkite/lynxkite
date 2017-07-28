package com.lynxanalytics.biggraph.frontend_operations

class RenameVertexAttributesTest extends OperationsTestBase {
  test("Rename vertex attributes") {
    val project = box("Create example graph")
      .box("Rename vertex attributes",
        Map("change_age" -> "new_age", "change_gender" -> "")).project
    assert(project.vertexAttributes.contains("new_age"))
    assert(!project.vertexAttributes.contains("age"))
    assert(!project.vertexAttributes.contains("gender"))
  }
}
