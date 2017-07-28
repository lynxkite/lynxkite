package com.lynxanalytics.biggraph.frontend_operations

class RenameEdgeAttributesTest extends OperationsTestBase {
  test("Rename edge attributes") {
    val project = box("Create example graph")
      .box("Rename edge attributes",
        Map("change_comment" -> "new_comment", "change_weight" -> "")).project
    assert(project.edgeAttributes.contains("new_comment"))
    assert(!project.edgeAttributes.contains("comment"))
    assert(!project.edgeAttributes.contains("weight"))
  }
}
