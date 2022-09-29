package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.graph_api.Scripting._

class MergeTwoAttributesOperationTest extends OperationsTestBase {
  test("Merge vertex two attributes") {
    val a = box("Create example graph")
    // The unification is used everywhere, I'm just worried about the type equality check.
    intercept[java.lang.AssertionError] {
      val b = a.box("Merge two vertex attributes", Map("name" -> "x", "attr1" -> "name", "attr2" -> "age"))
      b.project
    }
    val c = a.box("Merge two vertex attributes", Map("name" -> "x", "attr1" -> "name", "attr2" -> "gender"))
    c.project
  }
}
