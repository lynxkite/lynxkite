package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class MergeTwoAttributesOperationTest extends OperationsTestBase {
  test("Merge vertex two attributes") {
    run("Create example graph")
    // The unification is used everywhere, I'm just worried about the type equality check.
    intercept[java.lang.AssertionError] {
      run("Merge two vertex attributes", Map("name" -> "x", "attr1" -> "name", "attr2" -> "age"))
      enforceComputation
    }
    run("Merge two vertex attributes", Map("name" -> "x", "attr1" -> "name", "attr2" -> "gender"))
  }
}

