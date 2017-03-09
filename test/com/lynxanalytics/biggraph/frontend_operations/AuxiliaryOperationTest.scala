// These tests check some lower level infrastructure beneath
// various operations. So, there is no single operation this class
// is revolving around.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers._

class AuxiliaryOperationTest extends OperationsTestBase {

  test("Optional and mandatory parameters work") {
    run("Create example graph")
    run("Aggregate edge attribute to vertices", Map(
      "prefix" -> "incoming",
      "direction" -> "incoming edges",
      // "aggregate-comment" -> "", This is now optional
      "aggregate-weight" -> "sum"))
    intercept[java.lang.AssertionError] {
      run("Aggregate edge attribute to vertices", Map(
        "prefix" -> "incoming",
        // "direction" -> "incoming edges", But this is not
        "aggregate-comment" -> "",
        "aggregate-weight" -> "sum"))
    }
  }
}

