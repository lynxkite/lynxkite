// These tests check some lower level infrastructure beneath
// various operations. So, there is no single operation this class
// is revolving around.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api.Scripting._

class AuxilaryOperationTest extends OperationsTestBase {

  test("Restore checkpoint after failing operation") {
    class Bug extends Exception("simulated bug")
    ops.register("Buggy op", new Operation(_, _, Operation.Category("Test", "test")) {
      def enabled = ???
      def parameters = List()
      def apply(params: Map[String, String]) = {
        project.vertexSet = null
        throw new Bug
      }
    })
    run("Example Graph")
    assert(project.vertexSet != null)
    try {
      run("Buggy op")
    } catch {
      case _: Bug =>
    }
    assert(project.vertexSet != null)
  }

  test("Optional and mandatory parameters work") {
    run("Example Graph")
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

