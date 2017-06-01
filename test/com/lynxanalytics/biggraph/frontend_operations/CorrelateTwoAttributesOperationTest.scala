package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import org.scalactic.TolerantNumerics

class CorrelateTwoAttributesOperationTest extends OperationsTestBase {
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(0.0001)

  test("Aggregate edge attribute to vertices, all directions") {
    val project = box("Create example graph")
      .box("Correlate two attributes", Map(
        "attrA" -> "age",
        "attrB" -> "age"))
      .box("Correlate two attributes", Map(
        "attrA" -> "age",
        "attrB" -> "income")).project
    assert(project.scalars("correlation of age and age").value.asInstanceOf[Double] === 1.0)
    assert(project.scalars("correlation of age and income").value.asInstanceOf[Double] === 1.0)
  }
}

