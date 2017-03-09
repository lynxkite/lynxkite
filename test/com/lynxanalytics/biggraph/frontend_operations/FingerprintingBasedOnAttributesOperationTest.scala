package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class FingerprintingBasedOnAttributesOperationTest extends OperationsTestBase {
  test("Fingerprinting based on attributes") {
    run("Import vertices", Map(
      "table" -> importCSV("OPERATIONSTEST$/fingerprint-100-vertices.csv"),
      "id_attr" -> "delete me"))
    run("Import edges for existing vertices", Map(
      "table" -> importCSV("OPERATIONSTEST$/fingerprint-100-edges.csv"),
      "attr" -> "id",
      "src" -> "src",
      "dst" -> "dst"))
    // Turn empty strings into "undefined".
    run("Derive vertex attribute", Map(
      "output" -> "email",
      "type" -> "string",
      "expr" -> "email ? email : undefined"))
    run("Derive vertex attribute", Map(
      "output" -> "name",
      "type" -> "string",
      "expr" -> "name ? name : undefined"))
    run("Fingerprint based on attributes", Map(
      "leftName" -> "email",
      "rightName" -> "name",
      "mo" -> "1",
      "ms" -> "0.5"))
    assert(project.scalars("fingerprinting matches found").value == 9)
    run("Discard edges")
    run("Connect vertices on attribute", Map("fromAttr" -> "email", "toAttr" -> "email"))
    assert(project.scalars("edge_count").value == 18)
    assert(project.scalars("vertex_count").value == 109)
    run("Merge vertices by attribute", Map(
      "key" -> "name",
      "aggregate_email" -> "",
      "aggregate_id" -> "",
      "aggregate_name" -> "",
      "aggregate_delete me" -> "",
      "aggregate_email similarity score" -> "",
      "aggregate_name similarity score" -> ""))
    assert(project.scalars("vertex_count").value == 100)
  }
}
