package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class FingerprintingBasedOnAttributesOperationTest extends OperationsTestBase {
  test("Fingerprinting based on attributes") {
    val vertices = importCSV("fingerprint-100-vertices.csv")
      .box("Import vertices")
    val edges = importCSV("fingerprint-100-edges.csv")
    val fingerprinted = box("Import edges for existing vertices", Map(
      "attr" -> "id",
      "src" -> "src",
      "dst" -> "dst"), Seq(vertices, edges))
      // Turn empty strings into None.
      .box("Derive vertex attribute", Map(
        "output" -> "email",
        "expr" -> "if (email.nonEmpty) Some(email) else None"))
      .box("Derive vertex attribute", Map(
        "output" -> "name",
        "expr" -> "if (name.nonEmpty) Some(name) else None"))
      .box("Fingerprint based on attributes", Map(
        "leftName" -> "email",
        "rightName" -> "name",
        "mo" -> "1",
        "ms" -> "0.5"))
    assert(fingerprinted.project.scalars("fingerprinting matches found").value == 9)
    val connected = fingerprinted
      .box("Discard edges")
      .box("Connect vertices on attribute", Map("fromAttr" -> "email", "toAttr" -> "email"))
    assert(connected.project.scalars("!edge_count").value == 18)
    assert(connected.project.scalars("!vertex_count").value == 109)
    val merged = connected.box("Merge vertices by attribute", Map("key" -> "name"))
    assert(merged.project.scalars("!vertex_count").value == 100)
  }
}
