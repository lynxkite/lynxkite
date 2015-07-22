package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class FingerprintingBasedOnAttributesOperationTest extends OperationsTestBase {
  test("Fingerprinting based on attributes") {
    run("Import vertices from CSV files", Map(
      "files" -> "OPERATIONSTEST$/fingerprint-100-vertices.csv",
      "header" -> "id,email,name",
      "delimiter" -> ",",
      "id-attr" -> "delete me",
      "omitted" -> "",
      "filter" -> ""))
    run("Import edges for existing vertices from CSV files", Map(
      "files" -> "OPERATIONSTEST$/fingerprint-100-edges.csv",
      "header" -> "src,dst",
      "delimiter" -> ",",
      "attr" -> "id",
      "src" -> "src",
      "dst" -> "dst",
      "omitted" -> "",
      "filter" -> ""))
    // Turn empty strings into "undefined".
    run("Derived vertex attribute", Map(
      "output" -> "email",
      "type" -> "string",
      "expr" -> "email ? email : undefined"))
    run("Derived vertex attribute", Map(
      "output" -> "name",
      "type" -> "string",
      "expr" -> "name ? name : undefined"))
    run("Fingerprinting based on attributes", Map(
      "leftName" -> "email",
      "rightName" -> "name",
      "weights" -> "!no weight",
      "mrew" -> "0.0",
      "mo" -> "1",
      "ms" -> "0.5"))
    assert(project.scalars("fingerprinting matches found").value == 9)
    run("Discard edges")
    run("Connect vertices on attribute", Map("fromAttr" -> "email", "toAttr" -> "email"))
    assert(project.scalars("edge_count").value == 18)
    assert(project.scalars("vertex_count").value == 109)
    run("Merge vertices by attribute", Map(
      "key" -> "name",
      "aggregate-email" -> "",
      "aggregate-id" -> "",
      "aggregate-name" -> "",
      "aggregate-delete me" -> "",
      "aggregate-email similarity score" -> "",
      "aggregate-name similarity score" -> ""))
    assert(project.scalars("vertex_count").value == 100)
  }
}
