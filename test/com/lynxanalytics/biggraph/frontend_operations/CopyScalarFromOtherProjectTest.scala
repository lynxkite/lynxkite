package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class CopyScalarFromOtherProjectTest extends OperationsTestBase {
  test("Take scalar from other project with wrong source name") {
    run("Create example graph")
    val other = clone(project)
    run("Rename scalar", Map("from" -> "greeting", "to" -> "farewell"), on = other)
    val ex = intercept[java.lang.AssertionError] {
      run(
        "Copy scalar from other project",
        Map(
          "sourceProject" -> s"!checkpoint(${other.checkpoint.get},ExampleGraph2)",
          "sourceScalarName" -> "greeting",
          "destScalarName" -> "doesntmatter"))
    }
    assert(ex.getMessage.contains(
      "No 'greeting' in project 'ExampleGraph2'."))
  }

  test("Take scalar from other project with conflicting name") {
    run("Create example graph")
    val other = clone(project)
    val ex = intercept[java.lang.AssertionError] {
      run(
        "Copy scalar from other project",
        Map(
          "sourceProject" -> s"!checkpoint(${other.checkpoint.get},ExampleGraph3)",
          "sourceScalarName" -> "greeting",
          "destScalarName" -> ""))
    }
    assert(ex.getMessage.contains(
      "Conflicting scalar name 'greeting'."))
  }

  test("Take scalar from other project scalar value ok") {
    run("Create example graph")
    val other = clone(project)
    run("Derive scalar", Map(
      "output" -> "scalar_val",
      "type" -> "double",
      "expr" -> "42.0"), on = other)
    run(
      "Copy scalar from other project",
      Map(
        "sourceProject" -> s"!checkpoint(${other.checkpoint.get},ExampleGraph4)",
        "sourceScalarName" -> "scalar_val",
        "destScalarName" -> "my_scalar"))

    assert(project.scalars("my_scalar").value == 42.0)
  }

  test("Take scalar from segmentation") {
    run("Create example graph")
    val other = clone(project)
    run("Add random vertex attribute", Map(
      "dist" -> "Standard Normal",
      "name" -> "rnd",
      "seed" -> "1474343267"), on = other)
    run("Segment by double attribute", Map(
      "attr" -> "rnd",
      "interval_size" -> "0.1",
      "name" -> "seg", "overlap" -> "no"), on = other)
    run("Derive scalar", Map(
      "output" -> "scalar_val",
      "type" -> "string",
      "expr" -> "'myvalue'"), on = other.segmentation("seg"))
    run(
      "Copy scalar from other project",
      Map(
        "sourceProject" -> s"!checkpoint(${other.checkpoint.get},Project2)",
        "sourceScalarName" -> "seg|scalar_val",
        "destScalarName" -> "my_scalar_2"))

    assert(project.scalars("my_scalar_2").value == "myvalue")
  }

  test("Take scalar from segmentation of segmentation") {
    run("Create example graph")
    val other = clone(project)
    run("Add random vertex attribute", Map(
      "dist" -> "Standard Normal",
      "name" -> "rnd",
      "seed" -> "1474343267"), on = other)
    run("Segment by double attribute", Map(
      "attr" -> "rnd",
      "interval_size" -> "0.1",
      "name" -> "seg",
      "overlap" -> "no"), on = other)
    run("Add random vertex attribute", Map(
      "dist" -> "Standard Normal",
      "name" -> "rnd2",
      "seed" -> "1474343267"), on = other.segmentation("seg"))
    run("Segment by double attribute", Map(
      "attr" -> "rnd2",
      "interval_size" -> "0.1",
      "name" -> "seg2",
      "overlap" -> "no"), on = other.segmentation("seg"))
    run("Derive scalar", Map(
      "output" -> "deep_scalar",
      "type" -> "string",
      "expr" -> "'deep value'"), on = other.segmentation("seg").segmentation("seg2"))
    run(
      "Copy scalar from other project",
      Map(
        "sourceProject" -> s"!checkpoint(${other.checkpoint.get},Project3)",
        "sourceScalarName" -> "seg|seg2|deep_scalar",
        "destScalarName" -> "my_scalar_3"))

    assert(project.scalars("my_scalar_3").value == "deep value")
  }

}
