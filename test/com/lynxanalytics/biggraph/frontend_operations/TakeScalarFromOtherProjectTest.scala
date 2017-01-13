package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class TakeScalarFromOtherProjectTest extends OperationsTestBase {
  test("Take scalar from other project with wrong source name") {
    run("Example Graph")
    val otherEditor = clone(project)
    run("Rename scalar", Map("from" -> "greeting", "to" -> "farewell"), on = otherEditor)
    val ex = intercept[java.lang.AssertionError] {
      run(
        "Take scalar from other project",
        Map(
          "otherProject" -> s"!checkpoint(${otherEditor.checkpoint.get},ExampleGraph2)",
          "origName" -> "greeting",
          "newName" -> "doesntmatter"))
    }
    assert(ex.getMessage.contains(
      "No 'greeting' in project 'ExampleGraph2'."))
  }

  test("Take scalar from other project with conflicting name") {
    run("Example Graph")
    val otherEditor = clone(project)
    val ex = intercept[java.lang.AssertionError] {
      run(
        "Take scalar from other project",
        Map(
          "otherProject" -> s"!checkpoint(${otherEditor.checkpoint.get},ExampleGraph2)",
          "origName" -> "greeting",
          "newName" -> ""))
    }
    assert(ex.getMessage.contains(
      "Conflicting scalar name 'greeting'."))
  }

  test("Take scalar from other project scalar value ok") {
    run("Example Graph")
    val otherEditor = clone(project)
    run("Derive scalar", Map(
      "output" -> "scalar_val",
      "type" -> "double",
      "expr" -> "42.0"), on = otherEditor)
    run(
      "Take scalar from other project",
      Map(
        "otherProject" -> s"!checkpoint(${otherEditor.checkpoint.get},ExampleGraph2)",
        "origName" -> "scalar_val",
        "newName" -> "my_scalar"))

    assert(project.scalars("my_scalar").value == 42.0)
  }

  test("Take scalar from segmentation") {
    run("Example Graph")
    val otherEditor = clone(project)
    run("Add random vertex attribute", Map(
      "dist" -> "Standard Normal",
      "name" -> "rnd",
      "seed" -> "1474343267"), on = otherEditor)
    run("Segment by double attribute", Map(
      "attr" -> "rnd",
      "interval-size" -> "0.1",
      "name" -> "seg", "overlap" -> "no"), on = otherEditor)
    run("Derive scalar", Map(
      "output" -> "scalar_val",
      "type" -> "string",
      "expr" -> "'myvalue'"), on = otherEditor.segmentation("seg"))
    run(
      "Take scalar from other project",
      Map(
        "otherProject" -> s"!checkpoint(${otherEditor.checkpoint.get},Project2)",
        "origName" -> "seg|scalar_val",
        "newName" -> "my_scalar_2"))

    assert(project.scalars("my_scalar_2").value == "myvalue")
  }

  test("Take scalar from segmentation of segmentation") {
    run("Example Graph")
    val otherEditor = clone(project)
    run("Add random vertex attribute", Map(
      "dist" -> "Standard Normal",
      "name" -> "rnd",
      "seed" -> "1474343267"), on = otherEditor)
    run("Segment by double attribute", Map(
      "attr" -> "rnd",
      "interval-size" -> "0.1",
      "name" -> "seg",
      "overlap" -> "no"), on = otherEditor)
    run("Add random vertex attribute", Map(
      "dist" -> "Standard Normal",
      "name" -> "rnd2",
      "seed" -> "1474343267"), on = otherEditor.segmentation("seg"))
    run("Segment by double attribute", Map(
      "attr" -> "rnd2",
      "interval-size" -> "0.1",
      "name" -> "seg2",
      "overlap" -> "no"), on = otherEditor.segmentation("seg"))
    run("Derive scalar", Map(
      "output" -> "deep_scalar",
      "type" -> "string",
      "expr" -> "'deep value'"), on = otherEditor.segmentation("seg").segmentation("seg2"))
    run(
      "Take scalar from other project",
      Map(
        "otherProject" -> s"!checkpoint(${otherEditor.checkpoint.get},Project2)",
        "origName" -> "seg|seg2|deep_scalar",
        "newName" -> "my_scalar_3"))

    assert(project.scalars("my_scalar_3").value == "deep value")
  }

}
