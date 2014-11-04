package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_operations.DynamicValue

class GraphDrawingControllerTest extends FunSuite with TestGraphOp with BigGraphEnvironment {
  val controller = new GraphDrawingController(this)

  test("get center of ExampleGraph with no filters") {
    val g = graph_operations.ExampleGraph()().result
    val req = CenterRequest(
      vertexSetId = g.vertices.gUID.toString,
      count = 1,
      filters = Seq())
    val res = controller.getCenter(req)
    assert(res.centers.toSet == Set("0"))
  }

  test("get 5 centers of ExampleGraph with no filters") {
    val g = graph_operations.ExampleGraph()().result
    val req = CenterRequest(
      vertexSetId = g.vertices.gUID.toString,
      count = 5,
      filters = Seq())
    val res = controller.getCenter(req)
    assert(res.centers.toSet == Set("0", "1", "2", "3"))
  }

  test("get center of ExampleGraph with filters set") {
    val g = graph_operations.ExampleGraph()().result
    val f = FEVertexAttributeFilter(
      attributeId = g.age.gUID.toString,
      valueSpec = "<=10")
    val req = CenterRequest(
      vertexSetId = g.vertices.gUID.toString,
      count = 1,
      filters = Seq(f))
    val res = controller.getCenter(req)
    assert(res.centers.toSet == Set("3"))
  }

  test("get sampled vertex diagram of ExampleGraph with no filters, no attrs") {
    val g = graph_operations.ExampleGraph()().result
    val req = VertexDiagramSpec(
      vertexSetId = g.vertices.gUID.toString,
      filters = Seq(),
      mode = "sampled",
      centralVertexIds = Seq("0"),
      sampleSmearEdgeBundleId = g.edges.gUID.toString,
      attrs = Seq(),
      radius = 1)
    val res = controller.getSampledVertexDiagram(req)
    assert(res.mode == "sampled")
    assert(res.vertices.toSet == Set(
      FEVertex(0.0, 0, 0, id = 0, attrs = Map()),
      FEVertex(0.0, 0, 0, id = 1, attrs = Map()),
      FEVertex(0.0, 0, 0, id = 2, attrs = Map())))
  }

  test("get sampled vertex diagram of ExampleGraph with filters and attrs") {
    val g = graph_operations.ExampleGraph()().result
    val age = g.age.gUID.toString
    val gender = g.gender.gUID.toString

    val f = FEVertexAttributeFilter(
      attributeId = age,
      valueSpec = "<=25")
    val req = VertexDiagramSpec(
      vertexSetId = g.vertices.gUID.toString,
      filters = Seq(f),
      mode = "sampled",
      centralVertexIds = Seq("0"),
      sampleSmearEdgeBundleId = g.edges.gUID.toString,
      attrs = Seq(age, gender),
      radius = 1)
    val res = controller.getSampledVertexDiagram(req)
    assert(res.mode == "sampled")
    assert(res.vertices.toSet == Set(
      FEVertex(0.0, 0, 0, id = 0, attrs = Map(
        age -> DynamicValue(20.3, "20.3", true),
        gender -> DynamicValue(0.0, "Male", true))),
      FEVertex(0.0, 0, 0, id = 1, attrs = Map(
        age -> DynamicValue(18.2, "18.2", true),
        gender -> DynamicValue(0.0, "Female", true)))))
  }

  //TODO: bucketed, histo, scalar, edgeDiagram tests

}
