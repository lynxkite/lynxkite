package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations

class GraphDrawingControllerTest extends FunSuite with TestGraphOp with BigGraphEnvironment {
  val controller = new GraphDrawingController(this)
  val project = Project("Test_Project")
  project.notes = "test project" // Make sure project directory exists.

  test("get center of ExampleGraph with no filters") {
    val eg = graph_operations.ExampleGraph()().result
    val req = CenterRequest(
      vertexSetId = eg.vertices.gUID.toString,
      count = 1,
      filters = Seq())
    val res = controller.getCenter(req)
    assert(res.centers.toSet == Set("0"))
  }

  test("get 5 centers of ExampleGraph with no filters") {
    val eg = graph_operations.ExampleGraph()().result
    val req = CenterRequest(
      vertexSetId = eg.vertices.gUID.toString,
      count = 5,
      filters = Seq())
    val res = controller.getCenter(req)
    assert(res.centers.toSet == Set("0", "1", "2", "3"))
  }

  test("get center of ExampleGraph with filters set") {
    val eg = graph_operations.ExampleGraph()().result
    val f = FEVertexAttributeFilter(
      attributeId = eg.age.gUID.toString,
      valueSpec = "<=10")
    val req = CenterRequest(
      vertexSetId = eg.vertices.gUID.toString,
      count = 1,
      filters = Seq(f))
    val res = controller.getCenter(req)
    assert(res.centers.toSet == Set("3"))
  }
}
