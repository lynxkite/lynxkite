package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._

class GraphStatsControllerTest extends FunSuite {
  test("get stats for the test graph") {
    val testEnviroment = new BigGraphTestEnviroment("graphstatscontroller")
    val bigGraph = testEnviroment.bigGraphManager.deriveGraph(Seq(), new InstantiateSimpleGraph)
    val testGraphStatsController = new GraphStatsController(testEnviroment)
    val id = bigGraph.gUID.toString
    val result = testGraphStatsController.getStats(GraphStatsRequest(id))
    assert(result.id === id)
    assert(result.vertices_count === 3)
    assert(result.edges_count === 4)
    assert(result.vertex_attributes === Seq("name"))
    assert(result.edge_attributes === Seq("comment"))
  }
}
