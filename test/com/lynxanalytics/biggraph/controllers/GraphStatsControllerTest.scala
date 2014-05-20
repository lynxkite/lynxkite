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
    assert(result.verticesCount === 3)
    assert(result.edgesCount === 4)
    assert(result.vertexAttributes === Seq("name", "id"))
    assert(result.edgeAttributes === Seq("comment"))
  }
}
