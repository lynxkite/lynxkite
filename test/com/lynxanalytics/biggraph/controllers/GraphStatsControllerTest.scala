package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.BigGraphTestEnviroment
import play.api.test.FakeRequest
import play.api.test.FakeHeaders
import play.api.test.Helpers
import play.api.test.Helpers._
import play.api.libs.json.Json

/* play.api.test should be replaced with https://github.com/scalatest/scalatestplus-play
 * as soon as it is published with documentation. Should happen any day.
 * More information: https://groups.google.com/forum/#!topic/scalatest-users/u7LKrKcV1k
 */

class GraphStatsControllerTest extends FunSuite {
  test("get stats for the test graph") {
    val bigGraph = BigGraphTestEnviroment.bigGraphManager.deriveGraph(Seq(), new InstantiateSimpleGraph)
    val testGraphStatsController = new GraphStatsController(BigGraphTestEnviroment)
    val id = bigGraph.gUID.toString
    val result = testGraphStatsController.process(GraphStatsRequest(id))
    assert(result.id === id)
    assert(result.vertices_count === 3)
    assert(result.edges_count === 4)
    assert(result.vertex_attributes.head === "name")
    assert(result.edge_attributes.head === "comment")
  }
}
