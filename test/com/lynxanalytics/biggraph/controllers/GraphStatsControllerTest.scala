package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.serving.JsonServer
import com.lynxanalytics.biggraph.BigGraphSingleton
import play.api.test.FakeRequest
import play.api.test.FakeHeaders
import play.api.test.Helpers
import play.api.test.Helpers._
import play.api.libs.json.Json

/* play.api.test should be replaced with https://github.com/scalatest/scalatestplus-play
 * as soon as it is published with documentation. Should happen any day.
 * More information: https://groups.google.com/forum/#!topic/scalatest-users/u7LKrKcV1k
 */


class GraphStatsControllerTest  extends FunSuite {
  test("get stats for the test graph") {
    val bigGraphManager = BigGraphSingleton.bigGraphManager
    val graphDataManager = BigGraphSingleton.graphDataManager
    val bigGraph = bigGraphManager.deriveGraph(Seq(), new InstantiateSimpleGraph)
    val id = bigGraph.gUID.toString
    val jsonString = "{\"id\":\"%s\"}".format(id)
    val request = FakeRequest(GET, "/api/test?q=" + jsonString)
    val result = JsonServer.graphStatsGet(request)
    assert(Helpers.status(result) === OK)
    assert((Json.parse(Helpers.contentAsString(result)) \ ("id")).as[String] === id)
    assert((Json.parse(Helpers.contentAsString(result)) \ ("vertices_size")).as[Long] === 3)
    assert((Json.parse(Helpers.contentAsString(result)) \ ("edges_size")).as[Long] === 4)
    assert((Json.parse(Helpers.contentAsString(result)) \ ("vertex_attributes")).as[Seq[String]].head === "name")
    assert((Json.parse(Helpers.contentAsString(result)) \ ("edge_attributes")).as[Seq[String]].head === "comment")
  }
}
