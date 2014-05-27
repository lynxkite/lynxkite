package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnviroment
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.graph_api._
import java.util.UUID

/**
 * Case classes used by the JsonServer to communicate with the web application
 */

case class GraphStatsRequest(id: String)

case class GraphStatsResponse(
  id: String,
  verticesCount: Long,
  edgesCount: Long,
  vertexAttributes: Seq[String],
  edgeAttributes: Seq[String])

/**
 * Logic for processing requests
 */

class GraphStatsController(enviroment: BigGraphEnviroment) {
  def getStats(request: GraphStatsRequest): GraphStatsResponse = {
    val bigGraph = BigGraphController.getBigGraphForId(request.id, enviroment)
    val graphData = enviroment.graphDataManager.obtainData(bigGraph)
    val vAttrs = bigGraph.vertexAttributes.getAttributesReadableAs[Any]
    val eAttrs = bigGraph.edgeAttributes.getAttributesReadableAs[Any]
    GraphStatsResponse(
      request.id,
      graphData.vertices.count,
      graphData.edges.count,
      vAttrs,
      eAttrs)
  }
}
