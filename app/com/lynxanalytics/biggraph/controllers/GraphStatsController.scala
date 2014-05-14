package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnviroment
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.graph_api._
import java.util.UUID

/**
 * Case classes used by the JsonServer to communicate with the web application
 */

case class GraphStatsRequest(id: String)

case class GraphStatsResponse(id: String,
                              vertices_count: Long,
                              edges_count: Long,
                              vertex_attributes: Seq[String],
                              edge_attributes: Seq[String])

/**
 * Logic for processing requests
 */

class GraphStatsController(enviroment: BigGraphEnviroment) {
  def getStats(request: GraphStatsRequest): GraphStatsResponse = {
    val bigGraph = {
      if (request.id == "x") {
        enviroment.bigGraphManager.deriveGraph(Seq(), new InstantiateSimpleGraph2)
      } else {
        enviroment.bigGraphManager.graphForGUID(UUID.fromString(request.id)).get
      }
    }
    val graphData = enviroment.graphDataManager.obtainData(bigGraph)
    val vAttrs = bigGraph.vertexAttributes.getAttributesReadableAs[Any]
    val eAttrs = bigGraph.edgeAttributes.getAttributesReadableAs[Any]
    GraphStatsResponse(request.id,
                       graphData.vertices.count,
                       graphData.edges.count,
                       vAttrs,
                       eAttrs)
  }
}
