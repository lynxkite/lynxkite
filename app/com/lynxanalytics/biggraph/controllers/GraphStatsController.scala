package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnviroment
import com.lynxanalytics.biggraph.serving
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
  def process(request: GraphStatsRequest): GraphStatsResponse = {
    val bigGraph = enviroment.bigGraphManager.graphForGUID(UUID.fromString(request.id)).get
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
