package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphSingleton
import com.lynxanalytics.biggraph.serving
import java.util.UUID
import scala.util.Try

/**
 * Case classes used by the JsonServer to communicate with the web application
 */

case class GraphStatsRequest(id: String)

case class GraphStatsResponse(id: String,
                              vertices_size: Long,
                              edges_size: Long,
                              vertex_attributes: Seq[String],
                              edge_attributes: Seq[String])

/**
 * Logic for processing requests
 */

object GraphStatsController {
  val bigGraphManager = BigGraphSingleton.bigGraphManager
  val graphDataManager = BigGraphSingleton.graphDataManager

  def process(request: GraphStatsRequest): GraphStatsResponse = {
    val bigGraph = bigGraphManager.graphForGUID(UUID.fromString(request.id)).get
    val graphData = graphDataManager.obtainData(bigGraph)
    val vAttrs = bigGraph.vertexAttributes.getAttributesReadableAs[Any]
    val eAttrs = bigGraph.edgeAttributes.getAttributesReadableAs[Any]
    GraphStatsResponse(request.id,
                       graphData.vertices.count,
                       graphData.edges.count,
                       vAttrs,
                       eAttrs)
  }
}
