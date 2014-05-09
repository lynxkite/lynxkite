package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api
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
  val repositoryPath = ??? // todo: set repository path
  val sc = ??? // todo: create spark context
  val bigGraphManager = graph_api.BigGraphManager(repositoryPath)
  val graphDataManager = graph_api.GraphDataManager(sc, repositoryPath)

  def process(request: GraphStatsRequest): Option[GraphStatsResponse] = {
    bigGraphManager.graphForGUID(UUID.fromString(request.id)) match {
      case Some(bigGraph) =>
        val graphData = graphDataManager.obtainData(bigGraph)
        val vAttrs = bigGraph.vertexAttributes.getAttributesReadableAs[Any]
        val eAttrs = bigGraph.edgeAttributes.getAttributesReadableAs[Any]
        Some(GraphStatsResponse(request.id,
                                graphData.vertices.count,
                                graphData.edges.count,
                                vAttrs,
                                eAttrs))
      case None => None
    }
  }

}