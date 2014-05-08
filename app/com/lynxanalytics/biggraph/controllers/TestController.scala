package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api
import com.lynxanalytics.biggraph.serving
import java.util.UUID

/**
 * Case classes used by the JsonServer to communicate with the web application
 */

case class TestRequest(attr: String)
case class TestResponse(attr: String)

/**
 * Logic for processing requests
 */

object TestController {
  def process(request: TestRequest): TestResponse = {
    TestResponse("test string: " + request.attr)
  }
}

// should it be in a separate file? or just rename TestControllers to Controllers?
object GraphStats {
  case class GraphStatsRequest(gUID: UUID) // and filters

  case class GraphStatsResponse(gUID: UUID,
                                vertices: Long,
                                edges: Long,
                                vertex_attributes: Seq[String],
                                edge_attributes: Seq[String])
                                // what about name, description and owner/creator

  val repositoryPath = ??? // todo: set repository path
  val sc = ??? // todo: create spark context
  val bigGraphManager = graph_api.BigGraphManager(repositoryPath)
  val graphDataManager = graph_api.GraphDataManager(sc, repositoryPath)

  def graphStats(request: GraphStatsRequest,
                 name: Option[String] = None): GraphStatsResponse = {
    val bigGraph: Option[graph_api.BigGraph] = bigGraphManager.graphForGUID(request.gUID)
    // todo: parse sequence of filters using the source BigGraph data
    // todo: create the filtered BigGraph object
    val filteredBigGraph: Option[graph_api.BigGraph] = bigGraph
    filteredBigGraph match {
      case None =>
        // todo: return error message within GraphStatsResponse somehow
        GraphStatsResponse(request.gUID, 0, 0, Seq(), Seq())
      case Some(bg) =>
        val filteredData = graphDataManager.obtainData(bg)
        val vAttrs = bg.vertexAttributes.getAttributesReadableAs[Any]
        val eAttrs = bg.edgeAttributes.getAttributesReadableAs[Any]
        // todo: get histograms
        // todo: list of known direct derived graphs and derivativees?
        GraphStatsResponse(request.gUID,
                           filteredData.vertices.count,
                           filteredData.edges.count,
                           vAttrs,
                           eAttrs)
    }
  }

  /*
  // some ideas here for restructuring filter parsing
  case class JsonFilter(attr: String,
                        bucket: String)

  trait UIFilter {
    def toGraphOp(): graph_api.GraphOperation
    def attr: String
    def bucket: String
  }


  def parseFilter(biggraph: graph_api.BigGraph, jsonfilter: JsonFilter): UIFilter = {
    val signature = biggraph.vertexAttributes
    jsonfilter.attr match {
      case "$vertexid" => SingleVertexFilter(jsonfilter.bucket.toLong)
      case "$vertexneighbors" =>
        VertexNeigborhoodFilter(Try(Some(jsonfilter.bucket.toLong)).getOrElse(None))
      case attr if signature.canRead[Double](attr) =>
        val bounds = jsonfilter.bucket.split("-").map(_.toDouble)
        NumFilter(biggraph, attr, bounds(0), bounds(1))
      case attr if signature.canRead[String](attr) =>
        if (jsonfilter.bucket.startsWith("NOT ")) {
          NotStrFilter(biggraph, attr, jsonfilter.bucket.drop(4).split(" | "))
        } else {
          StrFilter(biggraph, attr, jsonfilter.bucket)
        }
      case _ =>
        throw new IllegalArgumentException("Filtering on this attribute type is not supported")
      }
    }
  */

}
