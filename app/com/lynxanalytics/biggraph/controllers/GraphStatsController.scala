package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.graph_api._
import java.util.UUID

/**
 * Case classes used by the JsonServer to communicate with the web application
 */

case class GraphStatsRequest(id: String)

case class GraphStatsResponse(id: String,
                              verticesCount: Long,
                              edgesCount: Long,
                              vertexAttributes: Seq[String],
                              edgeAttributes: Seq[String])

/**
 * Logic for processing requests
 */

class GraphStatsController(enviroment: BigGraphEnvironment) {
  def getStats(request: GraphStatsRequest): GraphStatsResponse = ???
}
