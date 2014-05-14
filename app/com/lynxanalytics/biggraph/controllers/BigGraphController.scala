package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnviroment
import com.lynxanalytics.biggraph.graph_api
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.serving
import java.util.UUID

case class BigGraphRequest(id: String)

case class GraphBasicData(
  title: String,
  id: String)

case class BigGraphResponse(
  title: String,
  sources: Seq[GraphBasicData],
  ops: Seq[GraphBasicData])

/**
 * Logic for processing requests
 */

class BigGraphController(enviroment: BigGraphEnviroment) {
  def basicDataFromGraph(bigGraph: BigGraph): GraphBasicData = {
    GraphBasicData(bigGraph.toLongString, bigGraph.gUID.toString)
  }

  private def responseFromGraph(bigGraph: BigGraph): BigGraphResponse = {
    BigGraphResponse(
      title = bigGraph.toLongString,
      sources = bigGraph.sources.map(basicDataFromGraph(_)),
      ops = Seq(basicDataFromGraph(enviroment.bigGraphManager.deriveGraph(
                                 Seq(bigGraph), new graph_operations.EdgeGraph))))
  }

  def getGraph(request: BigGraphRequest): BigGraphResponse = {
    responseFromGraph(BigGraphManager.getBigGraphForId(request.id, enviroment))
  }
}

