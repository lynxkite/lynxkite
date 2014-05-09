package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api
import com.lynxanalytics.biggraph.serving

case class BigGraphRequest(id: String)

case class GraphBasicData(
  title: String,
  id: String)

case class BigGraphResponse(
  title: String,
  stats: String,
  sources: Seq[GraphBasicData],
  ops: Seq[GraphBasicData])

/**
 * Logic for processing requests
 */

object BigGraphController {
  def process(request: BigGraphRequest): BigGraphResponse = {
    BigGraphResponse(
      title = request.id,
      stats = "Stats of " + request.id,
      sources = Seq(
          GraphBasicData(request.id + "pre1", request.id + "pre1"),
          GraphBasicData(request.id + "pre2", request.id + "pre2")),
      ops = Seq(
          GraphBasicData(request.id + "post1", request.id + "post1"),
          GraphBasicData(request.id + "post2", request.id + "post2")))
  }
}
