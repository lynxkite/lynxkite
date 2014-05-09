package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphSingleton
import com.lynxanalytics.biggraph.graph_api
import com.lynxanalytics.biggraph.graph_api.BigGraph
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.serving
import java.util.UUID

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
  val bigGraphManager = BigGraphSingleton.bigGraphManager

  // TODO(forevian): remove this block once standalone graph stats ready.
  val graphDataManager = BigGraphSingleton.graphDataManager
  def fakeGraphStats(graph: BigGraph): String = {
    val data = graphDataManager.obtainData(graph)
    "Vertices: %d Edges: %s".format(data.vertices.count, data.edges.count)
  }

  def basicDataFromGraph(graph: BigGraph): GraphBasicData = {
    GraphBasicData(graph.toLongString, graph.gUID.toString)
  }

  private def responseFromGraph(graph: BigGraph): BigGraphResponse = {
    BigGraphResponse(
      title = graph.toLongString,
      stats = fakeGraphStats(graph),
      sources = graph.sources.map(basicDataFromGraph(_)),
      ops = Seq(basicDataFromGraph(bigGraphManager.deriveGraph(
                                 Seq(graph), new graph_operations.EdgeGraph))))
  }

  def process(request: BigGraphRequest): BigGraphResponse = {
    if (request.id == "x") {
      responseFromGraph(bigGraphManager.deriveGraph(Seq(), new InstantiateSimpleGraph2))
    } else {
      responseFromGraph(bigGraphManager.graphForGUID(UUID.fromString(request.id)).get)
    }
  }
}


// TODO: remove this once we have some more sane starting operations.
// For now this is a copy of the same class created in GraphTestUtils. The reason for the copy
// is that prod code cannot (or at least should not) depend on test code, but I don't want
// to move this out to some permanent non-test location as it will go away from prod code
// but will stick around in test code.
import graph_api._
import org.apache.spark.graphx.Edge
import attributes.AttributeSignature
import attributes.DenseAttributes

class InstantiateSimpleGraph2 extends GraphOperation {
  @transient var executionCounter = 0

  def isSourceListValid(sources: Seq[BigGraph]) = (sources.size == 0)

  def execute(target: BigGraph, manager: GraphDataManager): GraphData = {
    val sc = manager.runtimeContext.sparkContext
    val vertexSig = vertexAttributes(target.sources)
    val edgeSig = edgeAttributes(target.sources)

    val vertexMaker = vertexSig.maker
    val nameIdx = vertexSig.writeIndex[String]("name")
    val vertices = Seq(
        (0l, vertexMaker.make.set(nameIdx, "Adam")),
        (1l, vertexMaker.make.set(nameIdx, "Eve")),
        (2l, vertexMaker.make.set(nameIdx, "Bob")))

    val edgeMaker = edgeSig.maker
    val commentIdx = edgeSig.writeIndex[String]("comment")
    val edges = Seq(
        new Edge(0l, 1l, edgeMaker.make.set(commentIdx, "Adam loves Eve")),
        new Edge(1l, 0l, edgeMaker.make.set(commentIdx, "Eve loves Adam")),
        new Edge(2l, 0l, edgeMaker.make.set(commentIdx, "Bob envies Adam")),
        new Edge(2l, 1l, edgeMaker.make.set(commentIdx, "Bob loves Eve")))

    executionCounter += 1

    return new SimpleGraphData(target, sc.parallelize(vertices), sc.parallelize(edges))
  }

  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature =
    AttributeSignature.empty.addAttribute[String]("name").signature

  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature =
    AttributeSignature.empty.addAttribute[String]("comment").signature
}

