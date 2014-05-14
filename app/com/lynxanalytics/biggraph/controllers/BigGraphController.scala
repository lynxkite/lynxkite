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
    responseFromGraph(BigGraphController.getBigGraphForId(request.id, enviroment))
  }
}
object BigGraphController {
  // TODO: currently a hack for handling "x" initial request
  def getBigGraphForId(id: String, enviroment: BigGraphEnviroment): BigGraph = {
    if (id == "x") {
      enviroment.bigGraphManager.deriveGraph(Seq(), new InstantiateSimpleGraph2)
    } else {
      enviroment.bigGraphManager.graphForGUID(UUID.fromString(id)).get
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
