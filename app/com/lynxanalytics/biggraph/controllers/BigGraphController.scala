package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnviroment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.serving
import java.util.UUID
import scala.collection.mutable

case class BigGraphRequest(id: String)

case class GraphBasicData(
  title: String,
  id: String)

case class FEOperationParameterMeta(
  title: String,
  defaultValue: String)

case class FEOperationMeta(
  operationId: Int,
  name: String,
  parameters: Seq[FEOperationParameterMeta])

case class BigGraphResponse(
  title: String,
  sources: Seq[GraphBasicData],
  derivatives: Seq[GraphBasicData],
  ops: Seq[FEOperationMeta])

case class FEOperationSpec(
  operationId: Int,
  parameters: Seq[String])

case class DeriveBigGraphRequest(
  sourceIds: Seq[String],
  operation: FEOperationSpec)

trait FEOperation {
  def applicableTo(bigGraphs: Seq[BigGraph]): Boolean
  val name: String
  def parameters(bigGraphs: Seq[BigGraph]): Seq[FEOperationParameterMeta] = parameters
  def parameters: Seq[FEOperationParameterMeta] = ???
  def toGraphOperation(parameters: Seq[String]): GraphOperation
}

trait SingleGraphFEOperation extends FEOperation {
  def applicableTo(bigGraphs: Seq[BigGraph]) = (bigGraphs.size == 1)
}
trait StartingFEOperation extends FEOperation {
  def applicableTo(bigGraphs: Seq[BigGraph]) = bigGraphs.isEmpty
}


class FEOperationRepository {
  def registerOperation(op: FEOperation): Unit = operations += op

  def getApplicableOperationMetas(bigGraphs: Seq[BigGraph]): Seq[FEOperationMeta] =
      operations
        .zipWithIndex
        .filter(_._1.applicableTo(bigGraphs))
        .map{case (op, id) => FEOperationMeta(id, op.name, op.parameters(bigGraphs))}

  def getGraphOperation(spec: FEOperationSpec): GraphOperation = {
    operations(spec.operationId).toGraphOperation(spec.parameters)
  }

  private val operations = mutable.Buffer[FEOperation]()
}

object FEOperations extends FEOperationRepository {
  registerOperation(
    new SingleGraphFEOperation {
      val name = "Find Maximal Cliques"
      override val parameters = Seq(
          FEOperationParameterMeta("Minimum Clique Size", "3"))
      def toGraphOperation(parameters: Seq[String]) =
        graph_operations.FindMaxCliques("clique_members", parameters.head.toInt)
    })
  registerOperation(
    new SingleGraphFEOperation {
      val name = "Edge Graph"
      override val parameters = Seq()
      def toGraphOperation(parameters: Seq[String]) = graph_operations.EdgeGraph()
    })
  registerOperation(
    new FEOperation {
      val name = "Remove non-symmetric edges"
      private val operation = new graph_operations.RemoveNonSymmetricEdges()
      def applicableTo(bigGraphs: Seq[BigGraph]): Boolean =
        (bigGraphs.size) == 1 && !bigGraphs.head.properties.symmetricEdges
      override val parameters = Seq()
      def toGraphOperation(parameters: Seq[String]) = operation
    })
  registerOperation(
    new FEOperation {
      val name = "Connected Components"
      private val operation = new graph_operations.ConnectedComponents("component_id")
      def applicableTo(bigGraphs: Seq[BigGraph]): Boolean = operation.isSourceListValid(bigGraphs)
      override val parameters = Seq()
      def toGraphOperation(parameters: Seq[String]) = operation
    })
  registerOperation(
    new FEOperation {
      val name = "Edges from set overlap"
      def applicableTo(bigGraphs: Seq[BigGraph]): Boolean =
        (bigGraphs.size) == 1 &&
          bigGraphs.head.vertexAttributes.getAttributesReadableAs[Array[Long]].size > 0
      override def parameters(bigGraphs: Seq[BigGraph]) = Seq(
        FEOperationParameterMeta(
          "Set attribute name",
          bigGraphs.head.vertexAttributes.getAttributesReadableAs[Array[Long]].head),
        FEOperationParameterMeta("Minimum Overlap", "3"))
      def toGraphOperation(parameters: Seq[String]) =
          new graph_operations.SetOverlap(parameters(0), parameters(1).toInt)
    })
  registerOperation(
    new StartingFEOperation {
      val name = "Uniform Random Graph"
      override val parameters = Seq(
        FEOperationParameterMeta("Number of vertices", "10"),
        FEOperationParameterMeta("Random seed", "0"),
        FEOperationParameterMeta("Edge probability", "0.5"))
      def toGraphOperation(parameters: Seq[String]) =
        graph_operations.SimpleRandomGraph(parameters(0).toInt,
                                           parameters(1).toInt,
                                           parameters(2).toFloat)
    })
  registerOperation(
    new StartingFEOperation {
      val name = "Simple Example Graph With Attributes"
      override val parameters = Seq()
      def toGraphOperation(parameters: Seq[String]) = InstantiateSimpleGraph2()
    })
}

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
      derivatives = enviroment.bigGraphManager
        .knownDirectDerivatives(bigGraph).map(basicDataFromGraph(_)),
      ops = FEOperations.getApplicableOperationMetas(Seq(bigGraph)))
  }

  def getGraph(request: BigGraphRequest): BigGraphResponse = {
    responseFromGraph(BigGraphController.getBigGraphForId(request.id, enviroment))
  }

  def deriveGraph(request: DeriveBigGraphRequest): GraphBasicData = {
    val sourceGraphs = request.sourceIds.map(
        id => BigGraphController.getBigGraphForId(id, enviroment))
    val op = FEOperations.getGraphOperation(request.operation)
    basicDataFromGraph(enviroment.bigGraphManager.deriveGraph(sourceGraphs, op))
  }

  def startingOperations(request: serving.EmptyRequest): Seq[FEOperationMeta] =
    FEOperations.getApplicableOperationMetas(Seq())
}
object BigGraphController {
  def getBigGraphForId(id: String, enviroment: BigGraphEnviroment): BigGraph = {
    enviroment.bigGraphManager.graphForGUID(UUID.fromString(id)).get
  }
}

// TODO: remove this once we have some more sane starting operations.
// For now this is a copy of the same class created in GraphTestUtils. The reason for the copy
// is that prod code cannot (or at least should not) depend on test code, but I don't want
// to move this out to some permanent non-test location as it will go away from prod code
// but will stick around in test code.
import org.apache.spark.graphx.Edge
import attributes.AttributeSignature
import attributes.DenseAttributes

case class InstantiateSimpleGraph2() extends GraphOperation {
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
        Edge(0l, 1l, edgeMaker.make.set(commentIdx, "Adam loves Eve")),
        Edge(1l, 0l, edgeMaker.make.set(commentIdx, "Eve loves Adam")),
        Edge(2l, 0l, edgeMaker.make.set(commentIdx, "Bob envies Adam")),
        Edge(2l, 1l, edgeMaker.make.set(commentIdx, "Bob loves Eve")))

    executionCounter += 1

    return new SimpleGraphData(target, sc.parallelize(vertices), sc.parallelize(edges))
  }

  def vertexAttributes(sources: Seq[BigGraph]): AttributeSignature =
    AttributeSignature.empty.addAttribute[String]("name").signature

  def edgeAttributes(sources: Seq[BigGraph]): AttributeSignature =
    AttributeSignature.empty.addAttribute[String]("comment").signature
}
