package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
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
  def parameters: Seq[FEOperationParameterMeta] = Seq()
  def toGraphOperation(parameters: Seq[String]): GraphOperation = operation
  def operation: GraphOperation = ???
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
      .map { case (op, id) => FEOperationMeta(id, op.name, op.parameters(bigGraphs)) }

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
      override def toGraphOperation(parameters: Seq[String]) =
        graph_operations.FindMaxCliques("clique_members", parameters.head.toInt)
    })
  registerOperation(
    new SingleGraphFEOperation {
      val name = "Edge Graph"
      override val operation = graph_operations.EdgeGraph()
    })
  registerOperation(
    new FEOperation {
      val name = "Remove non-symmetric edges"
      override val operation = new graph_operations.RemoveNonSymmetricEdges()
      def applicableTo(bigGraphs: Seq[BigGraph]): Boolean =
        bigGraphs.size == 1 && !bigGraphs.head.properties.symmetricEdges
    })
  registerOperation(
    new FEOperation {
      val name = "Connected Components"
      override val operation = new graph_operations.ConnectedComponents("component_id")
      def applicableTo(bigGraphs: Seq[BigGraph]): Boolean = operation.isSourceListValid(bigGraphs)
    })
  registerOperation(
    new FEOperation {
      val name = "Edges from set overlap"
      def applicableTo(bigGraphs: Seq[BigGraph]): Boolean =
        bigGraphs.size == 1 &&
          bigGraphs.head.vertexAttributes.getAttributesReadableAs[Array[Long]].size > 0
      override def parameters(bigGraphs: Seq[BigGraph]) = Seq(
        FEOperationParameterMeta(
          "Set attribute name",
          bigGraphs.head.vertexAttributes.getAttributesReadableAs[Array[Long]].head),
        FEOperationParameterMeta("Minimum Overlap", "3"))
      override def toGraphOperation(parameters: Seq[String]) =
        new graph_operations.SetOverlap(parameters(0), parameters(1).toInt)
    })
  registerOperation(
    new SingleGraphFEOperation {
      val name = "Add constant edge attribute"
      override val parameters = Seq(
        FEOperationParameterMeta("Name of new attribute", "weight"),
        FEOperationParameterMeta("Value", "1"))
      override def toGraphOperation(parameters: Seq[String]) =
        new graph_operations.ConstantDoubleEdgeAttribute(parameters(0), parameters(1).toDouble)
    })
  registerOperation(
    new SingleGraphFEOperation {
      val name = "Add reversed edges"
      override val operation = graph_operations.AddReversedEdges()
      override val parameters = Seq()
    })
  registerOperation(
    new SingleGraphFEOperation {
      val name = "Compute clustering coefficient"
      override val operation = graph_operations.ClusteringCoefficient("clustering_coefficient")
    })
  registerOperation(
    new FEOperation {
      val name = "Weighted out-degree"
      def applicableTo(bigGraphs: Seq[BigGraph]): Boolean =
        bigGraphs.size == 1 &&
          bigGraphs.head.edgeAttributes.getAttributesReadableAs[Double].size > 0
      override def parameters(bigGraphs: Seq[BigGraph]) = Seq(
        FEOperationParameterMeta(
          "Weight attribute",
          bigGraphs.head.edgeAttributes.getAttributesReadableAs[Double].head),
        FEOperationParameterMeta("Target attribute name", "weighted_out_degree"))
      override def toGraphOperation(parameters: Seq[String]) =
        graph_operations.WeightedOutDegree(parameters(0), parameters(1))
    })
  registerOperation(
    new SingleGraphFEOperation {
      val name = "Reverse edges"
      override val operation = graph_operations.ReverseEdges()
    })
  registerOperation(
    new SingleGraphFEOperation {
      val name = "Drop attributes"
      override val operation = graph_operations.DropAttributes()
    })
  registerOperation(
    new FEOperation {
      val name = "Page Rank"
      def applicableTo(bigGraphs: Seq[BigGraph]): Boolean =
        bigGraphs.size == 1 &&
          bigGraphs.head.edgeAttributes.getAttributesReadableAs[Double].size > 0
      override def parameters(bigGraphs: Seq[BigGraph]) = Seq(
        FEOperationParameterMeta(
          "Weight attribute",
          bigGraphs.head.edgeAttributes.getAttributesReadableAs[Double].head),
        FEOperationParameterMeta("Target attribute name", "page_rank"),
        FEOperationParameterMeta("Damping factor", "0.85"),
        FEOperationParameterMeta("Number of iterations", "10"))
      override def toGraphOperation(parameters: Seq[String]) =
        graph_operations.PageRank(
          parameters(0), parameters(1), parameters(2).toDouble, parameters(3).toInt)
    })
  registerOperation(
    new FEOperation {
      val name = "Expand vertex sets"
      def applicableTo(bigGraphs: Seq[BigGraph]): Boolean =
        bigGraphs.size == 1 &&
          bigGraphs.head.vertexAttributes.getAttributesReadableAs[Array[Long]].size > 0
      override def parameters(bigGraphs: Seq[BigGraph]) = Seq(
        FEOperationParameterMeta(
          "ID set attribute",
          bigGraphs.head.vertexAttributes.getAttributesReadableAs[Array[Long]].head),
        FEOperationParameterMeta("New attribute with old IDs", "containers"))
      override def toGraphOperation(parameters: Seq[String]) =
        graph_operations.ExpandVertexSet(
          parameters(0), parameters(1))
    })

  registerOperation(
    new StartingFEOperation {
      val name = "Uniform Random Graph"
      override val parameters = Seq(
        FEOperationParameterMeta("Number of vertices", "10"),
        FEOperationParameterMeta("Random seed", "0"),
        FEOperationParameterMeta("Edge probability", "0.5"))
      override def toGraphOperation(parameters: Seq[String]) =
        graph_operations.SimpleRandomGraph(
          parameters(0).toInt,
          parameters(1).toInt,
          parameters(2).toFloat)
    })
  registerOperation(
    new StartingFEOperation {
      val name = "Simple Example Graph With Attributes"
      override val operation = InstantiateSimpleGraph2()
    })
  registerOperation(
    new StartingFEOperation {
      val name = "Import Graph from CSV"
      override val parameters = Seq(
        FEOperationParameterMeta("Vertex header file", ""),
        FEOperationParameterMeta("Vertex CSV file(s) separated by ',' and/or matched by '*'", ""),
        FEOperationParameterMeta("Edge header file", ""),
        FEOperationParameterMeta("Edge CSV file(s) separated by ',' and/or matched by '*'", ""),
        FEOperationParameterMeta("Vertex id field name", ""),
        FEOperationParameterMeta("Edge source field name", ""),
        FEOperationParameterMeta("Edge destination field name", ""),
        FEOperationParameterMeta("Delimiter", ","),
        FEOperationParameterMeta("Skip header row while processing data (true/false)", "false"),
        FEOperationParameterMeta("AWS Access Key ID (optional)", ""),
        FEOperationParameterMeta("AWS Secret Access Key (optional)", ""))
      override def toGraphOperation(parameters: Seq[String]) =
        graph_operations.CSVImport(
          graph_util.Filename(parameters(0)),
          parameters(1).split(",").map(graph_util.Filename(_, parameters(9), parameters(10))),
          graph_util.Filename(parameters(2)),
          parameters(3).split(",").map(graph_util.Filename(_, parameters(9), parameters(10))),
          parameters(4),
          parameters(5),
          parameters(6),
          parameters(7),
          parameters(8).toBoolean)
    })
  registerOperation(
    new StartingFEOperation {
      val name = "Import Graph from Edge List CSV"
      override val parameters = Seq(
        FEOperationParameterMeta("Edge header file", ""),
        FEOperationParameterMeta("Edge CSV file(s) separated by ',' and/or matched by '*'", ""),
        FEOperationParameterMeta("Vertex id attribute name", "vertexId"),
        FEOperationParameterMeta("Edge source field name", ""),
        FEOperationParameterMeta("Edge destination field name", ""),
        FEOperationParameterMeta("Delimiter", ","),
        FEOperationParameterMeta("Skip header row while processing data (true/false)", "false"),
        FEOperationParameterMeta("AWS Access Key ID (optional)", ""),
        FEOperationParameterMeta("AWS Secret Access Key (optional)", ""),
        FEOperationParameterMeta("Disallowed vertex IDs (optional, comma separated list)", ""))
      override def toGraphOperation(parameters: Seq[String]) =
        graph_operations.EdgeCSVImport(
          graph_util.Filename(parameters(0)),
          parameters(1).split(",").map(graph_util.Filename(_, parameters(7), parameters(8))),
          parameters(2),
          parameters(3),
          parameters(4),
          parameters(5),
          parameters(6).toBoolean,
          parameters(9).split(",").toSet)
    })
}

/**
 * Logic for processing requests
 */

class BigGraphController(environment: BigGraphEnvironment) {
  def basicDataFromGraph(bigGraph: BigGraph): GraphBasicData = {
    GraphBasicData(bigGraph.toLongString, bigGraph.gUID.toString)
  }

  private def responseFromGraph(bigGraph: BigGraph): BigGraphResponse = {
    BigGraphResponse(
      title = bigGraph.toLongString,
      sources = bigGraph.sources.map(basicDataFromGraph(_)),
      derivatives = environment.bigGraphManager
        .knownDirectDerivatives(bigGraph).map(basicDataFromGraph(_)),
      ops = FEOperations.getApplicableOperationMetas(Seq(bigGraph)))
  }

  def getGraph(request: BigGraphRequest): BigGraphResponse = {
    responseFromGraph(BigGraphController.getBigGraphForId(request.id, environment))
  }

  def deriveGraph(request: DeriveBigGraphRequest): GraphBasicData = {
    val sourceGraphs = request.sourceIds.map(
      id => BigGraphController.getBigGraphForId(id, environment))
    val op = FEOperations.getGraphOperation(request.operation)
    basicDataFromGraph(environment.bigGraphManager.deriveGraph(sourceGraphs, op))
  }

  def startingOperations(request: serving.Empty): Seq[FEOperationMeta] =
    FEOperations.getApplicableOperationMetas(Seq())
}

object BigGraphController {
  def getBigGraphForId(id: String, environment: BigGraphEnvironment): BigGraph = {
    environment.bigGraphManager.graphForGUID(UUID.fromString(id)).get
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
