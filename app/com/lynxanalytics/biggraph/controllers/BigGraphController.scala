package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.serving
import scala.collection.mutable
import scala.util.{ Failure, Success, Try }

case class BigGraphRequest(id: String)

// Something with a display name and an internal ID.
case class UIValue(
  id: String,
  title: String)
object UIValue {
  def fromEntity(e: MetaGraphEntity): UIValue = UIValue(e.gUID.toString, e.toString)
}

case class FEOperationMeta(
  id: String,
  title: String,
  parameters: Seq[FEOperationMeta.Param])

object FEOperationMeta {
  case class Param(
    id: String,
    title: String,
    kind: String = "scalar", // vertex-set, edge-bundle, ...
    defaultValue: String = "",
    options: Seq[UIValue] = Seq())
}

case class FEEdgeBundle(
  id: String,
  title: String,
  source: UIValue,
  destination: UIValue)

case class FEVertexSet(
  id: String,
  title: String,
  inEdges: Seq[FEEdgeBundle],
  outEdges: Seq[FEEdgeBundle],
  localEdges: Seq[FEEdgeBundle],
  ops: Seq[FEOperationMeta])

case class FEOperationSpec(
  id: String,
  parameters: Map[String, String])

case class DeriveBigGraphRequest(
  operation: FEOperationSpec)

trait FEOperation {
  val id: String = title
  val title: String
  val starting = parameters.find(_.kind != "scalar").isEmpty
  val parameters: Seq[FEOperationMeta.Param]
  // Use `require()` to perform parameter validation.
  def instance(params: Map[String, String]): MetaGraphOperationInstance
  def isValid(params: Map[String, String]): Boolean = {
    Try(instance(params)) match {
      case Success(_) => true
      case Failure(e: IllegalArgumentException) => false
      case Failure(e) => throw e
    }
  }
  protected def makeInstance(op: MetaGraphOperation, inputs: (Symbol, MetaGraphEntity)*) =
    MetaGraphOperationInstance(op, MetaDataSet(inputs.toMap))
}

class FEOperationRepository {
  val manager = PlaceHolderMetaGraphManagerFactory.get

  def registerOperation(op: FEOperation): Unit = operations += ((op.id, op))

  def getStartingOperationMetas: Seq[FEOperationMeta] = {
    operations.values.toSeq.filter(_.starting).map {
      op => FEOperationMeta(op.id, op.title, op.parameters)
    }
  }

  // Get non-starting operations, based on a current view.
  def getApplicableOperationMetas(vs: VertexSet): Seq[FEOperationMeta] =
    getApplicableOperationMetas(optionsFor(vs))

  def optionsFor(vs: VertexSet): MetaDataSeq = {
    val in = manager.incomingBundles(vs).toSet
    val out = manager.outgoingBundles(vs).toSet
    val neighbors = in.map(_.srcVertexSet) ++ out.map(_.dstVertexSet) - vs
    val strangers = manager.allVertexSets - vs
    // List every vertex set if there are no neighbors.
    val vertexSets = vs +: (if (neighbors.nonEmpty) neighbors.toSeq else strangers.toSeq)
    val edgeBundles = (in ++ out).toSeq
    val vertexAttributes = vertexSets.flatMap(manager.attributes(_))
    val edgeAttributes = edgeBundles.flatMap(manager.attributes(_))
    return MetaDataSeq(
      vertexSets, edgeBundles, vertexAttributes, edgeAttributes)
  }

  def getApplicableOperationMetas(options: MetaDataSeq): Seq[FEOperationMeta] = {
    val vertexSets = options.vertexSets.map(UIValue.fromEntity(_))
    val edgeBundles = options.edgeBundles.map(UIValue.fromEntity(_))
    val vertexAttributes = options.vertexAttributes.map(UIValue.fromEntity(_))
    val edgeAttributes = options.edgeAttributes.map(UIValue.fromEntity(_))
    operations.values.toSeq.filterNot(_.starting).flatMap { op =>
      val params: Seq[FEOperationMeta.Param] = op.parameters.flatMap {
        case p if p.kind == "vertex-set" => vertexSets.headOption.map(
          first => p.copy(options = vertexSets, defaultValue = first.id))
        case p if p.kind == "edge-bundle" => edgeBundles.headOption.map(
          first => p.copy(options = edgeBundles, defaultValue = first.id))
        case p if p.kind == "vertex-attribute" => vertexAttributes.headOption.map(
          first => p.copy(options = vertexAttributes, defaultValue = first.id))
        case p if p.kind == "edge-attribute" => edgeAttributes.headOption.map(
          first => p.copy(options = edgeAttributes, defaultValue = first.id))
        case p => Some(p)
      }
      if (params.length == op.parameters.length) {
        // There is a valid option for every parameter, so this is a legitimate operation.
        Some(FEOperationMeta(op.id, op.title, params))
      } else {
        None
      }
    }
  }

  def getGraphOperationInstance(spec: FEOperationSpec): MetaGraphOperationInstance = {
    operations(spec.id).instance(spec.parameters)
  }

  private val operations = mutable.Map[String, FEOperation]()
}

object FEOperations extends FEOperationRepository {
  import FEOperationMeta.Param

  registerOperation(new FEOperation {
    val title = "Find maximal cliques"
    val parameters = Seq(
      Param("vs", "Vertex set", kind = "vertex-set"),
      Param("es", "Edge bundle", kind = "edge-bundle"),
      Param("min", "Minimum clique size", "3"))
    def instance(params: Map[String, String]) = makeInstance(
      graph_operations.FindMaxCliques(params("min").toInt),
      'vsIn -> manager.vertexSet(params("vs").asUUID),
      'esIn -> manager.edgeBundle(params("es").asUUID))
  })

  registerOperation(new FEOperation {
    val title = "Add constant edge attribute"
    val parameters = Seq(
      Param("eb", "Edge bundle", kind = "edge-bundle"),
      Param("v", "Value", "1"))
    override def instance(params: Map[String, String]) = {
      val edges = manager.edgeBundle(params("eb").asUUID)
      makeInstance(
        graph_operations.AddConstantDoubleEdgeAttribute(params("v").toDouble),
        'edges -> edges, 'ignoredSrc -> edges.srcVertexSet, 'ignoredDst -> edges.dstVertexSet)
    }
  })
}

object PlaceHolderMetaGraphManagerFactory {
  val get = new MetaGraphManager("/tmp/")
}

/**
 * Logic for processing requests
 */

class BigGraphController(environment: BigGraphEnvironment) {
  val manager = PlaceHolderMetaGraphManagerFactory.get

  private def toFE(vs: VertexSet): FEVertexSet = {
    val in = manager.incomingBundles(vs).toSet
    val out = manager.outgoingBundles(vs).toSet
    val local = in & out
    FEVertexSet(
      id = vs.gUID.toString,
      title = vs.toString,
      inEdges = (in -- local).toSeq.map(toFE(_)),
      outEdges = (out -- local).toSeq.map(toFE(_)),
      localEdges = local.toSeq.map(toFE(_)),
      ops = FEOperations.getApplicableOperationMetas(vs))
  }

  private def toFE(eb: EdgeBundle): FEEdgeBundle = {
    FEEdgeBundle(
      id = eb.gUID.toString,
      title = eb.toString,
      source = UIValue.fromEntity(eb.srcVertexSet),
      destination = UIValue.fromEntity(eb.dstVertexSet))
  }

  def getGraph(request: BigGraphRequest): FEVertexSet = {
    toFE(manager.vertexSet(request.id.asUUID))
  }

  def deriveGraph(request: DeriveBigGraphRequest): FEVertexSet = {
    ???
  }

  def startingOperations(request: serving.Empty): Seq[FEOperationMeta] =
    FEOperations.getStartingOperationMetas
}
