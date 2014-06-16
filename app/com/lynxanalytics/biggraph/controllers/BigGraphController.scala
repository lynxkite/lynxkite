package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.serving
import scala.collection.mutable
import scala.util.{ Failure, Success, Try }

case class VertexSetRequest(id: String)

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
  parameters: Seq[FEOperationParameterMeta])

case class FEOperationParameterMeta(
    id: String,
    title: String,
    kind: String = "scalar", // vertex-set, edge-bundle, ...
    defaultValue: String = "",
    options: Seq[UIValue] = Seq()) {

  val validKinds = Seq("scalar", "vertex-set", "edge-bundle", "vertex-attribute", "edge-attribute")
  require(validKinds.contains(kind), s"'$kind' is not a valid parameter type")
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

trait FEOperation {
  val id: String = getClass.getName
  val title: String
  val parameters: Seq[FEOperationParameterMeta]
  lazy val starting = parameters.forall(_.kind == "scalar")
  // Use `require()` to perform parameter validation.
  def instance(params: Map[String, String]): MetaGraphOperationInstance
  def isValid(params: Map[String, String]): Boolean = {
    Try(instance(params)) match {
      case Success(_) => true
      case Failure(e: IllegalArgumentException) => false
      case Failure(e) => throw e
    }
  }
}

// An ordered bundle of metadata types.
case class MetaDataSeq(vertexSets: Seq[VertexSet] = Seq(),
                       edgeBundles: Seq[EdgeBundle] = Seq(),
                       vertexAttributes: Seq[VertexAttribute[_]] = Seq(),
                       edgeAttributes: Seq[EdgeAttribute[_]] = Seq())

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
    val vertexSets = if (neighbors.nonEmpty) vs +: neighbors.toSeq else vs +: strangers.toSeq
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
      val params: Seq[FEOperationParameterMeta] = op.parameters.flatMap {
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

  def vertexSet(request: VertexSetRequest): FEVertexSet = {
    toFE(manager.vertexSet(request.id.asUUID))
  }

  def applyOp(request: FEOperationSpec): FEVertexSet = {
    val instance = FEOperations.getGraphOperationInstance(request)
    // Move to an output, or to an input if there is no output.
    val vs = instance.outputs.vertexSets.values.toSeq ++ instance.inputs.vertexSets.values.toSeq
    return toFE(vs.head)
  }

  def startingOperations(request: serving.Empty): Seq[FEOperationMeta] =
    FEOperations.getStartingOperationMetas
}
