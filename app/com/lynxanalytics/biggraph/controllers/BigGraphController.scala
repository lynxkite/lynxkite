package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.serving
import scala.collection.mutable
import scala.util.{ Failure, Success, Try }

case class FEStatus(success: Boolean, failureReason: String = "")
object FEStatus {
  val success = FEStatus(true)
  def failure(failureReason: String) = FEStatus(false, failureReason)
}

case class VertexSetRequest(id: String)

// Something with a display name and an internal ID.
case class UIValue(
  id: String,
  title: String)
object UIValue {
  def fromEntity(e: MetaGraphEntity): UIValue = UIValue(e.gUID.toString, e.toString)
  def seq(list: String*) = list.map(id => UIValue(id, id))
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

  val validKinds = Seq(
    "scalar", "vertex-set", "edge-bundle", "vertex-attribute", "edge-attribute",
    "multi-vertex-attribute", "multi-edge-attribute")
  require(validKinds.contains(kind), s"'$kind' is not a valid parameter type")
}

case class FEEdgeBundle(
  id: String,
  title: String,
  source: UIValue,
  destination: UIValue,
  attributes: Seq[UIValue])

case class FEVertexSet(
  id: String,
  title: String,
  inEdges: Seq[FEEdgeBundle],
  outEdges: Seq[FEEdgeBundle],
  localEdges: Seq[FEEdgeBundle],
  attributes: Seq[UIValue],
  ops: Seq[FEOperationMeta])

case class FEOperationSpec(
  id: String,
  parameters: Map[String, String])

abstract class FEOperation {
  val id: String = getClass.getName
  val title: String
  val parameters: Seq[FEOperationParameterMeta]
  lazy val starting = parameters.forall(_.kind == "scalar")
  def apply(params: Map[String, String]): FEStatus
}

// An ordered bundle of metadata types.
case class MetaDataSeq(vertexSets: Seq[VertexSet] = Seq(),
                       edgeBundles: Seq[EdgeBundle] = Seq(),
                       vertexAttributes: Seq[VertexAttribute[_]] = Seq(),
                       edgeAttributes: Seq[EdgeAttribute[_]] = Seq())

class FEOperationRepository(env: BigGraphEnvironment) {
  implicit val manager = env.metaGraphManager
  implicit val dataManager = env.dataManager

  def registerOperation(op: FEOperation): Unit = {
    assert(!operations.contains(op.id), s"Already registered: ${op.id}")
    operations(op.id) = op
  }

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
      vertexSets.filter(manager.isVisible(_)),
      edgeBundles.filter(manager.isVisible(_)),
      vertexAttributes.filter(manager.isVisible(_)),
      edgeAttributes.filter(manager.isVisible(_)))
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
        case p if p.kind == "multi-vertex-attribute" => Some(p.copy(options = vertexAttributes))
        case p if p.kind == "multi-edge-attribute" => Some(p.copy(options = edgeAttributes))
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

  def applyOp(spec: FEOperationSpec): FEStatus =
    operations(spec.id).apply(spec.parameters)

  private val operations = mutable.Map[String, FEOperation]()
}

/**
 * Logic for processing requests
 */

class BigGraphController(env: BigGraphEnvironment) {
  val manager = env.metaGraphManager
  val operations = new FEOperations(env)

  private def toFE(vs: VertexSet): FEVertexSet = {
    val in = manager.incomingBundles(vs).toSet.filter(manager.isVisible(_))
    val out = manager.outgoingBundles(vs).toSet.filter(manager.isVisible(_))
    val local = in & out

    FEVertexSet(
      id = vs.gUID.toString,
      title = vs.toString,
      inEdges = (in -- local).toSeq.map(toFE(_)),
      outEdges = (out -- local).toSeq.map(toFE(_)),
      localEdges = local.toSeq.map(toFE(_)),
      attributes = manager.attributes(vs).filter(manager.isVisible(_)).map(UIValue.fromEntity(_)),
      ops = operations.getApplicableOperationMetas(vs).sortBy(_.title))
  }

  private def toFE(eb: EdgeBundle): FEEdgeBundle = {
    FEEdgeBundle(
      id = eb.gUID.toString,
      title = eb.toString,
      source = UIValue.fromEntity(eb.srcVertexSet),
      destination = UIValue.fromEntity(eb.dstVertexSet),
      attributes = manager.attributes(eb).filter(manager.isVisible(_)).map(UIValue.fromEntity(_)))
  }

  def vertexSet(request: VertexSetRequest): FEVertexSet = {
    toFE(manager.vertexSet(request.id.asUUID))
  }

  def applyOp(request: FEOperationSpec): FEStatus =
    operations.applyOp(request)

  def startingOperations(request: serving.Empty): Seq[FEOperationMeta] =
    operations.getStartingOperationMetas.sortBy(_.title)

  def startingVertexSets(request: serving.Empty): Seq[UIValue] =
    manager.allVertexSets
      .filter(_.source.inputs.all.isEmpty)
      .filter(manager.isVisible(_))
      .map(UIValue.fromEntity(_)).toSeq
}
