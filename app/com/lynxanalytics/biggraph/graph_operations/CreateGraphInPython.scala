// Creates a new graph using Python executed on Sphynx.
package com.lynxanalytics.biggraph.graph_operations

import play.api.libs.json

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

import org.apache.spark

object CreateGraphInPython extends OpFromJson {
  class Output(implicit
      instance: MetaGraphOperationInstance,
      fields: Seq[DerivePython.Field]) extends MagicOutput(instance) {
    val vertices = vertexSet
    private val names = fields.map(f => f.parent + "." + f.name)
    val hasEdges = names.contains("es.src") && names.contains("es.dst")
    assert(
      hasEdges || fields.find(_.parent == "es").isEmpty,
      "To define edges you must output 'es.src' and 'es.dst'.")
    val edges = if (hasEdges) edgeBundle(vertices, vertices) else null
    private val parents: Map[String, VertexSet] =
      if (hasEdges) Map("vs" -> vertices.entity, "es" -> edges.idSet) else Map("vs" -> vertices)
    val (scalarFields, attrFields) = fields.partition(_.parent == "scalars")
    val attrs = attrFields.map { f =>
      vertexAttribute(parents(f.parent), f.fullName)(f.tpe.typeTag)
    }
    val scalars = scalarFields.map(f => scalar(f.fullName)(f.tpe.typeTag))
  }

  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    CreateGraphInPython(
      (j \ "code").as[String],
      (j \ "outputFields").as[List[DerivePython.Field]])
  }
}

import CreateGraphInPython._
case class CreateGraphInPython private[graph_operations] (
    code: String,
    outputFields: List[DerivePython.Field])
  extends TypedMetaGraphOp[NoInput, Output] {
  override def toJson = Json.obj(
    "code" -> code,
    "outputFields" -> outputFields)
  override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, outputFields)
}

