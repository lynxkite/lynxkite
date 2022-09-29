// Creates a new graph using Python executed on Sphynx.
package com.lynxanalytics.lynxkite.graph_operations

import play.api.libs.json

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.Implicits._

import org.apache.spark

object CreateGraphInPython extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance, fields: Seq[DerivePython.Field])
      extends MagicOutput(instance) {
    val vertices = vertexSet
    private val names = fields.map(f => f.parent + "." + f.name)
    val edges = edgeBundle(vertices, vertices)
    private val parents = Map[String, VertexSet]("vs" -> vertices.entity, "es" -> edges.idSet)
    val (scalarFields, attrFields) = fields.partition(_.parent == "graph_attributes")
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
