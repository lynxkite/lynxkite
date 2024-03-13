// Creates new attributes using Python executed on Sphynx.
package com.lynxanalytics.biggraph.graph_operations

import play.api.libs.json

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

import org.apache.spark

object TextEmbeddingPython extends OpFromJson {
  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    TextEmbeddingPython(
      (j \ "method").as[String],
      (j \ "model_name").as[String],
      (j \ "batch_size").as[String])
  }
}

import TextEmbeddingPython._
case class TextEmbeddingPython(
    method: String,
    modelName: String,
    batchSize: String) // A number or an empty string to use the default.
    extends TypedMetaGraphOp[VertexAttributeInput[String], AttributeOutput[Vector[Double]]] {
  override def toJson = Json.obj(
    "method" -> method,
    "model_name" -> modelName,
    "batch_size" -> batchSize)
  override lazy val inputs = new VertexAttributeInput[String]
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new AttributeOutput[Vector[Double]](inputs.vs.entity)
  }
}
