// Creates an HTML output using Python executed on Sphynx.
// For security, this output should not be freely controlled by the user.
// Instead it's used for returning plots encoded in HTML.
package com.lynxanalytics.biggraph.graph_operations

import play.api.libs.json

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._
import DerivePython._

import org.apache.spark

object DeriveHTMLPython extends OpFromJson {
  class Input(fields: Seq[Field]) extends MagicInputSignature {
    val (scalarFields, propertyFields) = fields.partition(_.parent == "graph_attributes")
    val (edgeFields, attrFields) =
      propertyFields.partition(f => f.parent == "es" && (f.name == "src" || f.name == "dst"))
    val edgeParents = edgeFields.map(_.parent).toSet
    val vss = propertyFields.map(f => f.parent -> vertexSet(Symbol(f.parent))).toMap
    val attrs = attrFields.map(f =>
      runtimeTypedVertexAttribute(vss(f.parent), f.fullName, f.tpe.typeTag))
    val srcs = edgeParents.map(p => p -> vertexSet(Symbol("src-for-" + p))).toMap
    val dsts = edgeParents.map(p => p -> vertexSet(Symbol("dst-for-" + p))).toMap
    val ebs = edgeParents.map(p =>
      p -> edgeBundle(srcs(p), dsts(p), idSet = vss(p), name = Symbol("edges-for-" + p))).toMap
    val scalars = scalarFields.map(f => runtimeTypedScalar(f.fullName, f.tpe.typeTag))
  }

  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    DeriveHTMLPython(
      (j \ "code").as[String],
      (j \ "inputFields").as[List[Field]])
  }
}

case class DeriveHTMLPython private[graph_operations] (
    code: String,
    inputFields: List[Field])
    extends TypedMetaGraphOp[Input, ScalarOutput[String]] {
  override def toJson = Json.obj(
    "code" -> code,
    "inputFields" -> inputFields)
  override lazy val inputs = new Input(inputFields)
  def outputMeta(instance: MetaGraphOperationInstance) = {
    implicit val i = instance
    new ScalarOutput[String]
  }
}
