// Creates new attributes using Python executed on Sphynx.
package com.lynxanalytics.biggraph.graph_operations

import play.api.libs.json

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

import org.apache.spark

object DerivePython extends OpFromJson {
  import scala.language.existentials
  case class Field(parent: String, name: String, tpe: SerializableType[_]) {
    def fullName = Symbol(s"$parent.$name")
  }
  // A custom json.Format is needed because of SerializableType[_].
  implicit val fField = new json.Format[Field] {
    def reads(j: json.JsValue): json.JsResult[Field] =
      json.JsSuccess(Field(
        (j \ "parent").as[String],
        (j \ "name").as[String],
        SerializableType.fromJson(j \ "tpe")))
    def writes(f: Field): json.JsValue =
      json.Json.obj("parent" -> f.parent, "name" -> f.name, "tpe" -> f.tpe.toJson)
  }

  class Input(fields: Seq[Field]) extends MagicInputSignature {
    val (scalarFields, propertyFields) = fields.partition(_.parent == "graph_attributes")
    val (edgeFields, attrFields) = propertyFields.partition(
      f => f.parent == "es" && (f.name == "src" || f.name == "dst"))
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
  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: Input, fields: Seq[Field]) extends MagicOutput(instance) {
    val (scalarFields, attrFields) = fields.partition(_.parent == "graph_attributes")
    val attrs = attrFields.map { f =>
      inputs.vss.get(f.parent) match {
        case Some(vs) => vertexAttribute(vs.entity, f.fullName)(f.tpe.typeTag)
        case None => throw new AssertionError(
          s"Cannot produce output for '${f.parent}' when we only have inputs for "
            + inputs.vss.keys.toSeq.sorted.mkString(", "))
      }
    }
    val scalars = scalarFields.map(f => scalar(f.fullName)(f.tpe.typeTag))
  }

  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    DerivePython(
      (j \ "code").as[String],
      (j \ "inputFields").as[List[Field]],
      (j \ "outputFields").as[List[Field]])
  }
}

import DerivePython._
case class DerivePython private[graph_operations] (
    code: String,
    inputFields: List[Field],
    outputFields: List[Field])
  extends TypedMetaGraphOp[Input, Output] {
  override def toJson = Json.obj(
    "code" -> code,
    "inputFields" -> inputFields,
    "outputFields" -> outputFields)
  override lazy val inputs = new Input(inputFields)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs, outputFields)
}

