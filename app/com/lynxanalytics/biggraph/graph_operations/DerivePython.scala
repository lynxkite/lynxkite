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
    val (scalarFields, attrFields) = fields.partition(_.parent == "scalars")
    val vss = attrFields.map(f => f.parent -> vertexSet(Symbol(f.parent))).toMap
    val attrs = attrFields.map(f =>
      runtimeTypedVertexAttribute(vss(f.parent), f.fullName, f.tpe.typeTag))
    val scalars = scalarFields.map(f => runtimeTypedScalar(f.fullName, f.tpe.typeTag))
  }
  class Output(implicit
      instance: MetaGraphOperationInstance,
      inputs: Input, fields: Seq[Field]) extends MagicOutput(instance) {
    val (scalarFields, attrFields) = fields.partition(_.parent == "scalars")
    val attrs = attrFields.map(f =>
      vertexAttribute(inputs.vss(f.parent).entity, f.fullName)(f.tpe.typeTag))
    val scalars = scalarFields.map(f => scalar(f.fullName)(f.tpe.typeTag))
  }

  private def toSerializableType(pythonType: String) = {
    pythonType match {
      case "str" => SerializableType.string
      case "float" => SerializableType.double
      case _ => throw new AssertionError(s"Unknown type: $pythonType")
    }
  }

  def run(
    code: String, inputs: Seq[String], outputs: Seq[String],
    project: com.lynxanalytics.biggraph.controllers.ProjectEditor)(
    implicit
    manager: MetaGraphManager): Unit = {
    // Parse the output list into Fields.
    val api = Seq("vs", "es", "scalars")
    val outputDeclaration = raw"(\w+)\.(\w+)\s*:\s*(\w+)".r
    val outputFields = outputs.map {
      case outputDeclaration(parent, name, tpe) =>
        assert(
          api.contains(parent),
          s"Invalid output: '$parent.$name'. Valid groups are: " + api.mkString(", "))
        Field(parent, name, toSerializableType(tpe))
      case output => throw new AssertionError(
        s"Output declarations must be formatted like 'vs.my_attr: str'. Got '$output'.")
    }
    // Parse the input list into Fields.
    val existingFields = project.vertexAttributes.map {
      case (name, attr) => s"vs.$name" -> Field("vs", name, SerializableType(attr.typeTag))
    }.toMap ++ project.edgeAttributes.map {
      case (name, attr) => s"es.$name" -> Field("es", name, SerializableType(attr.typeTag))
    }.toMap ++ project.scalars.map {
      case (name, s) => s"scalars.$name" -> Field("scalars", name, SerializableType(s.typeTag))
    }.toMap
    val inputFields = inputs.map { i =>
      existingFields.get(i) match {
        case Some(f) => f
        case None => throw new AssertionError(
          s"No available input called '$i'. Available inputs are: " +
            existingFields.keys.toSeq.sorted.mkString(", "))
      }
    }
    // Run the operation.
    val op = DerivePython(code, inputFields.toList, outputFields.toList)
    import Scripting._
    val builder = InstanceBuilder(op)
    for ((f, i) <- op.attrFields.zipWithIndex) {
      val attr = f.parent match {
        case "vs" => project.vertexAttributes(f.name)
        case "es" => project.edgeAttributes(f.name)
      }
      builder(op.attrs(i), attr)
      builder(op.vss(f.parent), attr.vertexSet)
    }
    for ((f, i) <- op.scalarFields.zipWithIndex) {
      builder(op.scalars(i), project.scalars(f.name))
    }
    builder.toInstance(manager)
    val res = builder.result
    // Save the outputs into the project.
    for ((f, i) <- res.attrFields.zipWithIndex) {
      f.parent match {
        case "vs" => project.newVertexAttribute(f.name, res.attrs(i))
        case "es" => project.newEdgeAttribute(f.name, res.attrs(i))
      }
    }
    for ((f, i) <- res.scalarFields.zipWithIndex) {
      project.newScalar(f.name, res.scalars(i))
    }
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

