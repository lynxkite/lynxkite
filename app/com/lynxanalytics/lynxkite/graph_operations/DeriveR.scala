// All operations for running R on Sphynx.
package com.lynxanalytics.lynxkite.graph_operations

import play.api.libs.json
import org.apache.spark

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.spark_util.Implicits._
import com.lynxanalytics.lynxkite.Environment
import com.lynxanalytics.lynxkite.controllers.BoxOutputKind

import Scripting._
import DerivePython._

case class DeriveR private[graph_operations] (
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

object DeriveR extends OpFromJson {
  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    DeriveR(
      (j \ "code").as[String],
      (j \ "inputFields").as[List[Field]],
      (j \ "outputFields").as[List[Field]])
  }

  val allowed = Environment.envOrElse("KITE_ALLOW_R", "") match {
    case "yes" => true
    case "no" => false
    case "" => false
    case unexpected => throw new AssertionError(
        s"KITE_ALLOW_R must be either 'yes' or 'no'. Found '$unexpected'.")
  }
  def assertAllowed() = {
    assert(allowed, "R code execution is disabled on this server for security reasons.")
  }

  private def toSerializableType(pythonType: String) = {
    pythonType match {
      case "character" => SerializableType.string
      case "double" => SerializableType.double
      case "integer" => SerializableType.long
      case "vector" => SerializableType.vector(SerializableType.double)
      case _ => throw new AssertionError(s"Unknown type: $pythonType")
    }
  }

  // Parses the output list into Fields.
  def outputFields(outputs: Seq[String], api: Seq[String]): Seq[Field] = {
    val outputDeclaration = raw"(\w+)\.(\w+)\s*:\s*([a-zA-Z0-9.]+)".r
    outputs.map {
      case outputDeclaration(parent, name, tpe) =>
        assert(
          api.contains(parent),
          s"Invalid output: '$parent.$name'. Valid groups are: " + api.mkString(", "))
        Field(parent, name, toSerializableType(tpe))
      case output => throw new AssertionError(
          s"Output declarations must be formatted like 'vs.my_attr: str'. Got '$output'.")
    }
  }

  def derive(
      code: String,
      inputs: Seq[String],
      outputs: Seq[String],
      project: com.lynxanalytics.lynxkite.controllers.ProjectEditor)(
      implicit manager: MetaGraphManager): Unit = {
    val api = Seq("vs", "es", "graph_attributes")
    // Parse the input list into Fields.
    val existingFields: Map[String, () => Field] = project.vertexAttributes.map {
      case (name, attr) => s"vs.$name" -> (() => Field("vs", name, SerializableType(attr.typeTag)))
    }.toMap ++ project.edgeAttributes.map {
      case (name, attr) => s"es.$name" -> (() => Field("es", name, SerializableType(attr.typeTag)))
    }.toMap ++ project.scalars.map {
      case (name, s) =>
        s"graph_attributes.$name" -> (() => Field("graph_attributes", name, SerializableType(s.typeTag)))
    }.toMap + {
      "es.src" -> (() => Field("es", "src", SerializableType.long))
    } + {
      "es.dst" -> (() => Field("es", "dst", SerializableType.long))
    }
    val inputFields: Seq[Field] = inputs.map { i =>
      existingFields.get(i) match {
        case Some(f) => f()
        case None => throw new AssertionError(
            s"No available input called '$i'. Available inputs are: " +
              existingFields.keys.toSeq.sorted.mkString(", "))
      }
    }
    // Run the operation.
    val op = DeriveR(code, inputFields.toList, outputFields(outputs, api).toList)
    val builder = InstanceBuilder(op)
    for ((f, i) <- op.attrFields.zipWithIndex) {
      val attr = f.parent match {
        case "vs" => project.vertexAttributes(f.name)
        case "es" => project.edgeAttributes(f.name)
      }
      builder(op.attrs(i), attr)
    }
    for (f <- op.edgeParents) {
      builder(op.ebs(f), project.edgeBundle)
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

  def create(
      code: String,
      outputs: Seq[String],
      project: com.lynxanalytics.lynxkite.controllers.ProjectEditor)(
      implicit manager: MetaGraphManager): Unit = {
    val api = Seq("vs", "es", "graph_attributes")
    // Run the operation.
    val res = CreateGraphInR(code, outputFields(outputs, api).toList)().result
    project.vertexSet = res.vertices
    project.edgeBundle = res.edges
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

  def inferInputs(code: String, kind: String): Seq[String] = {
    val api = kind match {
      case BoxOutputKind.Project => Seq("vs", "es", "graph_attributes")
      case BoxOutputKind.Table => Seq("df")
    }
    val outputs = inferOutputs(code, kind).map(_.replaceFirst(":.*", "")).toSet
    val mentions = api.flatMap { parent =>
      val a = s"\\b$parent\\.\\w+".r.findAllMatchIn(code).map(_.matched).toSeq
      val b = s"""\\b$parent\\s*\\[\\s*['"](\\w+)['"]\\s*\\]""".r
        .findAllMatchIn(code).map(m => s"$parent.${m.group(1)}").toSeq
      a ++ b
    }.toSet
    (mentions -- outputs).toSeq.sorted
  }
  def inferOutputs(code: String, kind: String): Seq[String] = {
    val api = kind match {
      case BoxOutputKind.Project => Seq("vs", "es", "graph_attributes")
      case BoxOutputKind.Table => Seq("df")
    }
    api.flatMap { parent =>
      val a = s"""\\b$parent\\.\\w+\\s*:\\s*[a-zA-Z0-9.]+""".r
        .findAllMatchIn(code).map(_.matched).toSeq
      val b = s"""\\b$parent\\s*\\[\\s*['"](\\w+)['"]\\s*\\]\\s*:\\s*([a-zA-Z0-9.]+)""".r
        .findAllMatchIn(code).map(m => s"$parent.${m.group(1)}: ${m.group(2)}").toSeq
      a ++ b
    }.sorted
  }

  def deriveTable(
      code: String,
      table: Table,
      outputs: Seq[String])(
      implicit manager: MetaGraphManager): Table = {
    val api = Seq("df")
    // Run the operation.
    val op = DeriveTableR(code, outputFields(outputs, api).toList)
    op(op.df, table).result.df
  }
}

object CreateGraphInR extends OpFromJson {
  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    CreateGraphInR(
      (j \ "code").as[String],
      (j \ "outputFields").as[List[Field]])
  }
}

case class CreateGraphInR private[graph_operations] (
    code: String,
    outputFields: List[Field])
    extends TypedMetaGraphOp[NoInput, CreateGraphInPython.Output] {
  override def toJson = Json.obj(
    "code" -> code,
    "outputFields" -> outputFields)
  override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new CreateGraphInPython.Output()(instance, outputFields)
}

object DeriveTableR extends OpFromJson {
  def fromJson(j: JsValue): TypedMetaGraphOp.Type = {
    DeriveTableR(
      (j \ "code").as[String],
      (j \ "outputFields").as[List[Field]])
  }
}

case class DeriveTableR private[graph_operations] (
    code: String,
    outputFields: List[Field])
    extends TypedMetaGraphOp[DeriveTablePython.Input, DeriveTablePython.Output]
    with UnorderedSphynxOperation {
  override def toJson = Json.obj(
    "code" -> code,
    "outputFields" -> outputFields)
  override lazy val inputs = new DeriveTablePython.Input
  def outputMeta(i: MetaGraphOperationInstance) = new DeriveTablePython.Output()(i, outputFields)
}
