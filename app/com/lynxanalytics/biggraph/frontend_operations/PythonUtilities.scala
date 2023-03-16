// Frontend operations for projects.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.Environment

object PythonUtilities {
  import graph_operations.DerivePython._

  val allowed = Environment.envOrElse("KITE_ALLOW_PYTHON", "") match {
    case "yes" => true
    case "no" => false
    case "" => false
    case unexpected => throw new AssertionError(
        s"KITE_ALLOW_PYTHON must be either 'yes' or 'no'. Found '$unexpected'.")
  }
  def assertAllowed() = {
    assert(allowed, "Python code execution is disabled on this server for security reasons.")
  }

  private def toSerializableType(pythonType: String) = {
    pythonType match {
      case "str" => SerializableType.string
      case "float" => SerializableType.double
      case "int" => SerializableType.long
      case "np.ndarray" => SerializableType.vector(SerializableType.double)
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

  // Parses the input list into Fields.
  def inputFields(inputs: Seq[String], project: com.lynxanalytics.biggraph.controllers.ProjectEditor): Seq[Field] = {
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
    inputs.map { i =>
      existingFields.get(i) match {
        case Some(f) => f()
        case None => throw new AssertionError(
            s"No available input called '$i'. Available inputs are: " +
              existingFields.keys.toSeq.sorted.mkString(", "))
      }
    }
  }

  def deriveGraphAttributes(
      code: String,
      inputs: Seq[String],
      outputs: Seq[String],
      project: com.lynxanalytics.biggraph.controllers.ProjectEditor)(
      implicit manager: MetaGraphManager): Unit = {
    val api = Seq("vs", "es", "graph_attributes")
    // Run the operation.
    val op = graph_operations.DerivePython(code, inputFields(inputs, project).toList, outputFields(outputs, api).toList)
    import Scripting._
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
    // builder.toInstance(manager)
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
      project: com.lynxanalytics.biggraph.controllers.ProjectEditor)(
      implicit manager: MetaGraphManager): Unit = {
    val api = Seq("vs", "es", "graph_attributes")
    // Run the operation.
    val res = graph_operations.CreateGraphInPython(code, outputFields(outputs, api).toList)().result
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
    val op = graph_operations.DeriveTablePython(code, outputFields(outputs, api).toList)
    op(op.df, table).result.df
  }

  def deriveTable(
      code: String,
      inputs: Seq[String],
      outputs: Seq[String],
      project: com.lynxanalytics.biggraph.controllers.ProjectEditor)(
      implicit manager: MetaGraphManager): Table = {
    val api = Seq("df")
    // Run the operation.
    val op = graph_operations.DeriveTableFromGraphPython(
      code,
      inputFields(inputs, project).toList,
      outputFields(outputs, api).toList)
    import Scripting._
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
    // builder.toInstance(manager)
    builder.result.df
  }

  def deriveHTML(
      code: String,
      mode: String,
      inputs: Seq[String],
      project: com.lynxanalytics.biggraph.controllers.ProjectEditor)(
      implicit manager: MetaGraphManager): Scalar[String] = {
    val api = Seq("vs", "es", "graph_attributes")
    // Run the operation.
    val op = graph_operations.DeriveHTMLPython(code, mode, inputFields(inputs, project).toList)
    import Scripting._
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
    // builder.toInstance(manager)
    builder.result.sc
  }

  def deriveHTML(
      code: String,
      mode: String,
      table: Table)(
      implicit manager: MetaGraphManager): Scalar[String] = {
    val op = graph_operations.DeriveHTMLTablePython(code, mode)
    op(op.df, table).result.sc
  }
}
