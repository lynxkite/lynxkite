// BatchMain allows for running lynxkite in batch mode: without web interface, just having it
// execute a workflow.

package com.lynxanalytics.biggraph

import scala.io.Source
import play.api.libs.json

import com.lynxanalytics.biggraph.graph_api.SymbolPath
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.frontend_operations.Operations
import com.lynxanalytics.biggraph.controllers.ProjectFrame
import com.lynxanalytics.biggraph.controllers.RootProjectState
import com.lynxanalytics.biggraph.controllers.SubProject
import com.lynxanalytics.biggraph.controllers.WorkflowOperation
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.serving.ProductionJsonServer._
import com.lynxanalytics.biggraph.serving.User

object BatchMain extends App {

  def getScalarMeta(
    projectName: String, scalarName: String)(
      implicit metaManager: MetaGraphManager): Scalar[_] = {
    val project = SubProject.parsePath(projectName).viewer
    project.scalars(scalarName)
  }

  def getAttributeMeta(
    projectName: String, attributeName: String)(
      implicit metaManager: MetaGraphManager): Attribute[_] = {
    val project = SubProject.parsePath(projectName).viewer
    if (project.vertexAttributes.contains(attributeName))
      project.vertexAttributes(attributeName)
    else
      project.edgeAttributes(attributeName)
  }

  val commandLine = s"run-kite.sh batch ${args.mkString(" ")}"
  val env = BigGraphProductionEnvironment
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager = env.dataManager

  if (args.size < 1) {
    System.err.println("""
Usage:
./run-kite.sh batch name_of_script_file [parameter_values]

parameter_values is list of items in the format parameter_name:parameter_value

For example:
./run-kite.sh batch my_script seed:42 input_file_name:data1.csv
""")
    System.exit(-1)
  }
  val scriptFileName :: paramSpecs = args.toList
  val params = paramSpecs
    .map { paramSpec =>
      val colonIdx = paramSpec.indexOf(':')
      assert(
        colonIdx > 0,
        s"Invalid parameter value spec: $paramSpec. " +
          "Parameter values should be specified as name:value")
      (paramSpec.take(colonIdx), paramSpec.drop(colonIdx + 1))
    }
    .toMap

  val drawing = new GraphDrawingController(env)
  val user = User("Batch User", isAdmin = true)
  val ops = new Operations(env)
  def normalize(name: String) = name.replace("-", "").toLowerCase
  val normalizedIds = ops.operationIds.map(id => normalize(id) -> id).toMap

  val imports = new org.codehaus.groovy.control.customizers.ImportCustomizer()
  imports.addImport("Project", classOf[GroovyRootProject].getName)
  val cfg = new org.codehaus.groovy.control.CompilerConfiguration()
  cfg.addCompilationCustomizers(imports)
  val binding = new groovy.lang.Binding()
  val shell = new groovy.lang.GroovyShell(binding, cfg)
  for ((k, v) <- params) {
    binding.setProperty(k, v)
  }
  shell.evaluate(new java.io.File(scriptFileName))
}

abstract class GroovyProject extends groovy.lang.GroovyObjectSupport {
  val subproject: SubProject

  override def getProperty(name: String): AnyRef = {
    import scala.collection.JavaConversions.mapAsJavaMap
    name match {
      case "scalars" => mapAsJavaMap(Map())
      case "segmentations" => mapAsJavaMap(Map())
      case _ => getMetaClass().getProperty(this, name)
    }
  }

  override def invokeMethod(name: String, args: AnyRef): AnyRef = {
    val params = {
      import scala.collection.JavaConversions.mapAsScalaMap
      val javaParams = args.asInstanceOf[Array[_]].head.asInstanceOf[java.util.Map[String, AnyRef]]
      mapAsScalaMap(javaParams).mapValues(_.toString).toMap
    }
    val id = {
      val normalized = BatchMain.normalize(name)
      assert(BatchMain.normalizedIds.contains(normalized), s"No such operation: $name")
      BatchMain.normalizedIds(normalized)
    }
    applyOperation(id, params)
    null
  }

  private def applyOperation(id: String, params: Map[String, String]): Unit = {
    BatchMain.ops.apply(BatchMain.user, subproject, FEOperationSpec(id, params))
  }
}

class GroovyRootProject(name: String) extends GroovyProject {
  import BatchMain.metaManager
  val project = ProjectFrame.fromName(name)
  val subproject = project.subproject

  if (!project.exists) {
    project.writeACL = BatchMain.user.email
    project.readACL = BatchMain.user.email
    project.initialize
    BatchMain.ops.apply(
      BatchMain.user,
      project.subproject,
      Operations.addNotesOperation(s"Created by batch job: ${BatchMain.commandLine}"))
  }
}

class GroovySubProject(val subproject: SubProject) extends GroovyProject
