// The code for providing a nice interface to Groovy.

package com.lynxanalytics.biggraph.groovy

import groovy.lang.{ Binding, GroovyShell }
import org.kohsuke.groovy.sandbox
import play.api.libs.json
import scala.collection.JavaConversions

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.frontend_operations.Operations
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.serving.User

case class GroovyContext(
    user: User,
    ops: OperationRepository,
    env: Option[BigGraphEnvironment] = None,
    commandLine: Option[String] = None) {

  implicit lazy val metaManager = env.get.metaGraphManager
  implicit lazy val dataManager = env.get.dataManager
  def normalize(name: String) = name.replace("-", "").toLowerCase
  lazy val normalizedIds = ops.operationIds.map(id => normalize(id) -> id).toMap

  def trustedShell(bindings: (String, AnyRef)*) = {
    val binding = new Binding()
    binding.setProperty("lynx", new GroovyInterface(this))
    for ((k, v) <- bindings) {
      binding.setProperty(k, v)
    }
    new GroovyShell(binding)
  }

  // withUntrustedShell has to unregister the thread-local sandbox after evaluations,
  // so the shell is only accessible within a block.
  def withUntrustedShell(bindings: (String, AnyRef)*)(fn: GroovyShell => Unit): Unit = {
    val binding = new Binding()
    for ((k, v) <- bindings) {
      binding.setProperty(k, v)
    }
    val cc = new org.codehaus.groovy.control.CompilerConfiguration()
    cc.addCompilationCustomizers(new sandbox.SandboxTransformer())
    val shell = new GroovyShell(binding, cc)
    val gs = new GroovySandbox(bindings.toMap.keySet)
    gs.register()
    try fn(shell)
    finally gs.unregister()
  }
}

// The sandbox used in withUntrustedShell.
class GroovySandbox(bindings: Set[String]) extends sandbox.GroovyValueFilter {
  override def filter(receiver: AnyRef): AnyRef = {
    // Make all operations not explicitly allowed below fail.
    // (This includes instance creation and setting properties for example.)
    throw new SecurityException(s"Script tried to execute a disallowed operation ($receiver)")
  }

  override def onMethodCall(
    invoker: sandbox.GroovyInterceptor.Invoker,
    receiver: Any, method: String, args: Object*): Object = {
    // Method calls are only allowed on GroovyWorkflowProject.
    if (receiver.isInstanceOf[GroovyWorkflowProject]) {
      invoker.call(receiver, method, args: _*)
    } else {
      throw new SecurityException(
        s"Script tried to execute a disallowed operation ($receiver.$method)")
    }
  }

  override def onGetProperty(
    invoker: sandbox.GroovyInterceptor.Invoker,
    receiver: Any, property: String): Object = {
    // The bindings can be accessed on the Script object plus any property on GroovyWorkflowProject.
    if (receiver.isInstanceOf[groovy.lang.Script] && bindings.contains(property) ||
      receiver.isInstanceOf[GroovyWorkflowProject]) {
      invoker.call(receiver, property)
    } else {
      throw new SecurityException(
        s"Script tried to execute a disallowed operation ($property.$method)")
    }
  }

  // This is called for all foo[bar] lookups.
  override def onGetArray(
    invoker: sandbox.GroovyInterceptor.Invoker,
    receiver: Any, index: Any): Object = {
    // Allow map lookup to make segmentations accessible.
    if (receiver.isInstanceOf[java.util.Map[_, _]]) {
      invoker.call(receiver, null, index)
    } else {
      throw new SecurityException(
        s"Script tried to execute a disallowed operation ($receiver[$index])")
    }
  }
}

// This is the interface that is visible from trustedShell as "lynx".
class GroovyInterface(ctx: GroovyContext) {
  def project(name: String): GroovyProject = {
    import ctx.metaManager
    val project = ProjectFrame.fromName(name)
    if (!project.exists) {
      project.writeACL = ctx.user.email
      project.readACL = ctx.user.email
      project.initialize
      ctx.ops.apply(
        ctx.user,
        project.subproject,
        Operations.addNotesOperation(s"Created by batch job: ${ctx.commandLine.get}"))
    }
    new GroovyBatchProject(ctx, project.subproject)
  }
}

// The basic interface for running operations against a project.
abstract class GroovyProject(ctx: GroovyContext)
    extends groovy.lang.GroovyObjectSupport {

  protected def applyOperation(id: String, params: Map[String, String]): Unit
  protected def getSegmentations: Map[String, GroovyProject]

  override def invokeMethod(name: String, args: AnyRef): AnyRef = {
    val argArray = args.asInstanceOf[Array[_]]
    val params = if (argArray.nonEmpty) {
      val javaParams = argArray.head.asInstanceOf[java.util.Map[String, AnyRef]]
      JavaConversions.mapAsScalaMap(javaParams).mapValues(_.toString).toMap
    } else Map[String, String]()
    val id = {
      val normalized = ctx.normalize(name)
      assert(ctx.normalizedIds.contains(normalized), s"No such operation: $name")
      ctx.normalizedIds(normalized)
    }
    applyOperation(id, params)
    null
  }

  override def getProperty(name: String): AnyRef = {
    name match {
      case "segmentations" => JavaConversions.mapAsJavaMap(getSegmentations)
      case _ => getMetaClass().getProperty(this, name)
    }
  }
}

// Batch mode creates checkpoints and gives access to scalars/attributes.
class GroovyBatchProject(ctx: GroovyContext, subproject: SubProject)
    extends GroovyProject(ctx) {

  override def getProperty(name: String): AnyRef = {
    name match {
      case "scalars" => JavaConversions.mapAsJavaMap(getScalars)
      case "vertexAttributes" => JavaConversions.mapAsJavaMap(getVertexAttributes)
      case "edgeAttributes" => JavaConversions.mapAsJavaMap(getEdgeAttributes)
      case _ => super.getProperty(name)
    }
  }
  protected def applyOperation(id: String, params: Map[String, String]): Unit = {
    ctx.ops.apply(ctx.user, subproject, FEOperationSpec(id, params))
  }

  protected def getScalars: Map[String, GroovyScalar] = {
    subproject.viewer.scalars.mapValues(new GroovyScalar(ctx, _))
  }

  protected def getVertexAttributes: Map[String, GroovyAttribute] = {
    subproject.viewer.vertexAttributes.mapValues(new GroovyAttribute(ctx, _))
  }

  protected def getEdgeAttributes: Map[String, GroovyAttribute] = {
    subproject.viewer.edgeAttributes.mapValues(new GroovyAttribute(ctx, _))
  }

  protected def getSegmentations: Map[String, GroovyProject] = {
    subproject.viewer.segmentationMap.keys.map { seg =>
      seg -> new GroovyBatchProject(ctx, new SubProject(subproject.frame, subproject.path :+ seg))
    }.toMap
  }
}

class GroovyScalar(ctx: GroovyContext, scalar: Scalar[_]) {
  override def toString = {
    import ctx.dataManager
    scalar.value.toString
  }
}

class GroovyAttribute(ctx: GroovyContext, attr: Attribute[_]) {
  def histogram: String = {
    val drawing = new GraphDrawingController(ctx.env.get)
    val req = HistogramSpec(
      attributeId = attr.gUID.toString,
      vertexFilters = Seq(),
      numBuckets = 10,
      axisOptions = AxisOptions(logarithmic = false))
    val res = drawing.getHistogram(ctx.user, req)
    import com.lynxanalytics.biggraph.serving.ProductionJsonServer._
    json.Json.toJson(res).toString
  }
}

// No checkpointing or entity access in workflow mode.
class GroovyWorkflowProject(ctx: GroovyContext, rootProject: ProjectEditor, path: Seq[String]) extends GroovyProject(ctx) {
  def viewer = rootProject.offspringEditor(path).viewer
  override protected def applyOperation(id: String, params: Map[String, String]): Unit = {
    val opctx = Operation.Context(ctx.user, viewer)
    // Execute the operation.
    val op = ctx.ops.appliedOp(opctx, FEOperationSpec(id, params))
    // Then copy back the state created by the operation. We have to copy at
    // root level, as operations might reach up and modify parent state as well.
    rootProject.state = op.project.rootEditor.state
  }

  override protected def getSegmentations: Map[String, GroovyProject] = {
    viewer.segmentationMap.keys.map { seg =>
      seg -> new GroovyWorkflowProject(ctx, rootProject, path :+ seg)
    }.toMap
  }
}
