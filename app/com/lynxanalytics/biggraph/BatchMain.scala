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

object BatchMain {
  private val commentRE = "#.*".r
  private val twoArgPtrn = raw"\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)"
  private val scalarRE = ("GetScalar" + twoArgPtrn).r
  private val scalarBenchRE = ("BenchmarkScalar" + twoArgPtrn).r
  private val opsRE = raw"Operations\s*\(\s*'([^']+)'\s*\)".r
  private val opsEnd = "EndOperations"
  private val waitForever = "WaitForever"
  private val resetTimer = "ResetTimer"
  private val histogramRE = ("Histogram" + twoArgPtrn).r

  def getScalarMeta(
    projectName: String, scalarName: String)(
      implicit metaManager: MetaGraphManager): Scalar[_] = {
    val project = ProjectFrame.fromName(projectName)
    project.viewer.scalars(scalarName)
  }

  def getAttributeMeta(
    projectName: String, attributeName: String)(
      implicit metaManager: MetaGraphManager): Attribute[_] = {
    val project = ProjectFrame.fromName(projectName).viewer
    if (project.vertexAttributes.contains(attributeName))
      project.vertexAttributes(attributeName)
    else
      project.edgeAttributes(attributeName)
  }

  def main(args: Array[String]) {
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
    val lit = Source.fromFile(scriptFileName).getLines()
    var timer = System.currentTimeMillis
    while (lit.hasNext) {
      val line = lit.next()
      val trimmed = line.trim
      trimmed match {
        case commentRE() => ()
        case "" => ()
        case scalarRE(projectNameSpec, scalarName) =>
          val projectName = WorkflowOperation.substituteUserParameters(projectNameSpec, params)
          val scalar = getScalarMeta(projectName, scalarName)
          log.info(s"Value of scalar ${scalarName} on project ${projectName}: ${scalar.value}")
          println(s"${projectName}|${scalarName}|${scalar.value}")
        case scalarBenchRE(projectNameSpec, scalarName) =>
          val projectName = WorkflowOperation.substituteUserParameters(projectNameSpec, params)
          val scalar = getScalarMeta(projectName, scalarName)
          val rc0 = dataManager.runtimeContext
          val t0 = System.nanoTime
          val value = scalar.value
          val duration = System.nanoTime - t0
          val rc1 = dataManager.runtimeContext
          assert(
            rc0 == rc1,
            "Runtime context changed while running, benchmark is invalid.\n" +
              s"Before: $rc0\nAfter: $rc1")
          val outRow = Seq(
            rc0.numExecutors,
            rc0.numAvailableCores,
            rc0.workMemoryPerCore,
            duration,
            value)
          println(outRow.mkString(","))
        case opsRE(projectNameSpec) =>
          val projectName = WorkflowOperation.substituteUserParameters(projectNameSpec, params)
          val project = ProjectFrame.fromName(projectName)
          val opRepo = new Operations(env)
          if (!project.exists) {
            // Create project if doesn't yet exist.
            project.writeACL = user.email
            project.readACL = user.email
            project.initialize
            opRepo.apply(
              user,
              SubProject(project, Seq()),
              Operations.addNotesOperation(
                s"Created by batch job: run-kite.sh batch ${args.mkString(" ")}"))
          }

          var opJson = ""
          var line = ""
          while (line != opsEnd) {
            if (line.nonEmpty) opJson += line + "\n"
            assert(lit.hasNext, "Unexpected end of script file, was looking for ${opsEnd}")
            line = lit.next
          }

          for (step <- WorkflowOperation.workflowSteps(opJson, params)) {
            val sp = SubProject(project, step.path)
            opRepo.apply(user, sp, step.op)
          }
        case histogramRE(project, attr) =>
          val req = HistogramSpec(
            attributeId = getAttributeMeta(project, attr).gUID.toString,
            vertexFilters = Seq(),
            numBuckets = 10,
            axisOptions = AxisOptions(logarithmic = false))
          val res = drawing.getHistogram(user, req)
          val j = json.Json.toJson(res).toString
          log.info(s"Histogram for $attr on project $project: $j")
          println(s"$project|$attr|$j")
        case `waitForever` =>
          println("Waiting indefinitely...")
          this.synchronized { this.wait() }
        case `resetTimer` =>
          val t = System.currentTimeMillis
          println(f"Time elapsed: ${0.001 * (t - timer)}%.3f s")
          timer = t
        case _ =>
          System.err.println(s"Cannot parse line: ${line}")
          System.exit(1)
      }
    }
  }
}
