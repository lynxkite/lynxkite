// BatchMain allows for running lynxkite in batch mode: without web interface, just having it
// execute a workflow.

package com.lynxanalytics.biggraph

import com.lynxanalytics.biggraph.graph_api.SymbolPath
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.controllers.Operation
import com.lynxanalytics.biggraph.controllers.Operations
import com.lynxanalytics.biggraph.controllers.Project
import com.lynxanalytics.biggraph.controllers.WorkflowOperation
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager
import com.lynxanalytics.biggraph.graph_api.Scalar
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.serving.User
import scala.io.Source

object BatchMain {
  private val commentRE = "#.*".r
  private val scalarArgPtrn = raw"\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)"
  private val scalarRE = ("GetScalar" + scalarArgPtrn).r
  private val scalarBenchRE = ("BenchmarkScalar" + scalarArgPtrn).r
  private val opsRE = raw"Operations\s*\(\s*'([^']+)'\s*\)".r
  private val opsEnd = "EndOperations"
  private val waitForever = "WaitForever"
  private val resetTimer = "ResetTimer"

  def getScalarMeta(
    projectName: String, scalarName: String, params: Map[String, String])(
      implicit metaManager: MetaGraphManager): Scalar[_] = {
    Project.validateName(projectName)
    val project = Project.fromName(projectName)
    project.scalars(scalarName)
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
./run-kite.sh batch my_script.json seed:42 input_file_name:data1.csv
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
          val scalar = getScalarMeta(projectName, scalarName, params)
          log.info(s"Value of scalar ${scalarName} on project ${projectName}: ${scalar.value}")
          println(s"${projectName}|${scalarName}|${scalar.value}")
        case scalarBenchRE(projectNameSpec, scalarName) =>
          val projectName = WorkflowOperation.substituteUserParameters(projectNameSpec, params)
          val scalar = getScalarMeta(projectName, scalarName, params)
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
          Project.validateName(projectName)
          val user = User("Batch User", isAdmin = true)
          val project = Project.fromName(projectName)

          if (!Operation.projects.contains(project)) {
            // Create project if doesn't yet exist.
            project.writeACL = user.email
            project.readACL = user.email
            project.notes = s"Created by batch job: run-kite.sh batch ${args.mkString(" ")}"
            project.checkpointAfter("")
          }

          val context = Operation.Context(user, project)

          var opJson = ""
          var line = ""
          while (line != opsEnd) {
            if (line.nonEmpty) opJson += line + "\n"
            assert(lit.hasNext, "Unexpected end of script file, was looking for ${opsEnd}")
            line = lit.next
          }

          val opRepo = new Operations(env)

          for (step <- WorkflowOperation.workflowSteps(opJson, params, context)) {
            val op = opRepo.operationOnSubproject(context.project, step, context.user)
            project.checkpoint(op.summary(step.op.parameters), step) {
              op.validateAndApply(step.op.parameters)
            }
          }
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
