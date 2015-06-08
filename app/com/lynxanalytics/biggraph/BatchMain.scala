// BatchMain allows for running lynxkite in batch mode: without web interface, just having it
// execute a workflow.

package com.lynxanalytics.biggraph

import com.lynxanalytics.biggraph.graph_api.SymbolPath
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.controllers.Operation
import com.lynxanalytics.biggraph.controllers.Operations
import com.lynxanalytics.biggraph.controllers.Project
import com.lynxanalytics.biggraph.controllers.WorkflowOperation
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.serving.User
import scala.io.Source

object BatchMain {
  private val commentRE = "#.*".r
  private val scalarRE = raw"GetScalar\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)".r
  private val opsRE = raw"Operations\s*\(\s*'([^']+)'\s*\)".r
  private val opsEnd = "EndOperations"

  def main(args: Array[String]) {
    // We set up log config to use Play's stupid non-default log config file location.
    val logConfigFile =
      scala.util.Properties.envOrNone("KITE_DEPLOYMENT_CONFIG_DIR").get + "/logger.xml"
    scala.util.Properties.setProp("logback.configurationFile", logConfigFile)
    val stageDir = scala.util.Properties.envOrNone("KITE_STAGE_DIR").get
    scala.util.Properties.setProp("LOGGER_HOME", stageDir)

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
    while (lit.hasNext) {
      val line = lit.next()
      val trimmed = line.trim
      trimmed match {
        case commentRE() => ()
        case "" => ()
        case scalarRE(projectNameSpec, scalarName) =>
          val projectName = WorkflowOperation.substituteUserParameters(projectNameSpec, params)
          log.info(s"Computing scalar ${scalarName} on project ${projectName}...")
          Project.validateName(projectName)
          val project = Project(SymbolPath.fromSafeSlashyString(projectName))
          val scalar = project.scalars(scalarName)
          log.info(s"Value of scalar ${scalarName} on project ${projectName}: ${scalar.value}")
          println(s"${projectName}|${scalarName}|${scalar.value}")
        case opsRE(projectNameSpec) =>
          val projectName = WorkflowOperation.substituteUserParameters(projectNameSpec, params)
          Project.validateName(projectName)
          val user = User("Batch User", isAdmin = true)
          val project = Project(SymbolPath.fromSafeSlashyString(projectName))

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
        case _ =>
          System.err.println(s"Cannot parse line: ${line}")
          System.exit(1)
      }
    }
  }
}
