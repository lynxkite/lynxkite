// BatchMain allows for running lynxkite in batch mode: without web interface, just having it
// execute a workflow.

package com.lynxanalytics.biggraph

import com.lynxanalytics.biggraph.controllers.Operation
import com.lynxanalytics.biggraph.controllers.Operations
import com.lynxanalytics.biggraph.controllers.Project
import com.lynxanalytics.biggraph.controllers.WorkflowOperation
import com.lynxanalytics.biggraph.serving.User
import scala.io.Source

object BatchMain {
  def main(args: Array[String]) {
    val env = BigGraphProductionEnvironment
    implicit val metaManager = env.metaGraphManager

    if (args.size < 2) {
      println("""
Usage:
./run-kite.sh batch name_of_script_file name_of_project [parameter_values]

parameter_values is list of items in the format parameter_name:parameter_value

For example:
./run-kite.sh batch my_script.json MyProject seed:42 input_file_name:data1.csv
""")
      System.exit(-1)
    }
    val scriptFileName :: projectName :: paramSpecs = args.toList
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

    Project.validateName(projectName)
    val user = User("Batch User", isAdmin = true)
    val project = Project(projectName)

    if (!Operation.projects.contains(project)) {
      // Create project if doesn't yet exist.
      project.writeACL = user.email
      project.readACL = user.email
      project.notes = ""
      project.checkpointAfter("")
    }

    val context = Operation.Context(user, project)

    val opJson = Source.fromFile(scriptFileName).mkString

    val opRepo = new Operations(env)

    for (step <- WorkflowOperation.workflowSteps(opJson, params, context)) {
      val op = opRepo.operationOnSubproject(context.project, step, context.user)
      project.checkpoint(op.summary(step.op.parameters), step) {
        op.validateAndApply(step.op.parameters)
      }
    }
  }
}
