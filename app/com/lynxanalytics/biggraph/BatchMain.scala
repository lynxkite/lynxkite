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

    val scriptFileName = args(0)
    val projectName = args(1)
    val params = args
      .drop(2)
      .map { paramSpec =>
        val colonIdx = paramSpec.indexOf(':')
        assert(
          colonIdx > 0,
          "Invalid parameter value spec. Parameter values should be specified as name:value")
        (paramSpec.take(colonIdx), paramSpec.drop(colonIdx + 1))
      }
      .toMap

    Project.validateName(projectName)
    val user = User("Batch User", isAdmin = true)
    val project = Project(projectName)
    project.writeACL = user.email
    project.readACL = user.email
    project.notes = ""
    project.checkpointAfter("")
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
