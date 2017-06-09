// Frontend operations that create a plot from a table.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_api.Scalar
import com.lynxanalytics.biggraph.graph_api.Scripting._

class VisualizationOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  implicit lazy val manager = env.metaGraphManager
  import Operation.Category
  import Operation.Context
  import OperationParams._

  val VisualizationOperations = Category("Visualization operations", "lightblue", icon = "eye")

  def register(id: String, factory: Context => Operation): Unit = {
    registerOp(
      id, "black_question_mark_ornament",
      VisualizationOperations, List("project"), List("visualization"), factory)
  }

  // A VisualizationOperation takes a Project as input and returns a Visualization as output.
  class VisualizationOperation(context: Operation.Context) extends SimpleOperation(context) {
    assert(
      context.meta.inputs == List("project"),
      s"A VisualizationOperation must input a single project. $context")
    assert(
      context.meta.outputs == List("visualization"),
      s"A VisualizationOperation must output a Visualization. $context"
    )

    protected lazy val project = context.inputs("project").project

    def apply() = ???

    override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
      params.validate()
      Map(
        context.box.output(context.meta.outputs(0)) ->
          BoxOutputState.visualization(
            project,
            params("state"))
      )
    }

    def enabled = FEStatus.enabled

    override val params = new ParameterHolder(context) // No "apply_to" parameters.
    params += VisualizationParam(
      "state",
      "Left-side and right-side UI statuses as JSON")
  }

  register("Create visualization", new VisualizationOperation(_))
}
