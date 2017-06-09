// Frontend operations that create a visualization from a project.
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

  // Takes a Project as input and returns a Visualization as output.
  registerOp(
    "Create visualization",
    "black_question_mark_ornament",
    VisualizationOperations,
    List("project"),
    List("visualization"),
    new SimpleOperation(_) {

      protected lazy val project = context.inputs("project").project

      override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
        params.validate()
        Map(
          context.box.output(context.meta.outputs(0)) ->
            BoxOutputState.visualization(
              project,
              params("state"))
        )
      }

      override val params = new ParameterHolder(context) // No "apply_to" parameters.
      params += VisualizationParam(
        "state",
        "Left-side and right-side UI statuses as JSON")
    })

}
