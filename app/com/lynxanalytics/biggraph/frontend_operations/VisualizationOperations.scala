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

  val VisualizationOperations = Category("Visuzalization operations", "lightblue", icon = "eye")

  def register(id: String, factory: Context => Operation): Unit = {
    registerOp(id, VisualizationOperations, List("project"), List("visualization"), factory)
  }

  // A VisualizationOperation takes a Table as input and returns a Plot as output.
  class VisualizationOperation(val context: Operation.Context) extends Operation {
    protected val id = context.meta.operationId

    assert(
      context.meta.inputs == List("project"),
      s"A VisualizationOperation must input a single project. $context")
    assert(
      context.meta.outputs == List("visualization"),
      s"A PlotOperation must output a Plot. $context"
    )

    protected lazy val project = context.inputs("project").project

    def summary: String = "TODO"

    def toFE: FEOperationMeta = FEOperationMeta(
      id,
      Operation.htmlId(id),
      parameters.map { param => param.toFE },
      List(),
      context.meta.categoryId,
      enabled,
      description = context.meta.description)

    def apply() = ???

    def validateParameters(values: Map[String, String]) = {
    }

    override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
      validateParameters(params)

      Map(
        context.box.output(context.meta.outputs(0)) ->
          BoxOutputState.visualization(
            project,
            params("state"))
      )
    }

    def enabled = FEStatus.enabled

    protected lazy val paramValues = context.box.parameters
    protected def params =
      parameters
        .map {
          paramMeta => (paramMeta.id, paramMeta.defaultValue)
        }
        .toMap ++ paramValues

    lazy val parameters = List(
      VisualizationParam(
        "state",
        "Left-side and right-side UI statuses as JSON"))

  }

  register("Create visualization", new VisualizationOperation(_))
}
