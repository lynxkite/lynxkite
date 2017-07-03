// Frontend operations that create a visualization from a project.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_api.Scalar
import com.lynxanalytics.biggraph.graph_api.Scripting._
import org.apache.spark
import play.api.libs.json

class VisualizationOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  implicit lazy val manager = env.metaGraphManager
  import Operation.Context
  import OperationParams._

  import Categories.VisualizationOperations

  def register(id: String, icon: String, inputs: List[String], outputs: List[String])(factory: Context => Operation): Unit = {
    registerOp(id, icon, VisualizationOperations, inputs, outputs, factory)
  }

  // Takes a Project as input and returns a Visualization as output.
  register(
    "Graph visualization",
    "black_question_mark_ornament",
    List("project"),
    List("visualization"))(new SimpleOperation(_) {

      protected lazy val project = context.inputs("project").project

      override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
        params.validate()
        Map(
          context.box.output(context.meta.outputs(0)) ->
            BoxOutputState.visualization(
              VisualizationState.fromString(
                params("state"),
                project)))
      }

      override val params = new ParameterHolder(context) // No "apply_to" parameters.
      import UIStatusSerialization._
      params += VisualizationParam(
        "state",
        "Left-side and right-side UI statuses as JSON",
        json.Json.toJson(TwoSidedUIStatus(
          left = Some(UIStatus.default.copy(graphMode = Some("sampled"))),
          right = None)).toString)
    })

  register(
    "Custom plot",
    "apl_functional_symbol_quad_up_caret",
    List("table"),
    List("plot"))(new PlotOperation(_))

  // A PlotOperation takes a Table as input and returns a Plot as output.
  class PlotOperation(context: Operation.Context) extends SmartOperation(context) {
    assert(
      context.meta.inputs == List("table"),
      s"A PlotOperation must input a single table. $context")
    assert(
      context.meta.outputs == List("plot"),
      s"A PlotOperation must output a Plot. $context"
    )

    protected val table = tableLikeInput("table").asTable

    def apply() = ???

    protected def makeOutput(plotResult: Scalar[String]): Map[BoxOutput, BoxOutputState] = {
      Map(context.box.output(
        context.meta.outputs(0)) -> BoxOutputState.plot(plotResult))
    }

    override def getOutputs() = {
      params.validate()
      makeOutput(plotResult)
    }

    def enabled = FEStatus.enabled

    def bestX = table.schema
      .find(_.dataType == spark.sql.types.StringType)
      .map(_.name).getOrElse("X")
    def bestY = table.schema
      .find(_.dataType == spark.sql.types.DoubleType)
      .map(_.name).getOrElse("Y")

    params += Code(
      "plot_code",
      "Plot code",
      language = "scala",
      defaultValue =
        s"""Vegas()\n  .withData(table)\n  .encodeX("$bestX", Nom)\n  .encodeY("$bestY", Quant)\n  .mark(Bar)""")

    def plotResult() = {
      val plotCode = params("plot_code")
      val op = graph_operations.CreatePlot(plotCode)
      op(op.t, table).result.plot
    }
  }
}
