// Frontend operations that create a plot from a table.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_api.Scalar
import com.lynxanalytics.biggraph.graph_api.Scripting._

class PlotOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  implicit lazy val manager = env.metaGraphManager
  import Operation.Category
  import Operation.Context
  import OperationParams._

  val PlotOperations = Category("Plot operations", "lightblue", icon = "bar-chart")

  def register(id: String, factory: Context => Operation): Unit = {
    registerOp(id, PlotOperations, List("table"), List("plotResult"), factory)
  }

  // A PlotOperation takes a Table as input and returns a PlotResult as output.
  class PlotOperation(val context: Operation.Context) extends BasicOperation {
    assert(
      context.meta.inputs == List("table"),
      s"A PlotOperation must input a single table. $context")
    assert(
      context.meta.outputs == List("plotResult"),
      s"A PlotOperation must output a PlotResult. $context"
    )

    protected lazy val table = tableInput("table")

    def apply() = ???

    protected def makeOutput(plotResult: Scalar[String]): Map[BoxOutput, BoxOutputState] = {
      Map(context.box.output(
        context.meta.outputs(0)) -> BoxOutputState.plot(plotResult))
    }

    def getOutputs(): Map[BoxOutput, BoxOutputState] = {
      validateParameters(params)
      makeOutput(plotResult)
    }

    def enabled = FEStatus.enabled

    lazy val parameters = List(
      Param("title", "Title"),
      Code("plotCode", "Plot code", language = "scala"))

    def plotResult() = {
      val title = params("title")
      val plotCode = params("plotCode")
      val op = graph_operations.CreatePlot(plotCode, title)
      op(op.t, table).result.plot
    }
  }

  register("Create plot", new PlotOperation(_))
}
