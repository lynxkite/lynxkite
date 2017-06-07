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
    registerOp(id, PlotOperations, List("table"), List("plot"), factory)
  }

  // A PlotOperation takes a Table as input and returns a Plot as output.
  class PlotOperation(context: Operation.Context) extends SmartOperation(context) {
    assert(
      context.meta.inputs == List("table"),
      s"A PlotOperation must input a single table. $context")
    assert(
      context.meta.outputs == List("plot"),
      s"A PlotOperation must output a Plot. $context"
    )

    protected lazy val table = tableInput("table")

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

    lazy val parameters = List(
      Code(
        "plot_code",
        "Plot code",
        language = "scala",
        defaultValue = "Vegas(\"My title\").\nwithData(Data).\n"))

    def plotResult() = {
      val plotCode = params("plot_code")
      val op = graph_operations.CreatePlot(plotCode)
      op(op.t, table).result.plot
    }
  }

  register("Create plot", new PlotOperation(_))
}
