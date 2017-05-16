// Frontend operations that create a plot from a table.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.controllers._
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

  register("Create plot", new PlotOperation(_) {
    lazy val parameters = List(
      Param("title", "Title"),
      NonNegInt("width", "Plot width", default = 500),
      NonNegInt("height", "Plot height", default = 500),
      Code("plotCode", "Plot code", language = "scala"))

    def plotResult() = {
      val title = params("title")
      val width = params("width").toInt
      val height = params("height").toInt
      val plotCode = params("plotCode")
      val op = graph_operations.CreatePlot(plotCode, title, width, height)
      op(op.t, table).result.plot
    }
  })
}
