package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_util.JDBCUtil
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import com.lynxanalytics.biggraph.model
import com.lynxanalytics.biggraph.serving.FrontendJson
import play.api.libs.json

class ExportOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  implicit lazy val manager = env.metaGraphManager

  import Operation.Category
  import Operation.Context
  import Operation.Implicits._

  private val tableInput = "table"
  private val exportResultConnection = TypedConnection("result", BoxOutputKind.ExportResult)
  val ExportOperations = Category("Export operations", "blue", icon = "folder-open")

  def register(id: String)(factory: Context => ExportOperation): Unit = {
    registerOp(id, ExportOperations, List(tableInput), List(exportResultConnection), factory)
  }

  import OperationParams._
  import org.apache.spark

  register("Export to CSV")(new ExportOperationToFile(_) {
    lazy val parameters = List(
      Param("path", "Path", defaultValue = "<download>"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("quote", "Quote", defaultValue = ""),
      Choice("header", "Include header", FEOption.list("yes", "no")),
      NonNegInt("version", "Version", default = 0)
    )

    def exportResult() = {
      val header = if (params("header") == "yes") true else false
      val op = graph_operations.ExportTableToCSV(
        params("path"), header,
        params("delimiter"), params("quote"),
        params("version").toInt
      )
      op(op.t, table).result.exportResult
    }
  })

  register("Export to JSON")(new ExportOperationToFile(_) {
    lazy val parameters = List(
      Param("path", "Path", defaultValue = "<download>"),
      NonNegInt("version", "Version", default = 0)
    )

    def exportResult() = {
      val op = graph_operations.ExportTableToStructuredFile(
        params("path"), "json", params("version").toInt
      )
      op(op.t, table).result.exportResult
    }
  })

  register("Export to Parquet")(new ExportOperationToFile(_) {
    lazy val parameters = List(
      Param("path", "Path", defaultValue = "<download>"),
      NonNegInt("version", "Version", default = 0)
    )

    def exportResult = {
      val op = graph_operations.ExportTableToStructuredFile(
        params("path"), "parquet", params("version").toInt
      )
      op(op.t, table).result.exportResult
    }
  })

  register("Export to ORC")(new ExportOperationToFile(_) {
    lazy val parameters = List(
      Param("path", "Path", defaultValue = "<download>"),
      NonNegInt("version", "Version", default = 0)
    )

    def exportResult = {
      val op = graph_operations.ExportTableToStructuredFile(
        params("path"), "orc", params("version").toInt
      )
      op(op.t, table).result.exportResult
    }
  })
}

