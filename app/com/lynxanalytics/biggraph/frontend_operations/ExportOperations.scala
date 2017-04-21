package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.controllers._

class ExportOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  implicit lazy val manager = env.metaGraphManager

  import Operation.Category
  import Operation.Context

  private val tableInput = "table"
  private val exportResultConnection = TypedConnection("result", BoxOutputKind.ExportResult)
  val ExportOperations = Category("Export operations", "blue", icon = "folder-open")

  def register(id: String)(factory: Context => ExportOperation): Unit = {
    registerOp(id, ExportOperations, List(tableInput), List(exportResultConnection), factory)
  }

  import OperationParams._

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

  registerExportToStructuredFile("Export to JSON")("json")
  registerExportToStructuredFile("Export to Parquet")("parquet")
  registerExportToStructuredFile("Export to ORC")("orc")

  def registerExportToStructuredFile(id: String)(format: String) {
    register(id)(new ExportOperationToFile(_) {
      lazy val parameters = List(
        Param("path", "Path", defaultValue = "<download>"),
        NonNegInt("version", "Version", default = 0)
      )

      def exportResult = {
        val op = graph_operations.ExportTableToStructuredFile(
          params("path"), format, params("version").toInt
        )
        op(op.t, table).result.exportResult
      }
    })
  }

}

