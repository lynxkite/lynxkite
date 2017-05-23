package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.controllers._

class ExportOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  implicit lazy val manager = env.metaGraphManager

  import Operation.Category
  import Operation.Context

  val ExportOperations = Category("Export operations", "blue", icon = "folder-open")

  def register(id: String)(factory: Context => ExportOperation): Unit = {
    registerOp(id, ExportOperations, List("table"), List("exportResult"), factory)
  }

  import OperationParams._

  register("Export to CSV")(new ExportOperationToFile(_) {
    lazy val format = "csv"
    lazy val parameters = List(
      Param("path", "Path", defaultValue = "<auto>"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("quote", "Quote", defaultValue = ""),
      Choice("header", "Include header", FEOption.list("yes", "no")),
      NonNegInt("version", "Version", default = 0)
    )

    def exportResult() = {
      val header = if (params("header") == "yes") true else false
      val path = generatePathIfNeeded(params("path"))
      val op = graph_operations.ExportTableToCSV(
        path, header,
        params("delimiter"), params("quote"),
        params("version").toInt
      )
      op(op.t, table).result.exportResult
    }
  })

  register("Export to JDBC")(new ExportOperation(_) {
    lazy val format = "jdbc"
    lazy val parameters = List(
      Param("jdbcUrl", "JDBC URL"),
      Param("table", "Table"),
      Choice("mode", "Mode", FEOption.list(
        "The table must not exist",
        "Drop the table if it already exists",
        "Insert into an existing table"))
    )

    def exportResult() = {
      val mode = params("mode") match {
        case "The table must not exist" => "error"
        case "Overwrite table if it already exists" => "overwrite"
        case "Insert into an existing table" => "append"
      }
      val op = graph_operations.ExportTableToJdbc(
        params("jdbcUrl"),
        params("table"),
        mode
      )
      op(op.t, table).result.exportResult
    }
  })

  registerExportToStructuredFile("Export to JSON")("json")
  registerExportToStructuredFile("Export to Parquet")("parquet")
  registerExportToStructuredFile("Export to ORC")("orc")

  def registerExportToStructuredFile(id: String)(fileFormat: String) {
    register(id)(new ExportOperationToFile(_) {
      lazy val format = fileFormat
      lazy val parameters = List(
        Param("path", "Path", defaultValue = "<auto>"),
        NonNegInt("version", "Version", default = 0)
      )

      val path = generatePathIfNeeded(params("path"))
      def exportResult = {
        val op = graph_operations.ExportTableToStructuredFile(
          path, format, params("version").toInt
        )
        op(op.t, table).result.exportResult
      }
    })
  }
}

