package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.controllers._

class ExportOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  implicit lazy val manager = env.metaGraphManager

  override val defaultIcon = "download"

  import Operation.Context

  import Categories.ExportOperations

  def register(id: String)(factory: Context => ExportOperation): Unit = {
    registerOp(id, defaultIcon, ExportOperations, List("table"), List("exported"), factory)
  }

  import OperationParams._

  register("Export to CSV")(new ExportOperationToFile(_) {
    lazy val format = "csv"
    params ++= List(
      Param("path", "Path", defaultValue = "<auto>"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("quote", "Quote", defaultValue = "\""),
      Choice("quote_all", "Quote all strings", FEOption.noyes),
      Choice("header", "Include header", FEOption.yesno),
      Param("escape", "Escape character", defaultValue = "\\"),
      Param("null_value", "Null value", defaultValue = ""),
      Param("date_format", "Date format", defaultValue = "yyyy-MM-dd"),
      Param("timestamp_format", "Timestamp format", defaultValue = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
      Choice("drop_leading_white_space", "Drop leading white space", FEOption.noyes),
      Choice("drop_trailing_white_space", "Drop trailing white space", FEOption.noyes),
      NonNegInt("version", "Version", default = 0),
      Choice("save_mode", "Save mode", FEOption.saveMode),
      Choice("for_download", "Export for download", FEOption.noyes))

    def exportResult() = {
      val header = if (params("header") == "yes") true else false
      val quoteAll = if (params("quote_all") == "yes") true else false
      val dropLeadingWhiteSpace = if (params("drop_leading_white_space") == "yes") true else false
      val dropTrailingWhiteSpace = if (params("drop_trailing_white_space") == "yes") true else false
      val path = generatePathIfNeeded(params("path"))
      val op = graph_operations.ExportTableToCSV(
        path = path,
        header = header,
        delimiter = params("delimiter"),
        quote = params("quote"),
        quoteAll = quoteAll,
        escape = params("escape"),
        nullValue = params("null_value"),
        dateFormat = params("date_format"),
        timestampFormat = params("timestamp_format"),
        dropLeadingWhiteSpace = dropLeadingWhiteSpace,
        dropTrailingWhiteSpace = dropTrailingWhiteSpace,
        version = params("version").toInt,
        saveMode = params("save_mode"),
        forDownload = params("for_download") == "yes")
      op(op.t, table).result.exportResult
    }
  })

  register("Export to JDBC")(new ExportOperation(_) {
    lazy val format = "jdbc"
    params ++= List(
      Param("jdbc_url", "JDBC URL"),
      Param("jdbc_table", "Table"),
      Choice("mode", "Mode", FEOption.list(
        "The table must not exist",
        "Drop the table if it already exists",
        "Insert into an existing table")))

    def exportResult() = {
      val mode = params("mode") match {
        case "The table must not exist" => "error"
        case "Drop the table if it already exists" => "overwrite"
        case "Insert into an existing table" => "append"
      }
      val op = graph_operations.ExportTableToJdbc(
        params("jdbc_url"),
        params("jdbc_table"),
        mode)
      op(op.t, table).result.exportResult
    }
  })

  register("Export to Hive")(new ExportOperation(_) {
    lazy val format = "hive"
    params ++= List(
      Param("table", "Table"),
      Choice("mode", "Mode", FEOption.list(
        "The table must not exist",
        "Drop the table if it already exists",
        "Insert into an existing table")),
      Choice("partition_by", "Partition by",
        FEOption.list(table.schema.map(_.name).toList),
        multipleChoice = true))

    def exportResult() = {
      val mode = params("mode") match {
        case "The table must not exist" => "error"
        case "Drop the table if it already exists" => "overwrite"
        case "Insert into an existing table" => "append"
      }
      val partitions =
        if (params("partition_by").isEmpty) Array[String]()
        else params("partition_by").split(",")
      val op = graph_operations.ExportTableToHive(
        params("table"),
        mode,
        partitions.toList)
      op(op.t, table).result.exportResult
    }
  })

  registerExportToStructuredFile("Export to JSON")("json")
  registerExportToStructuredFile("Export to Parquet")("parquet")
  registerExportToStructuredFile("Export to ORC")("orc")
  registerExportToStructuredFile("Export to AVRO")("avro")
  registerExportToStructuredFile("Export to Delta")("delta")

  def registerExportToStructuredFile(id: String)(fileFormat: String) {
    register(id)(new ExportOperationToFile(_) {
      lazy val format = fileFormat
      params ++= List(
        Param("path", "Path", defaultValue = "<auto>"),
        NonNegInt("version", "Version", default = 0),
        Choice("save_mode", "Save mode", FEOption.saveMode),
        Choice("for_download", "Export for download", FEOption.noyes))

      val path = generatePathIfNeeded(params("path"))
      def exportResult = {
        val op = graph_operations.ExportTableToStructuredFile(
          path, format, params("version").toLong, params("save_mode"), params("for_download") == "yes")
        op(op.t, table).result.exportResult
      }
    })
  }

  registerOp("Save to snapshot", defaultIcon, ExportOperations, List("state"), List(), new TriggerableOperation(_) {
    params ++= List(
      Param("path", "Path"),
      TriggerBoxParam("save", "Save to snapshot", "Snapshot created."))

    private def getState() = context.inputs("state")

    override def trigger(wc: WorkspaceController, gdc: GraphDrawingController) = {
      import scala.concurrent.ExecutionContext.Implicits.global
      gdc.getComputeBoxResult(getGUIDs("state"))
        .map(_ => wc.createSnapshotFromState(user, params("path"), getState))
    }
  })
}
