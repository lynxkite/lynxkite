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

  private val tableConnection = TypedConnection("table", BoxOutputKind.Table)
  private val exportResultConnection = TypedConnection("export_result", BoxOutputKind.ExportResult)
  val ExportOperations = Category("Export operations", "blue", icon = "folder-open")

  def register(id: String)(factory: Context => ExportOperation): Unit = {
    registerOp(id, ExportOperations, List(tableConnection), List(exportResultConnection), factory)
  }

  import OperationParams._
  import org.apache.spark

  register("Export to CSV")(new ExportOperation(_) {
    lazy val parameters = List(???)
    def apply() = ???
  })

  register("Export to Parquet")(new ExportOperation(_) {
    lazy val parameters = List(???)
    def apply() = ???
  })

  register("Export to JSON")(new ExportOperation(_) {
    lazy val parameters = List(???)
    def apply() = ???
  })

  register("Export to ORC")(new ExportOperation(_) {
    lazy val parameters = List(???)
    def apply() = ???
  })
}

