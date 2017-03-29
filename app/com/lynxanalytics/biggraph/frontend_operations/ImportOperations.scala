// Frontend operations for importing tables.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import com.lynxanalytics.biggraph.model
import com.lynxanalytics.biggraph.serving.FrontendJson
import play.api.libs.json

class ImportOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  implicit lazy val manager = env.metaGraphManager
  import Operation.Category
  import Operation.Context
  import Operation.Implicits._

  private val tableConnection = TypedConnection("table", BoxOutputKind.Table)
  val ImportOperations = Category("Import operations", "green", icon = "folder-open")

  def register(id: String)(factory: Context => ImportOperation): Unit = {
    registerOp(id, ImportOperations, List(), List(tableConnection), factory)
  }

  import OperationParams._
  import org.apache.spark

  register("Import CSV")(new ImportOperation(_) {
    lazy val parameters = List(
      FileParam("filename", "File"),
      Param("columns", "Columns in file"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Choice("error_handling", "Error handling", List(
        FEOption("FAILFAST", "Fail on any malformed line"),
        FEOption("DROPMALFORMED", "Ignore malformed lines"),
        FEOption("PERMISSIVE", "Salvage malformed lines: truncate or fill with nulls"))),
      Choice("infer", "Infer types", options = FEOption.noyes),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      Code("sql", "SQL"),
      TableParam("imported_table", "Table GUID"))
    def getDataFrame(context: spark.sql.SQLContext) = {
      val errorHandling = params("error_handling")
      val infer = params("infer") == "yes"
      val columns = params("columns")
      assert(
        Set("PERMISSIVE", "DROPMALFORMED", "FAILFAST").contains(errorHandling),
        s"Unrecognized error handling mode: $errorHandling")
      assert(!infer || columns.isEmpty, "List of columns cannot be set when using type inference.")
      val reader = context
        .read
        .format("csv")
        .option("mode", errorHandling)
        .option("delimiter", params("delimiter"))
        .option("inferSchema", if (infer) "true" else "false")
        // We don't want to skip lines starting with #.
        .option("comment", null)
      val readerWithSchema = if (columns.nonEmpty) {
        reader.schema(SQLController.stringOnlySchema(columns.split(",", -1)))
      } else {
        // Read column names from header.
        reader.option("header", "true")
      }
      val hadoopFile = graph_util.HadoopFile(params("filename"))
      hadoopFile.assertReadAllowedFrom(user)
      FileImportValidator.checkFileHasContents(hadoopFile)
      val df = readerWithSchema.load(hadoopFile.resolvedName)
      val sql = params("sql")
      if (sql.isEmpty) df
      else DataManager.sql(context, sql, List("this" -> df))
    }
  })
}
