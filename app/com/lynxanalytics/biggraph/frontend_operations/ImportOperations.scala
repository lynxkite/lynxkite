// Frontend operations for importing data from outside of the workspace.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_util.JDBCUtil
import com.lynxanalytics.biggraph.controllers._

class ImportOperations(env: SparkFreeEnvironment) extends OperationRegistry {
  implicit lazy val manager = env.metaGraphManager
  import Operation.Context
  import Operation.Implicits._

  import Categories.ImportOperations

  override val defaultIcon = "upload"

  def register(id: String)(factory: Context => ImportOperation): Unit = {
    registerOp(id, defaultIcon, ImportOperations, List(), List("table"), factory)
  }

  def register(id: String, inputs: List[String], outputs: List[String])(
    factory: Context => Operation): Unit = {
    registerOp(id, defaultIcon, ImportOperations, inputs, outputs, factory)
  }

  import OperationParams._
  import org.apache.spark

  def simpleFileName(name: String): String = {
    // Drop the hash if this is an upload.
    if (name.startsWith("UPLOAD$")) name.split("/", -1).last.split("\\.", 2).last
    else name.split("/", -1).last
  }

  register("Import CSV")(new ImportOperation(_) {
    params ++= List(
      new DummyParam("last_settings", areSettingsStaleReplyMessage()),
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
      Code("sql", "SQL", language = "sql"),
      ImportedTableParam("imported_table", "Table GUID"))

    override def summary = {
      val fn = simpleFileName(params("filename"))
      s"Import $fn"
    }

    def getRawDataFrame(context: spark.sql.SQLContext) = {
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
      readerWithSchema.load(hadoopFile.resolvedName)
    }
  })

  register("Import JDBC")(new ImportOperation(_) {
    params ++= List(
      new DummyParam("last_settings", areSettingsStaleReplyMessage()),
      Param("jdbc_url", "JDBC URL"),
      Param("jdbc_table", "JDBC table"),
      Param("key_column", "Key column"),
      NonNegInt("num_partitions", "Number of partitions", default = 0),
      Param("partition_predicates", "Partition predicates"),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      Param("connection_properties", "Connection properties"),
      Code("sql", "SQL", language = "sql"),
      ImportedTableParam("imported_table", "Table GUID"))

    def getRawDataFrame(context: spark.sql.SQLContext) = {
      JDBCUtil.read(
        context,
        params("jdbc_url"),
        params("jdbc_table"),
        params("key_column"),
        params("num_partitions").toInt,
        splitParam("partition_predicates").toList,
        splitParam("connection_properties").map { e =>
          val eq = e.indexOf("=")
          assert(eq != -1,
            s"Invalid connection property definition: ${params("connection_properties")}")
          e.take(eq) -> e.drop(eq + 1)
        }.toMap)
    }
  })

  abstract class FileWithSchema(context: Context) extends ImportOperation(context) {
    val format: String
    params ++= List(
      new DummyParam("last_settings", areSettingsStaleReplyMessage()),
      FileParam("filename", "File"),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      Code("sql", "SQL", language = "sql"),
      ImportedTableParam("imported_table", "Table GUID"))

    override def summary = {
      val fn = simpleFileName(params("filename"))
      s"Import $fn"
    }

    def getRawDataFrame(context: spark.sql.SQLContext) = {
      val hadoopFile = graph_util.HadoopFile(params("filename"))
      hadoopFile.assertReadAllowedFrom(user)
      FileImportValidator.checkFileHasContents(hadoopFile)
      context.read.format(format).load(hadoopFile.resolvedName)
    }
  }

  register("Import Parquet")(new FileWithSchema(_) { val format = "parquet" })
  register("Import ORC")(new FileWithSchema(_) { val format = "orc" })
  register("Import JSON")(new FileWithSchema(_) { val format = "json" })

  register("Import from Hive")(new ImportOperation(_) {
    params ++= List(
      new DummyParam("last_settings", areSettingsStaleReplyMessage()),
      FileParam("hive_table", "Hive table"),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      Code("sql", "SQL", language = "sql"),
      ImportedTableParam("imported_table", "Table GUID"))
    def getRawDataFrame(context: spark.sql.SQLContext) = {
      assert(
        DataManager.hiveConfigured,
        "Hive is not configured for this LynxKite instance. Contact your system administrator.")
      context.table(params("hive_table"))
    }
  })

  register("Import snapshot", List(), List("state"))(new SimpleOperation(_) {
    params += Param("path", "Path")
    override def getOutputs() = {
      val snapshot = DirectoryEntry.fromName(params("path")).asSnapshotFrame
      Map(context.box.output("state") -> snapshot.getState)
    }
  })
}
