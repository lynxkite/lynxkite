// Frontend operations for importing data from outside of the workspace.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_util.{ JDBCUtil, Neo4jUtil }
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.spark_util.SQLHelper

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
    // Include the directory if the filename is a wildcard.
    else if (name.split("/", -1).last.contains("*")) name.split("/", -1).takeRight(2).mkString("/")
    else name.split("/", -1).last
  }

  register("Import CSV")(new ImportOperation(_) {
    params ++= List(
      FileParam("filename", "File"),
      Param(
        "columns", "Columns in file", placeholder = "Leave empty to read header.",
        group = "Advanced settings"),
      Code("delimiter", "Delimiter", defaultValue = ",", group = "Advanced settings", language = ""),
      Code("quote", "Quote character", defaultValue = "\"", group = "Advanced settings", language = ""),
      Code("escape", "Escape character", defaultValue = "\\", group = "Advanced settings", language = ""),
      Code("null_value", "Null value", defaultValue = "", group = "Advanced settings", language = ""),
      Code("date_format", "Date format", defaultValue = "yyyy-MM-dd", group = "Advanced settings", language = ""),
      Code(
        "timestamp_format", "Timestamp format", defaultValue = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        group = "Advanced settings", language = ""),
      Choice(
        "ignore_leading_white_space", "Ignore leading white space", options = FEOption.noyes,
        group = "Advanced settings"),
      Choice(
        "ignore_trailing_white_space", "Ignore trailing white space", options = FEOption.noyes,
        group = "Advanced settings"),
      Code("comment", "Comment character", defaultValue = "", group = "Advanced settings", language = ""),
      Choice(
        "error_handling", "Error handling", List(
          FEOption("FAILFAST", "Fail on any malformed line"),
          FEOption("DROPMALFORMED", "Ignore malformed lines"),
          FEOption("PERMISSIVE", "Salvage malformed lines: truncate or fill with nulls")),
        group = "Advanced settings"),
      Choice("infer", "Infer types", options = FEOption.noyes, group = "Advanced settings"),
      Param(
        "imported_columns", "Columns to import", placeholder = "Leave empty to import all columns.",
        group = "Advanced settings"),
      Param("limit", "Limit", group = "Advanced settings"),
      Code("sql", "SQL", language = "sql", group = "Advanced settings"),
      ImportedTableParam("imported_table"),
      new DummyParam("last_settings", ""))

    override def summary = {
      if (params("filename").nonEmpty) {
        val fn = simpleFileName(params("filename"))
        s"Import $fn"
      } else "Import CSV"
    }

    def getRawDataFrame(context: spark.sql.SQLContext) = {
      val errorHandling = params("error_handling")
      val infer = params("infer") == "yes"
      val ignoreLeadingWhiteSpace = params("ignore_leading_white_space") == "yes"
      val ignoreTrailingWhiteSpace = params("ignore_trailing_white_space") == "yes"
      val columns = params("columns")
      assert(
        Set("PERMISSIVE", "DROPMALFORMED", "FAILFAST").contains(errorHandling),
        s"Unrecognized error handling mode: $errorHandling")
      assert(!infer || columns.isEmpty, "List of columns cannot be set when using type inference.")
      val reader = context
        .read
        .format("csv")
        .option("mode", errorHandling)
        .option("sep", params("delimiter"))
        .option("quote", params("quote"))
        .option("escape", params("escape"))
        .option("nullValue", params("null_value"))
        .option("dateFormat", params("date_format"))
        .option("timestampFormat", params("timestamp_format"))
        .option("ignoreLeadingWhiteSpace", if (ignoreLeadingWhiteSpace) "true" else "false")
        .option("ignoreTrailingWhiteSpace", if (ignoreTrailingWhiteSpace) "true" else "false")
        .option("comment", params("comment"))
        .option("inferSchema", if (infer) "true" else "false")
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
      Param("jdbc_url", "JDBC URL"),
      Param("jdbc_table", "JDBC table"),
      Param("key_column", "Key column"),
      NonNegInt("num_partitions", "Number of partitions", default = 0),
      Param("partition_predicates", "Partition predicates"),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      Param("connection_properties", "Connection properties"),
      Code("sql", "SQL", language = "sql"),
      ImportedTableParam("imported_table"),
      new DummyParam("last_settings", ""))

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
          assert(
            eq != -1,
            s"Invalid connection property definition: ${params("connection_properties")}")
          e.take(eq) -> e.drop(eq + 1)
        }.toMap)
    }
  })

  register("Import Neo4j")(new ImportOperation(_) {
    params ++= List(
      Param("node_label", "Node label"),
      Param("relationship_type", "Relationship type"),
      NonNegInt("num_partitions", "Number of partitions", default = 0),
      Choice("infer", "Infer types", options = FEOption.noyes),
      Code("sql", "SQL", language = "sql"),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      ImportedTableParam("imported_table"),
      new DummyParam("last_settings", ""))

    def getRawDataFrame(context: spark.sql.SQLContext) = {
      Neo4jUtil.read(
        context,
        params("node_label"),
        params("relationship_type"),
        splitParam("imported_columns").toSet,
        params("infer") == "yes",
        params("limit"),
        params("num_partitions").toInt)
    }
  })

  abstract class FileWithSchema(context: Context) extends ImportOperation(context) {
    val format: String
    params ++= List(
      FileParam("filename", "File"),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      Code("sql", "SQL", language = "sql"),
      ImportedTableParam("imported_table"),
      new DummyParam("last_settings", ""))

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
      Param("hive_table", "Hive table"),
      Param("imported_columns", "Columns to import"),
      Param("limit", "Limit"),
      Code("sql", "SQL", language = "sql"),
      ImportedTableParam("imported_table"),
      new DummyParam("last_settings", ""))
    def getRawDataFrame(context: spark.sql.SQLContext) = {
      assert(
        SparkDomain.hiveConfigured,
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

  register("Import union of table snapshots", List(), List("table"))(new SimpleOperation(_) {
    params += Param("paths", "Paths")
    override def getOutputs() = {
      val paths = params("paths").split(",")
      val tables = paths.map { path => DirectoryEntry.fromName(path).asSnapshotFrame.getState().table }
      val schema = tables.head.schema
      for (table <- tables) {
        SQLHelper.assertTableHasCorrectSchema(table, schema)
      }
      val protoTables = tables.zipWithIndex.map { case (table, i) => i.toString -> ProtoTable(table) }.toMap
      val sql = (0 until protoTables.size).map { i => s"select * from `$i`" }.mkString(" union all ")
      val result = graph_operations.ExecuteSQL.run(sql, protoTables)
      Map(context.box.output("table") -> BoxOutputState.from(result))
    }
  })

  register("Import well-known graph dataset", List(), List("graph"))(
    new ProjectOutputOperation(_) {
      params += Choice("name", "Name", options = FEOption.list(
        "Cora", "CiteSeer", "Karate Club", "PubMed"))
      override def summary = {
        val fn = params("name")
        s"Import $fn"
      }
      def enabled = FEStatus.enabled
      def apply() = {
        val g = graph_operations.PyTorchGeometricDataset(params("name"))().result
        project.vertexSet = g.vs
        project.edgeBundle = g.es
        project.newVertexAttribute("x", g.x)
        project.newVertexAttribute("y", g.y)
      }
    })
}
