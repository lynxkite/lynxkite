// SQLController includes the request handlers for SQL in Kite.
package com.lynxanalytics.biggraph.controllers

import org.apache.spark
import scala.concurrent.Future

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.TableStats
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.table.TableImport
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class DataFrameSpec(project: String, sql: String)
case class SQLQueryRequest(df: DataFrameSpec, maxRows: Int)
case class SQLQueryResult(header: List[String], data: List[List[String]])

case class SQLExportToTableRequest(
  df: DataFrameSpec,
  table: String,
  privacy: String)
case class SQLExportToCSVRequest(
  df: DataFrameSpec,
  path: String,
  header: Boolean,
  delimiter: String,
  quote: String)
case class SQLExportToJsonRequest(
  df: DataFrameSpec,
  path: String)
case class SQLExportToParquetRequest(
  df: DataFrameSpec,
  path: String)
case class SQLExportToORCRequest(
  df: DataFrameSpec,
  path: String)
case class SQLExportToJdbcRequest(
    df: DataFrameSpec,
    jdbcUrl: String,
    table: String,
    mode: String) {
  val validModes = Seq( // Save as the save modes accepted by DataFrameWriter.
    "error", // The table will be created and must not already exist.
    "overwrite", // The table will be dropped (if it exists) and created.
    "append") // The table must already exist.
  assert(validModes.contains(mode), s"Mode ($mode) must be one of $validModes.")
}
case class SQLExportToFileResult(download: Option[serving.DownloadFileRequest])

trait GenericImportRequest {
  val table: String
  val privacy: String
  // Empty list means all columns.
  val columnsToImport: List[String]

  def dataFrame(implicit dataManager: DataManager): spark.sql.DataFrame
  def notes: String

  def restrictedDataFrame(implicit dataManager: DataManager): spark.sql.DataFrame =
    restrictToColumns(dataFrame, columnsToImport)

  private def restrictToColumns(
    full: spark.sql.DataFrame, columnsToImport: Seq[String]): spark.sql.DataFrame = {
    if (columnsToImport.nonEmpty) {
      val columns = columnsToImport.map(spark.sql.functions.column(_))
      full.select(columns: _*)
    } else full
  }
}

case class CSVImportRequest(
    table: String,
    privacy: String,
    files: String,
    // Name of columns. Empty list means to take column names from the first line of the file.
    columnNames: List[String],
    delimiter: String,
    // One of: PERMISSIVE, DROPMALFORMED or FAILFAST
    mode: String,
    columnsToImport: List[String]) extends GenericImportRequest {
  assert(CSVImportRequest.ValidModes.contains(mode), s"Unrecognized CSV mode: $mode")

  def dataFrame(implicit dataManager: DataManager): spark.sql.DataFrame = {
    val reader = dataManager.masterSQLContext
      .read
      .format("com.databricks.spark.csv")
      .option("mode", mode)
      .option("delimiter", delimiter)
      // We don't want to skip lines starting with #
      .option("comment", null)
    val readerWithSchema = if (columnNames.nonEmpty) {
      reader.schema(SQLController.stringOnlySchema(columnNames))
    } else {
      // Read column names from header.
      reader.option("header", "true")
    }
    val hadoopFile = HadoopFile(files)
    // TODO: #2889 (special characters in S3 passwords).
    readerWithSchema.load(hadoopFile.resolvedName)
  }

  def notes = s"Imported from CSV files ${files}."
}
object CSVImportRequest {
  val ValidModes = Set("PERMISSIVE", "DROPMALFORMED", "FAILFAST")
}

case class JdbcImportRequest(
    table: String,
    privacy: String,
    jdbcUrl: String,
    jdbcTable: String,
    keyColumn: String,
    columnsToImport: List[String]) extends GenericImportRequest {

  def dataFrame(implicit dataManager: DataManager): spark.sql.DataFrame = {
    assert(jdbcUrl.startsWith("jdbc:"), "JDBC URL has to start with jdbc:")
    val stats = {
      val connection = java.sql.DriverManager.getConnection(jdbcUrl)
      try TableStats(jdbcTable, keyColumn)(connection)
      finally connection.close()
    }
    val numPartitions = dataManager.runtimeContext.partitionerForNRows(stats.count).numPartitions
    dataManager.masterSQLContext
      .read
      .jdbc(
        jdbcUrl,
        jdbcTable,
        keyColumn,
        stats.minKey,
        stats.maxKey,
        numPartitions,
        new java.util.Properties)
  }

  def notes: String = {
    val uri = new java.net.URI(jdbcUrl.drop("jdbc:".size))
    val urlSafePart = s"${uri.getScheme()}://${uri.getAuthority()}${uri.getPath()}"
    s"Imported from table ${jdbcTable} at ${urlSafePart}."
  }
}

trait FilesWithSchemaImportRequest extends GenericImportRequest {
  val files: String
  val format: String

  def dataFrame(implicit dataManager: DataManager): spark.sql.DataFrame = {
    val hadoopFile = HadoopFile(files)
    dataManager.masterHiveContext.read.format(format).load(hadoopFile.resolvedName)
  }

  def notes = s"Imported from ${format} files ${files}."
}

case class ParquetImportRequest(
    table: String,
    privacy: String,
    files: String,
    columnsToImport: List[String]) extends FilesWithSchemaImportRequest {
  val format = "parquet"
}

case class ORCImportRequest(
    table: String,
    privacy: String,
    files: String,
    columnsToImport: List[String]) extends FilesWithSchemaImportRequest {
  val format = "orc"
}

case class JsonImportRequest(
    table: String,
    privacy: String,
    files: String,
    columnsToImport: List[String]) extends FilesWithSchemaImportRequest {
  val format = "json"
}

case class HiveImportRequest(
    table: String,
    privacy: String,
    hiveTable: String,
    columnsToImport: List[String]) extends GenericImportRequest {

  def dataFrame(implicit dataManager: DataManager): spark.sql.DataFrame = {
    assert(
      dataManager.hiveConfigured,
      "Hive is not configured for this Kite instance. Contact your system administrator.")
    dataManager.masterHiveContext.table(hiveTable)
  }
  def notes = s"Imported from Hive table ${hiveTable}."
}

class SQLController(val env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager: DataManager = env.dataManager
  // We don't want to block the HTTP threads -- we want to return Futures instead. But the DataFrame
  // API is not Future-based, so we need to block some threads. This also avoids #2906.
  implicit val executionContext = ThreadUtil.limitedExecutionContext("SQLController", 100)
  def async[T](func: => T): Future[T] = Future(func)

  def doImport(user: serving.User, request: GenericImportRequest) = async[FEOption] {
    SQLController.saveTable(
      request.restrictedDataFrame,
      request.notes,
      user,
      request.table,
      request.privacy)
  }

  def importCSV(user: serving.User, request: CSVImportRequest) = doImport(user, request)
  def importJdbc(user: serving.User, request: JdbcImportRequest) = doImport(user, request)
  def importParquet(user: serving.User, request: ParquetImportRequest) = doImport(user, request)
  def importORC(user: serving.User, request: ORCImportRequest) = doImport(user, request)
  def importJson(user: serving.User, request: JsonImportRequest) = doImport(user, request)
  def importHive(user: serving.User, request: HiveImportRequest) = doImport(user, request)

  private def dfFromSpec(user: serving.User, spec: DataFrameSpec): spark.sql.DataFrame = {
    val tables = metaManager.synchronized {
      val p = SubProject.parsePath(spec.project)
      assert(p.frame.exists, s"Project ${spec.project} does not exist.")
      p.frame.assertReadAllowedFrom(user)

      val v = p.viewer
      v.allRelativeTablePaths.map {
        path => (path.toString -> Table(path, v))
      }
    }
    // Every query runs in its own SQLContext for isolation.
    val sqlContext = dataManager.newSQLContext()
    for ((tableName, table) <- tables) {
      table.toDF(sqlContext).registerTempTable(tableName)
    }

    log.info(s"Trying to execute query: ${spec.sql}")
    sqlContext.sql(spec.sql)
  }

  def runSQLQuery(user: serving.User, request: SQLQueryRequest) = async[SQLQueryResult] {
    val df = dfFromSpec(user, request.df)

    SQLQueryResult(
      header = df.columns.toList,
      data = df.head(request.maxRows).map {
        row =>
          row.toSeq.map {
            case null => "null"
            case item => item.toString
          }.toList
      }.toList
    )
  }

  def exportSQLQueryToTable(
    user: serving.User, request: SQLExportToTableRequest) = async[Unit] {
    val df = dfFromSpec(user, request.df)
    SQLController.saveTable(
      df, s"From ${request.df.project} by running ${request.df.sql}",
      user, request.table, request.privacy)
  }

  def exportSQLQueryToCSV(
    user: serving.User, request: SQLExportToCSVRequest) = async[SQLExportToFileResult] {
    downloadableExportToFile(
      user,
      request.df,
      request.path,
      "csv",
      Map(
        "delimiter" -> request.delimiter,
        "quote" -> request.quote,
        "nullValue" -> "",
        "header" -> (if (request.header) "true" else "false")),
      stripHeaders = request.header)
  }

  def exportSQLQueryToJson(
    user: serving.User, request: SQLExportToJsonRequest) = async[SQLExportToFileResult] {
    downloadableExportToFile(user, request.df, request.path, "json")
  }

  def exportSQLQueryToParquet(
    user: serving.User, request: SQLExportToParquetRequest) = async[Unit] {
    exportToFile(user, request.df, HadoopFile(request.path), "parquet")
  }

  def exportSQLQueryToORC(
    user: serving.User, request: SQLExportToORCRequest) = async[Unit] {
    exportToFile(user, request.df, HadoopFile(request.path), "orc")
  }

  def exportSQLQueryToJdbc(
    user: serving.User, request: SQLExportToJdbcRequest) = async[Unit] {
    val df = dfFromSpec(user, request.df)
    df.write.mode(request.mode).jdbc(request.jdbcUrl, request.table, new java.util.Properties)
  }

  private def downloadableExportToFile(
    user: serving.User,
    dfSpec: DataFrameSpec,
    path: String,
    format: String,
    options: Map[String, String] = Map(),
    stripHeaders: Boolean = false): SQLExportToFileResult = {
    val file = if (path == "<download>") {
      dataManager.repositoryPath / "exports" / Timestamp.toString + "." + format
    } else {
      HadoopFile(path)
    }
    exportToFile(user, dfSpec, file, format, options)
    val download =
      if (path == "<download>") Some(serving.DownloadFileRequest(file.symbolicName, stripHeaders))
      else None
    SQLExportToFileResult(download)
  }

  private def exportToFile(
    user: serving.User,
    dfSpec: DataFrameSpec,
    file: HadoopFile,
    format: String,
    options: Map[String, String] = Map()): Unit = {
    val df = dfFromSpec(user, dfSpec)
    // TODO: #2889 (special characters in S3 passwords).
    df.write.format(format).options(options).save(file.resolvedName)
  }

}
object SQLController {
  def stringOnlySchema(columns: Seq[String]) = {
    import spark.sql.types._
    StructType(columns.map(StructField(_, StringType, true)))
  }

  def saveTable(
    df: spark.sql.DataFrame,
    notes: String,
    user: serving.User,
    tableName: String,
    privacy: String)(
      implicit metaManager: MetaGraphManager,
      dataManager: DataManager): FEOption = metaManager.synchronized {
    assert(!DirectoryEntry.fromName(tableName).exists, s"Entry '$tableName' already exists.")
    val entry = DirectoryEntry.fromName(tableName)
    entry.assertParentWriteAllowedFrom(user)
    val table = TableImport.importDataFrameAsync(df)
    val checkpoint = table.saveAsCheckpoint(notes)
    val frame = entry.asNewTableFrame(checkpoint)
    frame.setupACL(privacy, user)
    FEOption.titledCheckpoint(checkpoint, frame.name, s"|${Table.VertexTableName}")
  }
}
