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

case class CSVImportRequest(
    table: String,
    privacy: String,
    files: String,
    // Name of columns. Empty list means to take column names from the first line of the file.
    columnNames: List[String],
    delimiter: String,
    // One of: PERMISSIVE, DROPMALFORMED or FAILFAST
    mode: String) {
  assert(CSVImportRequest.ValidModes.contains(mode), s"Unrecognized CSV mode: $mode")
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
  // Empty list means all columns.
  columnsToImport: List[String])

trait FilesWithSchemaImportRequest {
  val table: String
  val privacy: String
  val files: String
  val columnsToImport: List[String]
  val format: String
}

case class ParquetImportRequest(
    table: String,
    privacy: String,
    files: String,
    // Empty list means all columns.
    columnsToImport: List[String]) extends FilesWithSchemaImportRequest {
  val format = "parquet"
}

case class ORCImportRequest(
    table: String,
    privacy: String,
    files: String,
    // Empty list means all columns.
    columnsToImport: List[String]) extends FilesWithSchemaImportRequest {
  val format = "orc"
}

class SQLController(val env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager: DataManager = env.dataManager
  // We don't want to block the HTTP threads -- we want to return Futures instead. But the DataFrame
  // API is not Future-based, so we need to block some threads. This also avoids #2906.
  implicit val executionContext = ThreadUtil.limitedExecutionContext("SQLController", 100)
  def async[T](func: => T): Future[T] = Future(func)

  def importCSV(user: serving.User, request: CSVImportRequest) = async[FEOption] {
    val reader = dataManager.masterSQLContext
      .read
      .format("com.databricks.spark.csv")
      .option("mode", request.mode)
      .option("delimiter", request.delimiter)
      // We don't want to skip lines starting with #
      .option("comment", null)
    val readerWithSchema = if (request.columnNames.nonEmpty) {
      reader.schema(SQLController.stringOnlySchema(request.columnNames))
    } else {
      // Read column names from header.
      reader.option("header", "true")
    }
    val hadoopFile = HadoopFile(request.files)
    // TODO: #2889 (special characters in S3 passwords).
    val df = readerWithSchema.load(hadoopFile.resolvedName)
    SQLController.saveTable(
      df, s"Imported from CSV files ${request.files}.",
      user, request.table, request.privacy)
  }

  def restrictToColumns(
    full: spark.sql.DataFrame, columnsToImport: Seq[String]): spark.sql.DataFrame = {
    if (columnsToImport.nonEmpty) {
      val columns = columnsToImport.map(spark.sql.functions.column(_))
      full.select(columns: _*)
    } else full
  }

  def importJdbc(user: serving.User, request: JdbcImportRequest) = async[FEOption] {
    val jdbcUrl = request.jdbcUrl
    assert(jdbcUrl.startsWith("jdbc:"), "JDBC URL has to start with jdbc:")
    val stats = {
      val connection = java.sql.DriverManager.getConnection(jdbcUrl)
      try TableStats(request.jdbcTable, request.keyColumn)(connection)
      finally connection.close()
    }
    val numPartitions = dataManager.runtimeContext.partitionerForNRows(stats.count).numPartitions
    val fullTable = dataManager.masterSQLContext
      .read
      .jdbc(
        jdbcUrl,
        request.jdbcTable,
        request.keyColumn,
        stats.minKey,
        stats.maxKey,
        numPartitions,
        new java.util.Properties)
    val df = restrictToColumns(fullTable, request.columnsToImport)
    // We don't want to put passwords and the likes in the notes.
    val uri = new java.net.URI(jdbcUrl.drop(5))
    val urlSafePart = s"${uri.getScheme()}://${uri.getAuthority()}${uri.getPath()}"
    SQLController.saveTable(
      df, s"Imported from table ${request.jdbcTable} at ${urlSafePart}.",
      user, request.table, request.privacy)
  }

  def importFromFilesWithSchema(
    user: serving.User,
    request: FilesWithSchemaImportRequest) =
    async[FEOption] {
      val hadoopFile = HadoopFile(request.files)
      val df = restrictToColumns(
        dataManager.masterSQLContext.read.format(request.format).load(hadoopFile.resolvedName),
        request.columnsToImport)
      SQLController.saveTable(
        df,
        s"Imported from ${request.format} files ${request.files}.",
        user,
        request.table,
        request.privacy)
    }

  def importParquet(user: serving.User, request: ParquetImportRequest) =
    importFromFilesWithSchema(user, request)

  def importORC(user: serving.User, request: ORCImportRequest) =
    importFromFilesWithSchema(user, request)

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
    assert(!DirectoryEntry.fromName(tableName).exists, s"$tableName already exists.")
    val entry = DirectoryEntry.fromName(tableName)
    entry.assertParentWriteAllowedFrom(user)
    val table = TableImport.importDataFrameAsync(df)
    val checkpoint = table.saveAsCheckpoint(notes)
    val frame = entry.asNewTableFrame(checkpoint)
    frame.setupACL(privacy, user)
    FEOption.titledCheckpoint(checkpoint, frame.name, s"|${Table.VertexTableName}")
  }
}
