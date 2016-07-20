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
import com.lynxanalytics.biggraph.serving.User
import com.lynxanalytics.biggraph.table.TableImport
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import play.api.libs.json

trait ViewRecipe {
  def createDataFrame(user: serving.User,
    context: SQLContext)(implicit dataManager: DataManager, metaManager: MetaGraphManager): spark.sql.DataFrame
}
object DataFrameSpec {
  // Utilities for testing.
  def local(project: String, sql: String) =
    new DataFrameSpec(isGlobal = false, directory = None, project = Some(project), sql = sql)
  def global(directory: String, sql: String) =
    new DataFrameSpec(isGlobal = true, directory = Some(directory), project = None, sql = sql)
}
case class DataFrameSpec(isGlobal: Boolean = false, directory: Option[String], project: Option[String], sql: String) extends ViewRecipe {
  override def createDataFrame(user: User, context: SQLContext)(implicit dataManager: DataManager, metaManager: MetaGraphManager): DataFrame = {
    if (isGlobal) globalSQL(user, context)
    else projectSQL(user, context)
  }

  // Finds the names of tables from string
  private def findTablesFromQuery(query: String): List[String] = {
    val split = query.split("`", -1)
    Iterator.range(start = 1, end = split.length, step = 2).map(split(_)).toList
  }

  // Creates a DataFrame from a global level SQL query.
  private def globalSQL(user: serving.User, context: SQLContext)(implicit dataManager: DataManager, metaManager: MetaGraphManager): spark.sql.DataFrame =
    metaManager.synchronized {
      assert(project.isEmpty,
        "The project field in the DataFrameSpec must be empty for global SQL queries.")

      val directoryName = directory.get
      val directoryPrefix = if (directoryName == "") "" else directoryName + "/"
      val givenTableNames = findTablesFromQuery(sql)
      // Maps the relative table names used in the sql query with the global name
      val tableNames = givenTableNames.map(name => (name, directoryPrefix + name)).toMap
      // Separates tables depending on whether they are tables in projects or imported tables without assigned projects
      val (projectTableNames, tableOrViewNames) = tableNames.partition(_._2.contains('|'))

      // For the assumed project tables we select those whose corresponding project really exists and
      // is accessible to the user
      val usedProjectNames = projectTableNames.mapValues(_.split('|').head)
      val usedProjects = usedProjectNames.mapValues(DirectoryEntry.fromName(_))
      val goodProjectViewers = usedProjects.collect {
        case (name, proj) if proj.isProject && proj.readAllowedFrom(user) => (name, proj.asProjectFrame.viewer)
      }

      val goodTablePathsInGoodProjects = goodProjectViewers.flatMap {
        case (name, proj) => proj.allRelativeTablePaths.find(_.path == name.split('|').tail.toSeq)
          .map { path => ((name, path), proj) }
      }

      val projectTables = goodTablePathsInGoodProjects.map {
        case ((name, path), proj) => (name, Table(path, proj))
      }

      val tableOrViewFrames = tableOrViewNames.mapValues(DirectoryEntry.fromName(_))
      val goodTablesOrViews = tableOrViewFrames.filter {
        case (name, entry) => entry.exists && entry.readAllowedFrom(user)
      }

      val (goodTables, goodViews) = goodTablesOrViews.partition(_._2.isTable)
      val importedTables = goodTables.mapValues(_.asTableFrame.table)
      val importedViews = goodViews.mapValues(a =>
        a.asViewFrame().getRecipe
      )
      queryTables(sql, projectTables ++ importedTables, user, context, importedViews)
    }

  // Executes an SQL query at the project level.
  private def projectSQL(user: serving.User, context: SQLContext)(implicit dataManager: DataManager, metaManager: MetaGraphManager): spark.sql.DataFrame = {
    val tables = metaManager.synchronized {
      assert(directory.isEmpty,
        "The directory field in the DataFrameSpec must be empty for local SQL queries.")
      val p = SubProject.parsePath(project.get)
      assert(p.frame.exists, s"Project $project does not exist.")
      p.frame.assertReadAllowedFrom(user)

      val v = p.viewer
      v.allRelativeTablePaths.map {
        path => (path.toString -> Table(path, v))
      }
    }
    queryTables(sql, tables, user, context)
  }

  private def queryTables(
    sql: String,
    tables: Iterable[(String, Table)], user: serving.User,
    context: SQLContext,
    viewRecipes: Iterable[(String, ViewRecipe)] = List())(implicit dataManager: DataManager, metaManager: MetaGraphManager): spark.sql.DataFrame = {
    for ((name, table) <- tables) {
      table.toDF(context).registerTempTable(name)
    }
    for ((name, recipe) <- viewRecipes) {
      recipe.createDataFrame(user, context).registerTempTable(name)
    }
    log.info(s"Trying to execute query: ${sql}")
    context.sql(sql)
  }
}
case class SQLQueryRequest(dfSpec: DataFrameSpec, maxRows: Int)
case class SQLQueryResult(header: List[String], data: List[List[String]])

case class SQLExportToTableRequest(
  dfSpec: DataFrameSpec,
  table: String,
  privacy: String)
case class SQLExportToCSVRequest(
  dfSpec: DataFrameSpec,
  path: String,
  header: Boolean,
  delimiter: String,
  quote: String)
case class SQLExportToJsonRequest(
  dfSpec: DataFrameSpec,
  path: String)
case class SQLExportToParquetRequest(
  dfSpec: DataFrameSpec,
  path: String)
case class SQLExportToORCRequest(
  dfSpec: DataFrameSpec,
  path: String)
case class SQLExportToJdbcRequest(
    dfSpec: DataFrameSpec,
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

trait GenericImportRequest extends ViewRecipe {
  val table: String
  val privacy: String
  // Empty list means all columns.
  val columnsToImport: List[String]
  protected val needHiveContext = false
  protected def dataFrame(user: serving.User, context: SQLContext)(
    implicit dataManager: DataManager): spark.sql.DataFrame
  def notes: String

  def restrictedDataFrame(user: serving.User,
                          context: SQLContext)(implicit dataManager: DataManager): spark.sql.DataFrame =
    restrictToColumns(dataFrame(user, context), columnsToImport)

  def defaultContext()(implicit dataManager: DataManager): SQLContext =
    if (needHiveContext) dataManager.masterHiveContext else dataManager.masterSQLContext

  private def restrictToColumns(
    full: spark.sql.DataFrame, columnsToImport: Seq[String]): spark.sql.DataFrame = {
    if (columnsToImport.nonEmpty) {
      val columns = columnsToImport.map(spark.sql.functions.column(_))
      full.select(columns: _*)
    } else full
  }

  override def createDataFrame(user: serving.User,
                               context: SQLContext)(
                                 implicit dataManager: DataManager, metaManager: MetaGraphManager): spark.sql.DataFrame =
    restrictedDataFrame(user, context)
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
    infer: Boolean,
    columnsToImport: List[String]) extends GenericImportRequest {
  assert(CSVImportRequest.ValidModes.contains(mode), s"Unrecognized CSV mode: $mode")
  assert(!infer || columnNames.isEmpty, "List of columns cannot be set when using type inference.")

  def dataFrame(user: serving.User, context: SQLContext)(implicit dataManager: DataManager): spark.sql.DataFrame = {
    val reader = context
      .read
      .format("com.databricks.spark.csv")
      .option("mode", mode)
      .option("delimiter", delimiter)
      .option("inferSchema", if (infer) "true" else "false")
      // We don't want to skip lines starting with #
      .option("comment", null)

    val readerWithSchema = if (columnNames.nonEmpty) {
      reader.schema(SQLController.stringOnlySchema(columnNames))
    } else {
      // Read column names from header.
      reader.option("header", "true")
    }
    val hadoopFile = HadoopFile(files)
    hadoopFile.assertReadAllowedFrom(user)
    FileImportValidator.checkFileHasContents(hadoopFile)
    // TODO: #2889 (special characters in S3 passwords).
    readerWithSchema.load(hadoopFile.resolvedName)
  }

  def notes = s"Imported from CSV files ${files}."
}
object CSVImportRequest extends FromJson[CSVImportRequest] {
  val ValidModes = Set("PERMISSIVE", "DROPMALFORMED", "FAILFAST")
  import com.lynxanalytics.biggraph.serving.FrontendJson.fCSVImportRequest
  override def fromJson(j: JsValue): CSVImportRequest = json.Json.fromJson(j).get
}

object FileImportValidator {
  def checkFileHasContents(hadoopFile: HadoopFile): Unit = {
    assert(hadoopFile.list.map(f => f.getContentSummary.getSpaceConsumed).sum > 0,
      s"No data was found at '${hadoopFile.symbolicName}' (no or empty files).")
  }
}

case class JdbcImportRequest(
    table: String,
    privacy: String,
    jdbcUrl: String,
    jdbcTable: String,
    keyColumn: String,
    columnsToImport: List[String]) extends GenericImportRequest {

  def dataFrame(user: serving.User, context: SQLContext)(implicit dataManager: DataManager): spark.sql.DataFrame = {
    assert(jdbcUrl.startsWith("jdbc:"), "JDBC URL has to start with jdbc:")
    val stats = {
      val connection = java.sql.DriverManager.getConnection(jdbcUrl)
      try TableStats(jdbcTable, keyColumn)(connection)
      finally connection.close()
    }
    val numPartitions = dataManager.runtimeContext.partitionerForNRows(stats.count).numPartitions
    context
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

object JdbcImportRequest extends FromJson[JdbcImportRequest] {
  import com.lynxanalytics.biggraph.serving.FrontendJson.fJdbcImportRequest
  override def fromJson(j: JsValue): JdbcImportRequest = json.Json.fromJson(j).get
}

trait FilesWithSchemaImportRequest extends GenericImportRequest {
  val files: String
  val format: String
  override val needHiveContext = true
  def dataFrame(user: serving.User, context: SQLContext)(implicit dataManager: DataManager): spark.sql.DataFrame = {
    val hadoopFile = HadoopFile(files)
    hadoopFile.assertReadAllowedFrom(user)
    FileImportValidator.checkFileHasContents(hadoopFile)
    context.read.format(format).load(hadoopFile.resolvedName)
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

object ParquetImportRequest extends FromJson[ParquetImportRequest] {
  import com.lynxanalytics.biggraph.serving.FrontendJson.fParquetImportRequest
  override def fromJson(j: JsValue): ParquetImportRequest = json.Json.fromJson(j).get
}

case class ORCImportRequest(
    table: String,
    privacy: String,
    files: String,
    columnsToImport: List[String]) extends FilesWithSchemaImportRequest {
  val format = "orc"
}

object ORCImportRequest extends FromJson[ORCImportRequest] {
  import com.lynxanalytics.biggraph.serving.FrontendJson.fORCImportRequest
  override def fromJson(j: JsValue): ORCImportRequest = json.Json.fromJson(j).get
}

case class JsonImportRequest(
    table: String,
    privacy: String,
    files: String,
    columnsToImport: List[String]) extends FilesWithSchemaImportRequest {
  val format = "json"
}

object JsonImportRequest extends FromJson[JsonImportRequest] {
  import com.lynxanalytics.biggraph.serving.FrontendJson.fJsonImportRequest
  override def fromJson(j: JsValue): JsonImportRequest = json.Json.fromJson(j).get
}

case class HiveImportRequest(
    table: String,
    privacy: String,
    hiveTable: String,
    columnsToImport: List[String]) extends GenericImportRequest {

  override val needHiveContext = true
  def dataFrame(user: serving.User, context: SQLContext)(implicit dataManager: DataManager): spark.sql.DataFrame = {
    assert(
      dataManager.hiveConfigured,
      "Hive is not configured for this Kite instance. Contact your system administrator.")
    context.table(hiveTable)
  }
  def notes = s"Imported from Hive table ${hiveTable}."
}

object HiveImportRequest extends FromJson[HiveImportRequest] {
  import com.lynxanalytics.biggraph.serving.FrontendJson.fHiveImportRequest
  override def fromJson(j: JsValue): HiveImportRequest = json.Json.fromJson(j).get
}

class SQLController(val env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager: DataManager = env.dataManager
  // We don't want to block the HTTP threads -- we want to return Futures instead. But the DataFrame
  // API is not Future-based, so we need to block some threads. This also avoids #2906.
  implicit val executionContext = ThreadUtil.limitedExecutionContext("SQLController", 100)
  def async[T](func: => T): Future[T] = Future(func)

  def doImport[T <: GenericImportRequest: json.Writes](user: serving.User, request: T): FEOption =
    SQLController.saveTable(
      request.restrictedDataFrame(user, request.defaultContext()),
      request.notes,
      user,
      request.table,
      request.privacy,
      importConfig = Some(TypedJson.createFromWriter(request).as[json.JsObject]))

  def saveView[T <: GenericImportRequest: json.Writes](user: serving.User, request: T): FEOption = {
    SQLController.saveView(
      request.notes,
      user,
      request.table,
      request.privacy, request)
  }

  import com.lynxanalytics.biggraph.serving.FrontendJson._
  def importCSV(user: serving.User, request: CSVImportRequest) = doImport(user, request)
  def importJdbc(user: serving.User, request: JdbcImportRequest) = doImport(user, request)
  def importParquet(user: serving.User, request: ParquetImportRequest) = doImport(user, request)
  def importORC(user: serving.User, request: ORCImportRequest) = doImport(user, request)
  def importJson(user: serving.User, request: JsonImportRequest) = doImport(user, request)
  def importHive(user: serving.User, request: HiveImportRequest) = doImport(user, request)

  def createViewCSV(user: serving.User, request: CSVImportRequest) = saveView(user, request)
  def createViewJdbc(user: serving.User, request: JsonImportRequest) = saveView(user, request)
  def createViewParquet(user: serving.User, request: ParquetImportRequest) = saveView(user, request)
  def createViewORC(user: serving.User, request: ORCImportRequest) = saveView(user, request)
  def createViewJson(user: serving.User, request: JsonImportRequest) = saveView(user, request)
  def createViewHive(user: serving.User, request: HiveImportRequest) = saveView(user, request)

  def runSQLQuery(user: serving.User, request: SQLQueryRequest) = async[SQLQueryResult] {
    val df = request.dfSpec.createDataFrame(user, SQLController.defaultContext(user))
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
    val df = request.dfSpec.createDataFrame(user, SQLController.defaultContext(user))

    SQLController.saveTable(
      df, s"From ${request.dfSpec.project} by running ${request.dfSpec.sql}",
      user, request.table, request.privacy)
  }

  def exportSQLQueryToCSV(
    user: serving.User, request: SQLExportToCSVRequest) = async[SQLExportToFileResult] {
    downloadableExportToFile(
      user,
      request.dfSpec,
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
    downloadableExportToFile(user, request.dfSpec, request.path, "json")
  }

  def exportSQLQueryToParquet(
    user: serving.User, request: SQLExportToParquetRequest) = async[Unit] {
    exportToFile(user, request.dfSpec, HadoopFile(request.path), "parquet")
  }

  def exportSQLQueryToORC(
    user: serving.User, request: SQLExportToORCRequest) = async[Unit] {
    exportToFile(user, request.dfSpec, HadoopFile(request.path), "orc")
  }

  def exportSQLQueryToJdbc(
    user: serving.User, request: SQLExportToJdbcRequest) = async[Unit] {
    val df = request.dfSpec.createDataFrame(user, SQLController.defaultContext(user))
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
    val df = dfSpec.createDataFrame(user, SQLController.defaultContext(user))
    // TODO: #2889 (special characters in S3 passwords).
    file.assertWriteAllowedFrom(user)
    df.write.format(format).options(options).save(file.resolvedName)
  }

}
object SQLController {
  def stringOnlySchema(columns: Seq[String]) = {
    import spark.sql.types._
    StructType(columns.map(StructField(_, StringType, true)))
  }

  private def assertAccessAndGetTableEntry(
    user: serving.User,
    tableName: String,
    privacy: String)(implicit metaManager: MetaGraphManager): DirectoryEntry = {

    assert(!tableName.isEmpty, "Table name must be specified.")
    val entry = DirectoryEntry.fromName(tableName)
    entry.assertParentWriteAllowedFrom(user)
    entry
  }

  def saveTable(
    df: spark.sql.DataFrame,
    notes: String,
    user: serving.User,
    tableName: String,
    privacy: String,
    importConfig: Option[json.JsObject] = None)(
      implicit metaManager: MetaGraphManager,
      dataManager: DataManager): FEOption = metaManager.synchronized {
    assertAccessAndGetTableEntry(user, tableName, privacy)
    val table = TableImport.importDataFrameAsync(df)
    val entry = assertAccessAndGetTableEntry(user, tableName, privacy)
    val checkpoint = table.saveAsCheckpoint(notes)
    val frame = entry.asNewTableFrame(checkpoint)
    importConfig.foreach(frame.setImportConfig)
    frame.setupACL(privacy, user)
    FEOption.titledCheckpoint(checkpoint, frame.name, s"|${Table.VertexTableName}")
  }

  def saveView[T <: GenericImportRequest: json.Writes](
    notes: String, user: serving.User, tableName: String, privacy: String, recipe: T)(
      implicit metaManager: MetaGraphManager,
      dataManager: DataManager) = {
    val entry = assertAccessAndGetTableEntry(user, tableName, privacy)
    val view = entry.asNewViewFrame(recipe, notes)
    FEOption.titledCheckpoint(view.checkpoint, tableName, s"|${tableName}")
  }

  def defaultContext(user: User)(implicit dataManager: DataManager): SQLContext = {
    if (user.isAdmin) {
      dataManager.newHiveContext()
    } else {
      dataManager.newSQLContext()
    }
  }
}
