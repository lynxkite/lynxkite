// SQLController includes the request handlers for SQL in Kite.
package com.lynxanalytics.biggraph.controllers

import org.apache.spark

import scala.concurrent.Future
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations._
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.spark_util.SQLHelper
import com.lynxanalytics.biggraph.serving
import org.apache.spark.sql.SQLContext
import play.api.libs.json

// FrameSettings holds details for creating an ObjectFrame.
trait FrameSettings {
  def name: String
  def notes: String
  def privacy: String
  def overwrite: Boolean
}

object TableSpec extends FromJson[TableSpec] {
  // Utilities for testing.
  def global(directory: String, sql: String) =
    new TableSpec(directory = Some(directory), sql = sql)

  import com.lynxanalytics.biggraph.serving.FrontendJson.fDataFrameSpec
  override def fromJson(j: JsValue): TableSpec = json.Json.fromJson(j).get
}

case class TableSpec(directory: Option[String], sql: String) {
  // Finds the names of tables from string
  private def findTablesFromQuery(query: String): List[String] = {
    val split = query.split("`", -1)
    Iterator.range(start = 1, end = split.length, step = 2).map(split(_)).toList
  }

  def globalSQL(user: serving.User)(
    implicit
    dm: DataManager, mm: MetaGraphManager): Table = {
    mm.synchronized {
      val directoryName = directory.get
      val directoryPrefix = if (directoryName == "") "" else directoryName + "/"
      val givenTableNames = findTablesFromQuery(sql)
      // Maps the relative table names used in the sql query with the global name
      val tableNames = givenTableNames.map(name => (name, directoryPrefix + name)).toMap
      val snapshotsAndInternalTables =
        tableNames.mapValues(wholePath => SQLController.parseTablePath(wholePath))

      val goodSnapshotStates = snapshotsAndInternalTables.collect {
        case (name, (snapshot, tablePath)) if snapshot.isSnapshot && snapshot.readAllowedFrom(user) =>
          (name, (snapshot.asSnapshotFrame.getState(), tablePath))
      }
      val protoTables = goodSnapshotStates.collect {
        case (name, (state, _)) if state.isTable => (name, ProtoTable(state.table))
        case (name, (state, tablePath)) if state.isProject =>
          val rootViewer = state.project.viewer
          val protoTable = rootViewer.getSingleProtoTable(tablePath.mkString("."))
          (name, protoTable)
      }
      ExecuteSQL.run(sql, protoTables)
    }
  }
}
case class SQLQueryRequest(dfSpec: TableSpec, maxRows: Int)
case class SQLColumn(name: String, dataType: String)
case class SQLQueryResult(header: List[SQLColumn], data: List[List[DynamicValue]])

case class SQLExportToTableRequest(
    dfSpec: TableSpec,
    table: String,
    privacy: String,
    overwrite: Boolean)
case class SQLExportToCSVRequest(
    dfSpec: TableSpec,
    path: String,
    header: Boolean,
    delimiter: String,
    quote: String)
case class SQLExportToJsonRequest(
    dfSpec: TableSpec,
    path: String)
case class SQLExportToParquetRequest(
    dfSpec: TableSpec,
    path: String)
case class SQLExportToORCRequest(
    dfSpec: TableSpec,
    path: String)
case class SQLExportToJdbcRequest(
    dfSpec: TableSpec,
    jdbcUrl: String,
    tableName: String,
    mode: String) {
  val validModes = Seq( // Save as the save modes accepted by DataFrameWriter.
    "error", // The table will be created and must not already exist.
    "overwrite", // The table will be dropped (if it exists) and created.
    "append") // The table must already exist.
  assert(validModes.contains(mode), s"Mode ($mode) must be one of $validModes.")
}
case class SQLExportToFileResult(download: Option[serving.DownloadFileRequest])

object FileImportValidator {
  def checkFileHasContents(hadoopFile: HadoopFile): Unit = {
    assert(
      hadoopFile.list.map(f => f.getContentSummary.getSpaceConsumed).sum > 0,
      s"No data was found at '${hadoopFile.symbolicName}' (no or empty files).")
  }
}

// path denotes a directory entry (table/view/project/directory), or a
// a segmentation or an implicit project table. Segmentations and implicit
// project tables have the same form of path but occupy separate namespaces.
// Therefore implict tables can only be accessed by specifying
// isImplictTable = true. (Implicit tables are the vertices, edge_attributes,
// and etc. tables that are automatically parts of projects.)
case class TableBrowserNodeRequest(
    path: String,
    query: Option[String],
    isImplicitTable: Boolean = false)

case class TableBrowserNode(
    absolutePath: String,
    name: String,
    objectType: String,
    columnType: String = "")
case class TableBrowserNodeResponse(list: Seq[TableBrowserNode])

case class TableBrowserNodeForBoxRequest(
    operationRequest: GetOperationMetaRequest,
    path: String)

class SQLController(val env: BigGraphEnvironment, ops: OperationRepository) extends play.api.http.HeaderNames {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager: DataManager = env.dataManager
  implicit val sparkDomain = env.sparkDomain
  // We don't want to block the HTTP threads -- we want to return Futures instead. But the DataFrame
  // API is not Future-based, so we need to block some threads. This also avoids #2906.
  implicit val executionContext = ThreadUtil.limitedExecutionContext("SQLController", 100)
  def async[T](func: => T): Future[T] = Future(func)

  def importBox(user: serving.User, box: Box, workspaceParameters: Map[String, String]) = async[ImportResult] {
    val op = ops.opForBox(
      user, box, inputs = Map[String, BoxOutputState](),
      workspaceParameters = workspaceParameters).asInstanceOf[Importer]
    op.runImport(env)
  }

  def getTableBrowserNodesForBox(workspaceController: WorkspaceController)(
    user: serving.User, request: TableBrowserNodeForBoxRequest): TableBrowserNodeResponse = {
    val inputTables = workspaceController.getOperationInputTables(user, request.operationRequest)
    if (request.path.isEmpty) { // Top level request, for boxes that means input tables.
      getInputTablesForBox(user, inputTables)
    } else { // Lower level request, for boxes that means table columns.
      assert(inputTables.contains(request.path), s"${request.path} is not a valid proto table")
      getColumnsFromSchema(inputTables(request.path).schema)
    }
  }

  private def getInputTablesForBox(
    user: serving.User, inputTables: Map[String, ProtoTable]): TableBrowserNodeResponse = {
    TableBrowserNodeResponse(
      list = inputTables.map {
        case (name, table) =>
          TableBrowserNode(
            absolutePath = name, // Same as name for top level nodes.
            name = name,
            objectType = "table")
      }.toList.sortBy(_.name)) // Map orders elements randomly so we need to sort for the UI.
  }

  // Returns the list of nodes for the table browser. The nodes can be:
  // - snapshots and subdirs in directories
  // - segmentations and implicit tables of a project kind snapshot
  // - columns of a table kind snapshot
  def getTableBrowserNodes(user: serving.User, request: TableBrowserNodeRequest): TableBrowserNodeResponse =
    metaManager.synchronized {
      val entryFull = DirectoryEntry.fromName(request.path)
      if (entryFull.isDirectory) {
        // If it is a directory, we don't want to split directory
        // names which contain dots.
        entryFull.assertReadAllowedFrom(user)
        getDirectory(user, entryFull.asDirectory, request.query)
      } else {
        val (entry, path) = SQLController.parseTablePath(request.path)
        entry.assertReadAllowedFrom(user)
        if (entry.isSnapshot) {
          getSnapshot(user, entry.asSnapshotFrame, path)
        } else {
          throw new AssertionError(
            s"Table browser nodes are only available for snapshots and directories (${entry.path}).")
        }
      }
    }

  private def getDirectory(
    user: serving.User,
    dir: Directory,
    query: Option[String]): TableBrowserNodeResponse = {
    val (visibleDirs, visibleObjectFrames) = if (!query.isEmpty && !query.get.isEmpty) {
      BigGraphController.entrySearch(user, dir, query.get, includeNotes = false)
    } else {
      BigGraphController.entryList(user, dir)
    }
    TableBrowserNodeResponse(list = (
      visibleDirs.map { dir =>
        TableBrowserNode(
          absolutePath = dir.path.toString,
          name = dir.path.name.name,
          objectType = "directory")
      } ++ visibleObjectFrames.filter(_.objectType == "snapshot").map { frame =>
        TableBrowserNode(
          absolutePath = frame.path.toString,
          name = frame.path.name.name,
          objectType = getObjectType(frame))
      }).toList)
  }

  private def getSnapshot(user: serving.User, frame: SnapshotFrame, pathTail: Seq[String]): TableBrowserNodeResponse = {
    val state = frame.getState
    if (state.isTable) {
      getColumnsFromSchema(state.table.schema)
    } else if (state.isProject) {
      val viewer = state.project.viewer
      if (viewer.hasOffspring(pathTail)) {
        // The path identifies a project or a segment.
        getProjectTables(frame.path, viewer, pathTail)
      } else {
        // The path identifies a table within a snapshot of a project kind.
        val protoTable = viewer.getSingleProtoTable(pathTail.mkString("."))
        getColumnsFromSchema(protoTable.schema)
      }
    } else {
      throw new AssertionError(
        s"Snaphot ${frame.path} has to be of table or project kind, not ${state.kind}.")
    }
  }

  private def getProjectTables(
    path: SymbolPath,
    parentViewer: RootProjectViewer,
    subPath: Seq[String]): TableBrowserNodeResponse = {
    val viewer = parentViewer.offspringViewer(subPath)
    val implicitTables = viewer.getProtoTables.map(_._1).toSeq.map {
      name =>
        TableBrowserNode(
          absolutePath = (Seq(path.toString) ++ subPath ++ Seq(name)).mkString("."),
          name = name,
          objectType = "table")
    }
    val subProjects = viewer.sortedSegmentations.map {
      segmentation =>
        TableBrowserNode(
          absolutePath =
            (Seq(path.toString) ++ subPath ++ Seq(segmentation.segmentationName)).mkString("."),
          name = segmentation.segmentationName,
          objectType = "segmentation")
    }

    TableBrowserNodeResponse(list = implicitTables ++ subProjects)
  }

  private def getColumnsFromSchema(schema: spark.sql.types.StructType): TableBrowserNodeResponse = {
    TableBrowserNodeResponse(
      list = schema.fields.map { field =>
        TableBrowserNode(
          absolutePath = "",
          name = field.name,
          objectType = "column",
          columnType = SQLHelper.typeTagFromDataType(field.dataType).tpe.toString)
      })
  }

  private def getObjectType(frame: ObjectFrame): String = {
    if (frame.isSnapshot) {
      frame.asSnapshotFrame.getState().kind
    } else {
      frame.objectType
    }
  }

  def getTableColumns(frame: ObjectFrame, tablePath: Seq[String]): TableBrowserNodeResponse = {
    ??? // TODO: Do it for snapshots instead.
  }

  def runSQLQuery(user: serving.User, request: SQLQueryRequest) = async[SQLQueryResult] {
    val table = request.dfSpec.globalSQL(user)
    val tableContents = dataManager.get(TableToScalar.run(table, request.maxRows))
    SQLQueryResult(
      header = tableContents.header.map { case (name, tt) => SQLColumn(name, tt.tpe.toString) },
      data = tableContents.rows)
  }

  def getTableSample(table: Table, sampleRows: Int) = async[GetTableOutputResponse] {
    val e = TableToScalar.run(table, sampleRows)
    val tableContents = dataManager.get(e)
    GetTableOutputResponse(
      header = tableContents.header.map { case (name, tt) => TableColumn(name, tt.tpe.toString) },
      data = tableContents.rows)
  }

  def exportSQLQueryToCSV(
    user: serving.User, request: SQLExportToCSVRequest) = async[SQLExportToFileResult] {
    assert(!user.wizardOnly, s"User ${user.email} is restricted to using wizards.")
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
    exportToFile(user, request.dfSpec, request.path, "parquet")
  }

  def exportSQLQueryToORC(
    user: serving.User, request: SQLExportToORCRequest) = async[Unit] {
    exportToFile(user, request.dfSpec, request.path, "orc")
  }

  def exportSQLQueryToJdbc(
    user: serving.User, request: SQLExportToJdbcRequest) = async[Unit] {
    val table = request.dfSpec.globalSQL(user)
    val op = ExportTableToJdbc(
      request.jdbcUrl,
      request.tableName,
      request.mode)
    val exported = op(op.t, table).result.exportResult
    dataManager.get(exported)
  }

  private def downloadableExportToFile(
    user: serving.User,
    dfSpec: TableSpec,
    path: String,
    format: String,
    options: Map[String, String] = Map(),
    stripHeaders: Boolean = false): SQLExportToFileResult = {
    val file = if (path == "<download>") {
      s"DATA$$/exports/$Timestamp.$format"
    } else {
      path
    }
    exportToFile(user, dfSpec, file, format, options)
    val download =
      if (path == "<download>") Some(serving.DownloadFileRequest(HadoopFile(file).symbolicName, stripHeaders))
      else None
    SQLExportToFileResult(download)
  }

  private def exportToFile(
    user: serving.User,
    dfSpec: TableSpec,
    file: String,
    format: String,
    options: Map[String, String] = Map()): Unit = {
    assert(!user.wizardOnly, s"User ${user.email} is restricted to using wizards.")
    // TODO: #2889 (special characters in S3 passwords).
    HadoopFile(file).assertWriteAllowedFrom(user) // TODO: Do we need this?
    val table = dfSpec.globalSQL(user)
    val op = ExportTableToStructuredFile(
      path = file,
      format = format,
      version = Timestamp.toLong,
      saveMode = "error if exists",
      forDownload = false)
    val exported = op(op.t, table).result.exportResult
    dataManager.get(exported)
  }

  def downloadCSV(table: Table, sampleRows: Int) = {
    log.info(s"downloadCSV: ${table} ${sampleRows}")
    val path = s"DATA$$/exports/${table.gUID}"
    val op = com.lynxanalytics.biggraph.graph_operations.ExportTableToCSV(
      path = path,
      header = true,
      delimiter = ",",
      quote = "\"",
      quoteAll = false,
      escape = "\\",
      nullValue = "",
      dateFormat = "yyyy-MM-dd",
      timestampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
      dropLeadingWhiteSpace = false,
      dropTrailingWhiteSpace = false,
      version = 0,
      saveMode = "overwrite",
      forDownload = false)
    val export: Scalar[String] = op(op.t, table).result.exportResult
    dataManager.get(export)
    val f = HadoopFile(path)
    assert((f / "_SUCCESS").exists, s"$path/_SUCCESS does not exist.")
    val files = (f / "part-*").list.sortBy(_.symbolicName)
    val length = files.map(_.length).sum
    log.info(s"downloading $length bytes: $files")
    val streams = files.iterator.map(_.open)
    val iter = new com.lynxanalytics.biggraph.serving.HeaderSkippingStreamIterator(path, streams)
    import scala.collection.JavaConverters._
    val stream = new java.io.SequenceInputStream(iter.asJavaEnumeration)
    play.api.mvc.Result(
      header = play.api.mvc.ResponseHeader(200, Map(
        CONTENT_LENGTH -> length.toString,
        CONTENT_DISPOSITION -> s"attachment; filename=table-${Timestamp.human}.csv")),
      body = play.api.http.HttpEntity.Streamed(
        akka.stream.scaladsl.StreamConverters.fromInputStream(() => stream),
        Some(length), None))
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

  // Every query runs in its own SQLContext for isolation.
  def defaultContext()(implicit sparkDomain: SparkDomain): SQLContext = {
    sparkDomain.newSQLContext()
  }

  // Splits a table path into a snapshot entry and an internal table path.
  def parseTablePath(path: String)(implicit metaManager: MetaGraphManager): (DirectoryEntry, Seq[String]) = {
    // The path 'd1/d2/d3/sn.s1.s2.vertices' is converted into
    // (DirectoryEntry for 'd1/d2/d3/sn', Array('s1', 's2', 'vertices'))
    // Parts d1, d2, d3, .. can contain dots, but sn doesn't.
    val parts = DirectoryEntry.fromName(path).path.map(x => x.name).toList
    val split = SubProject.splitPipedPath(parts.last)
    val entryPathList = parts.init :+ split.head
    val entryPath = entryPathList.mkString("/")
    val entry = DirectoryEntry.fromName(entryPath)
    (entry, split.tail)
  }
}
