// SQLController includes the request handlers for SQL in Kite.
package com.lynxanalytics.biggraph.controllers

import org.apache.spark

import scala.concurrent.Future
import scala.reflect.runtime.universe.TypeTag
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_operations.ExecuteSQL
import com.lynxanalytics.biggraph.graph_operations.ImportDataFrame
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.JDBCUtil
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.spark_util.SQLHelper
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.serving.User
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import play.api.libs.json

// FrameSettings holds details for creating an ObjectFrame.
trait FrameSettings {
  def name: String
  def notes: String
  def privacy: String
  def overwrite: Boolean
}

object DataFrameSpec extends FromJson[DataFrameSpec] {
  // Utilities for testing.
  def local(project: String, sql: String) =
    new DataFrameSpec(directory = None, project = Some(project), sql = sql)
  def global(directory: String, sql: String) =
    new DataFrameSpec(directory = Some(directory), project = None, sql = sql)

  import com.lynxanalytics.biggraph.serving.FrontendJson.fDataFrameSpec
  override def fromJson(j: JsValue): DataFrameSpec = json.Json.fromJson(j).get
}
case class DataFrameSpec(directory: Option[String], project: Option[String], sql: String) {
  assert(directory.isDefined ^ project.isDefined,
    "Exactly one of directory and project should be defined")
  def createDataFrame(user: User, context: SQLContext)(
    implicit dataManager: DataManager, metaManager: MetaGraphManager): DataFrame = {
    if (project.isDefined) ??? // TODO: Delete this method.
    else globalSQL(user, context)
  }

  // Finds the names of tables from string
  private def findTablesFromQuery(query: String): List[String] = {
    val split = query.split("`", -1)
    Iterator.range(start = 1, end = split.length, step = 2).map(split(_)).toList
  }

  // Creates a DataFrame from a global level SQL query.
  private def globalSQL(user: serving.User, context: SQLContext)(
    implicit dataManager: DataManager, metaManager: MetaGraphManager): spark.sql.DataFrame =
    metaManager.synchronized {
      assert(project.isEmpty,
        "The project field in the DataFrameSpec must be empty for global SQL queries.")

      val directoryName = directory.get
      val directoryPrefix = if (directoryName == "") "" else directoryName + "/"
      val givenTableNames = findTablesFromQuery(sql)
      // Maps the relative table names used in the sql query with the global name
      val tableNames = givenTableNames.map(name => (name, directoryPrefix + name)).toMap
      val pathAndTableName = tableNames.mapValues(wholePath => {
        val split = wholePath.split('|')
        (split.head, split.tail)
      })
      val snapshotsAndInternalTables = pathAndTableName.mapValues {
        case (snapshotPath, tablePath) => (DirectoryEntry.fromName(snapshotPath), tablePath)
      }
      val goodSnapshotStates = snapshotsAndInternalTables.collect {
        case (name, (snapshot, tablePath)) if snapshot.isSnapshot && snapshot.readAllowedFrom(user) =>
          (name, (snapshot.asSnapshotFrame.getState(), tablePath))
      }
      val protoTables = goodSnapshotStates.collect {
        case (name, (state, tablePath)) if state.isTable => (name, ProtoTable(state.table))
        case (name, (state, tablePath)) if state.isProject =>
          val rootViewer = state.project.viewer
          val protoTable = rootViewer.getSingleProtoTable(tablePath.mkString("|"))
          (name, protoTable)
      }
      val minimizedProtoTables = ProtoTable.minimize(sql, protoTables)
      val tables = minimizedProtoTables.mapValues(protoTable => protoTable.toTable)
      val result = ExecuteSQL.run(sql, tables)
      import Scripting._
      result.df
    }

  private def queryTables(
    sql: String,
    tables: Iterable[(String, Table)])(
      implicit dataManager: DataManager, metaManager: MetaGraphManager): spark.sql.DataFrame = {
    import Scripting._
    val dfs = tables.map { case (name, table) => name -> table.df }
    DataManager.sql(dataManager.newSQLContext, sql, dfs.toList)
  }
}
case class SQLQueryRequest(dfSpec: DataFrameSpec, maxRows: Int)
case class SQLColumn(name: String, dataType: String)
case class SQLQueryResult(header: List[SQLColumn], data: List[List[DynamicValue]])

case class SQLExportToTableRequest(
  dfSpec: DataFrameSpec,
  table: String,
  privacy: String,
  overwrite: Boolean)
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
case class SQLCreateViewRequest(
    name: String, privacy: String, dfSpec: DataFrameSpec, overwrite: Boolean) extends ViewRecipe with FrameSettings {
  override def createDataFrame(
    user: User, context: SQLContext)(
      implicit dataManager: DataManager, metaManager: MetaGraphManager): DataFrame =
    dfSpec.createDataFrame(user, context)

  override def notes: String = dfSpec.sql
}

object SQLCreateViewRequest extends FromJson[SQLCreateViewRequest] {
  import com.lynxanalytics.biggraph.serving.FrontendJson.fSQLCreateView
  override def fromJson(j: JsValue): SQLCreateViewRequest = json.Json.fromJson(j).get
}

object FileImportValidator {
  def checkFileHasContents(hadoopFile: HadoopFile): Unit = {
    assert(hadoopFile.list.map(f => f.getContentSummary.getSpaceConsumed).sum > 0,
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

class SQLController(val env: BigGraphEnvironment, ops: OperationRepository) {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager: DataManager = env.dataManager
  // We don't want to block the HTTP threads -- we want to return Futures instead. But the DataFrame
  // API is not Future-based, so we need to block some threads. This also avoids #2906.
  implicit val executionContext = ThreadUtil.limitedExecutionContext("SQLController", 100)
  def async[T](func: => T): Future[T] = Future(func)

  def saveView[T <: ViewRecipe with FrameSettings: json.Writes](
    user: serving.User, recipe: T): FEOption = {
    SQLController.saveView(
      recipe.notes,
      user,
      recipe.name,
      recipe.privacy, recipe.overwrite, recipe)
  }

  import com.lynxanalytics.biggraph.serving.FrontendJson._
  def importBox(user: serving.User, box: Box) = async[String] {
    val op = ops.opForBox(
      user, box, inputs = null, workspaceParameters = null).asInstanceOf[ImportOperation]
    val df = op.getDataFrame(SQLController.defaultContext(user))
    val table = ImportDataFrame.run(df)
    dataManager.getFuture(table) // Start importing in the background.
    table.gUID.toString
  }

  def createViewDFSpec(user: serving.User, spec: SQLCreateViewRequest) = saveView(user, spec)

  private def getInputTablesForBox(
    user: serving.User, inputTables: Map[String, ProtoTable]): TableBrowserNodeResponse = {
    TableBrowserNodeResponse(
      list = inputTables.map {
        case (name, table) =>
          TableBrowserNode(
            absolutePath = name, // Same as name for top level nodes.
            name = name,
            objectType = "table")
      }.toList.sortWith(_.name < _.name)) // Map orders elements randomly so we need to sort for the UI.
  }

  def getTableBrowserNodesForBox(
    user: serving.User, inputTables: Map[String, ProtoTable], path: String): TableBrowserNodeResponse = {
    if (path.isEmpty) {  // Top level request, for boxes that means input tables.
      getInputTablesForBox(user, inputTables)
    } else {  // Lower level request, for boxes that means table columns.
      assert(inputTables.contains(path), s"$path is not a valid proto table")
      getColumnsFromSchema(inputTables(path).schema)
    }
  }

  // Return list of nodes for the table browser. The nodes can be:
  // - Snapshots and subdirs in directories
  // - segmentations and implicit tables of a snapshot
  // - columns of a table
  def getTableBrowserNodes(user: serving.User, request: TableBrowserNodeRequest) = async[TableBrowserNodeResponse] {
    val pathParts = SubProject.splitPipedPath(request.path)
    val entry = DirectoryEntry.fromName(pathParts.head)
    entry.assertReadAllowedFrom(user)
    if (entry.isDirectory) {
      getDirectory(user, entry.asDirectory, request.query)
    } else if (entry.isSnapshot) {
      getSnapshot(user, entry.asSnapshotFrame, pathParts.tail)
    } else {
      throw new AssertionError(
        s"Table browser nodes are only available for snapshots and directories (${entry.path}).")
    }
  }

  def getDirectory(
    user: serving.User,
    dir: Directory,
    query: Option[String]): TableBrowserNodeResponse = {
    val (visibleDirs, visibleObjectFrames) = if (!query.isEmpty && !query.get.isEmpty) {
      BigGraphController.projectSearch(user, dir, query.get, includeNotes = false)
    } else {
      BigGraphController.projectList(user, dir)
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
          objectType = frame.objectType
        )
      }
    ).toList)
  }

  def getSnapshot(user: serving.User, frame: SnapshotFrame, pathTail: Seq[String]): TableBrowserNodeResponse = {
    val state = frame.getState
    if (state.isTable) {
      getColumnsFromSchema(state.table.schema)
    } else if (state.isProject) {
      if (pathTail.isEmpty) {
        // The last path segment is the frame, therefore the state is really a project.
        getProjectTables(frame.path, state.project.viewer, pathTail)
      } else {
        // The path identifies a table within a snapshot of a project kind.
        val protoTable = state.project.viewer.getSingleProtoTable(pathTail.mkString("|"))
        getColumnsFromSchema(protoTable.schema)
      }
    } else {
      throw new AssertionError(
        s"Snaphot ${frame.path} has to be of table or project kind, not ${state.kind}.")
    }
  }

  def getProjectTables(path: SymbolPath, parentViewer: RootProjectViewer, subPath: Seq[String]): TableBrowserNodeResponse = {
    val viewer = parentViewer.offspringViewer(subPath)
    val implicitTables = viewer.getProtoTables.map(_._1).toSeq.map {
      name =>
        TableBrowserNode(
          absolutePath = (Seq(path.toString) ++ subPath ++ Seq(name)).mkString("|"),
          name = name,
          objectType = "table")
    }
    val subProjects = viewer.sortedSegmentations.map {
      segmentation =>
        TableBrowserNode(
          absolutePath =
            (Seq(path.toString) ++ subPath ++ Seq(segmentation.segmentationName)).mkString("|"),
          name = segmentation.segmentationName,
          objectType = "segmentation"
        )
    }

    TableBrowserNodeResponse(list = implicitTables ++ subProjects)
  }

  def getColumnsFromSchema(schema: spark.sql.types.StructType): TableBrowserNodeResponse = {
    TableBrowserNodeResponse(
      list = schema.fields.map { field =>
        TableBrowserNode(
          absolutePath = "",
          name = field.name,
          objectType = "column",
          columnType = ProjectViewer.feTypeName(
            SQLHelper.typeTagFromDataType(field.dataType))
        )
      })
  }

  def getTableColumns(frame: ObjectFrame, tablePath: Seq[String]): TableBrowserNodeResponse = {
    ??? // TODO: Do it for snapshots instead.
  }

  def runSQLQuery(user: serving.User, request: SQLQueryRequest) = async[SQLQueryResult] {
    val df = request.dfSpec.createDataFrame(user, SQLController.defaultContext(user))
    val columns = df.schema.toList.map { field =>
      field.name -> SQLHelper.typeTagFromDataType(field.dataType).asInstanceOf[TypeTag[Any]]
    }
    SQLQueryResult(
      header = columns.map { case (name, tt) => SQLColumn(name, ProjectViewer.feTypeName(tt)) },
      data = SQLHelper.toSeqRDD(df).take(request.maxRows).map {
        row =>
          row.toSeq.toList.zip(columns).map {
            case (null, field) => DynamicValue("null", defined = false)
            case (item, (name, tt)) => DynamicValue.convert(item)(tt)
          }
      }.toList
    )
  }

  // TODO: Remove code duplication
  def getTableSample(table: Table, sampleRows: Int = 10): GetTableOutputResponse = {
    val columns = table.schema.toList.map { field =>
      field.name -> SQLHelper.typeTagFromDataType(field.dataType).asInstanceOf[TypeTag[Any]]
    }
    import Scripting._
    val df = table.df
    GetTableOutputResponse(
      header = columns.map { case (name, tt) => TableColumn(name, ProjectViewer.feTypeName(tt)) },
      data = SQLHelper.toSeqRDD(df).take(sampleRows).map {
        row =>
          row.toSeq.toList.zip(columns).map {
            case (null, field) => DynamicValue("null", defined = false)
            case (item, (name, tt)) => DynamicValue.convert(item)(tt)
          }
      }.toList
    )
  }

  def exportSQLQueryToTable(
    user: serving.User, request: SQLExportToTableRequest) = async[Unit] {
    val df = request.dfSpec.createDataFrame(user, SQLController.defaultContext(user))
    SQLController.saveTable(
      df, s"From ${request.dfSpec.project} by running ${request.dfSpec.sql}",
      user, request.table, request.privacy, request.overwrite,
      importConfig = Some(TypedJson.createFromWriter(request).as[json.JsObject]))
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
    overwrite: Boolean = false,
    importConfig: Option[json.JsObject] = None)(
      implicit metaManager: MetaGraphManager,
      dataManager: DataManager): FEOption = metaManager.synchronized {
    ???
  }

  def saveView[T <: ViewRecipe: json.Writes](
    notes: String, user: serving.User, name: String, privacy: String, overwrite: Boolean, recipe: T)(
      implicit metaManager: MetaGraphManager,
      dataManager: DataManager) = {
    ???
  }

  // Every query runs in its own SQLContext for isolation.
  def defaultContext(user: User)(implicit dataManager: DataManager): SQLContext = {
    dataManager.newSQLContext()
  }
}
