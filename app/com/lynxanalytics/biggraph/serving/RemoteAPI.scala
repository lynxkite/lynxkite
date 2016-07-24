// An API that allows controlling a running LynxKite instance via JSON commands.
package com.lynxanalytics.biggraph.serving

import org.apache.spark.sql.{ DataFrame, SQLContext, types }
import play.api.libs.json
import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.frontend_operations
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.serving.FrontendJson._
import com.lynxanalytics.biggraph.table.TableImport

object RemoteAPIProtocol {
  case class CheckpointResponse(checkpoint: String)
  case class OperationRequest(
    checkpoint: String,
    operation: String,
    parameters: Map[String, String])
  case class LoadNameRequest(name: String)
  case class SaveCheckpointRequest(checkpoint: String, name: String)
  case class ScalarRequest(checkpoint: String, scalar: String)

  object GlobalSQLRequest extends FromJson[GlobalSQLRequest] {
    override def fromJson(j: json.JsValue) = j.as[GlobalSQLRequest]
  }
  case class GlobalSQLRequest(query: String, checkpoints: Map[String, String]) extends ViewRecipe {
    override def createDataFrame(
      user: User, context: SQLContext)(
        implicit dataManager: DataManager, metaManager: MetaGraphManager): DataFrame = {
      for ((name, cp) <- checkpoints) {
        val viewer =
          new controllers.RootProjectViewer(metaManager.checkpointRepo.readCheckpoint(cp))
        registerTablesOfViewer(user, context, viewer, name)
      }
      context.sql(query)
    }

    // Takes all the tables in the project/table/view given by the viewer and registers all of
    // them with an optionally prefixed name.
    private def registerTablesOfViewer(
      user: User, sqlContext: SQLContext, viewer: controllers.RootProjectViewer,
      prefix: String = "")(implicit mm: MetaGraphManager, dm: DataManager) = {
      val fullPrefix = if (prefix.nonEmpty) prefix + "|" else ""
      for (path <- viewer.allRelativeTablePaths) {
        val df = controllers.Table(path, viewer).toDF(sqlContext)
        df.registerTempTable(fullPrefix + path.toString)
        if (path.toString == "vertices") df.registerTempTable(prefix)
      }
      for (r <- viewer.viewRecipe) {
        r.createDataFrame(user, sqlContext).registerTempTable(prefix)
      }
    }
  }

  case class CheckpointRequest(checkpoint: String)
  case class TakeFromViewRequest(checkpoint: String, limit: Int)
  // Each row is a map, repeating the schema. Values may be missing for some rows.
  case class TableResult(rows: List[Map[String, json.JsValue]])
  case class DirectoryEntryRequest(
    path: String)
  case class DirectoryEntryResult(
    exists: Boolean,
    isTable: Boolean,
    isProject: Boolean,
    isDirectory: Boolean,
    isReadAllowed: Boolean,
    isWriteAllowed: Boolean)
  case class ExportViewToCSVRequest(
    checkpoint: String, path: String, header: Boolean, delimiter: String, quote: String)
  case class ExportViewToJsonRequest(checkpoint: String, path: String)
  case class ExportViewToORCRequest(checkpoint: String, path: String)
  case class ExportViewToParquetRequest(checkpoint: String, path: String)
  case class ExportViewToJdbcRequest(
    checkpoint: String, jdbcUrl: String, table: String, mode: String)
  case class ExportViewToTableRequest(checkpoint: String)

  implicit val wCheckpointResponse = json.Json.writes[CheckpointResponse]
  implicit val rOperationRequest = json.Json.reads[OperationRequest]
  implicit val rLoadNameRequest = json.Json.reads[LoadNameRequest]
  implicit val rSaveCheckpointRequest = json.Json.reads[SaveCheckpointRequest]
  implicit val rScalarRequest = json.Json.reads[ScalarRequest]
  implicit val fGlobalSQLRequest = json.Json.format[GlobalSQLRequest]
  implicit val wDynamicValue = json.Json.writes[DynamicValue]
  implicit val wTableResult = json.Json.writes[TableResult]
  implicit val rCheckpointRequest = json.Json.reads[CheckpointRequest]
  implicit val rTakeFromViewRequest = json.Json.reads[TakeFromViewRequest]
  implicit val rDirectoryEntryRequest = json.Json.reads[DirectoryEntryRequest]
  implicit val wDirectoryEntryResult = json.Json.writes[DirectoryEntryResult]
  implicit val rExportViewToCSVRequest = json.Json.reads[ExportViewToCSVRequest]
  implicit val rExportViewToJsonRequest = json.Json.reads[ExportViewToJsonRequest]
  implicit val rExportViewToORCRequest = json.Json.reads[ExportViewToORCRequest]
  implicit val rExportViewToParquetRequest = json.Json.reads[ExportViewToParquetRequest]
  implicit val rExportViewToJdbcRequest = json.Json.reads[ExportViewToJdbcRequest]
  implicit val rExportViewToTableRequest = json.Json.reads[ExportViewToTableRequest]
}

object RemoteAPIServer extends JsonServer {
  import RemoteAPIProtocol._
  val userController = ProductionJsonServer.userController
  val c = new RemoteAPIController(BigGraphProductionEnvironment)
  def getScalar = jsonPost(c.getScalar)
  def getDirectoryEntry = jsonPost(c.getDirectoryEntry)
  def newProject = jsonPost(c.newProject)
  def loadProject = jsonPost(c.loadProject)
  def loadTable = jsonPost(c.loadTable)
  def loadView = jsonPost(c.loadView)
  def runOperation = jsonPost(c.runOperation)
  def saveProject = jsonPost(c.saveProject)
  def saveTable = jsonPost(c.saveTable)
  def saveView = jsonPost(c.saveView)
  def takeFromView = jsonPost(c.takeFromView)
  def exportViewToCSV = jsonPost(c.exportViewToCSV)
  def exportViewToJson = jsonPost(c.exportViewToJson)
  def exportViewToORC = jsonPost(c.exportViewToORC)
  def exportViewToParquet = jsonPost(c.exportViewToParquet)
  def exportViewToJdbc = jsonPost(c.exportViewToJdbc)
  def exportViewToTable = jsonPost(c.exportViewToTable)
  private def createView[T <: ViewRecipe: json.Writes: json.Reads] =
    jsonPost[T, CheckpointResponse](c.createView)
  def createViewJdbc = createView[JdbcImportRequest]
  def createViewHive = createView[HiveImportRequest]
  def createViewCSV = createView[CSVImportRequest]
  def createViewParquet = createView[ParquetImportRequest]
  def createViewORC = createView[ORCImportRequest]
  def createViewJson = createView[JsonImportRequest]
  def globalSQL = createView[GlobalSQLRequest]
  def computeProject = jsonPost(c.computeProject)
}

class RemoteAPIController(env: BigGraphEnvironment) {
  import RemoteAPIProtocol._

  implicit val metaManager = env.metaGraphManager
  implicit val dataManager = env.dataManager
  val ops = new frontend_operations.Operations(env)
  val sqlController = new SQLController(env)

  def normalize(operation: String) = operation.replace("-", "").toLowerCase

  lazy val normalizedIds = ops.operationIds.map(id => normalize(id) -> id).toMap

  def getDirectoryEntry(user: User, request: DirectoryEntryRequest): DirectoryEntryResult = {
    val entry = new DirectoryEntry(
      SymbolPath.parse(request.path))
    DirectoryEntryResult(
      exists = entry.exists,
      isTable = entry.isTable,
      isProject = entry.isProject,
      isDirectory = entry.isDirectory,
      isReadAllowed = entry.readAllowedFrom(user),
      isWriteAllowed = entry.writeAllowedFrom(user)
    )
  }

  def newProject(user: User, request: Empty): CheckpointResponse = {
    CheckpointResponse("") // Blank checkpoint.
  }

  def loadObject(
    user: User,
    request: LoadNameRequest,
    validator: controllers.DirectoryEntry => Boolean): CheckpointResponse = {
    val entry = controllers.DirectoryEntry.fromName(request.name)
    assert(validator(entry), "Invalid frame type")
    val frame = entry.asObjectFrame
    frame.assertReadAllowedFrom(user)
    CheckpointResponse(frame.checkpoint)
  }

  def loadProject(user: User, request: LoadNameRequest): CheckpointResponse = {
    loadObject(user, request, _.isProject)
  }

  def loadTable(user: User, request: LoadNameRequest): CheckpointResponse = {
    loadObject(user, request, _.isTable)
  }

  def loadView(user: User, request: LoadNameRequest): CheckpointResponse = {
    loadObject(user, request, _.isView)
  }

  def saveFrame(
    user: User,
    request: SaveCheckpointRequest,
    saver: (controllers.DirectoryEntry, String) => controllers.ObjectFrame): CheckpointResponse = {

    val entry = controllers.DirectoryEntry.fromName(request.name)
    entry.assertWriteAllowedFrom(user)
    val p = saver(entry, request.checkpoint)
    p.writeACL = user.email
    p.readACL = user.email
    CheckpointResponse(request.checkpoint)
  }

  def saveProject(user: User, request: SaveCheckpointRequest): CheckpointResponse = {
    saveFrame(user, request, _.asNewProjectFrame(_))
  }

  def saveTable(user: User, request: SaveCheckpointRequest): CheckpointResponse = {
    saveFrame(user, request, _.asNewTableFrame(_))
  }

  def saveView(user: User, request: SaveCheckpointRequest): CheckpointResponse = {
    saveFrame(user, request, _.asNewViewFrame(_))
  }

  def getViewer(cp: String): controllers.RootProjectViewer =
    new controllers.RootProjectViewer(metaManager.checkpointRepo.readCheckpoint(cp))

  def runOperation(user: User, request: OperationRequest): CheckpointResponse = {
    val normalized = normalize(request.operation)
    assert(normalizedIds.contains(normalized), s"No such operation: ${request.operation}")
    val operation = normalizedIds(normalized)
    val viewer = getViewer(request.checkpoint)
    val context = controllers.Operation.Context(user, viewer)
    val spec = controllers.FEOperationSpec(operation, request.parameters)
    val newState = ops.applyAndCheckpoint(context, spec)
    CheckpointResponse(newState.checkpoint.get)
  }

  def getScalar(user: User, request: ScalarRequest): DynamicValue = {
    val viewer = getViewer(request.checkpoint)
    val scalar = viewer.scalars(request.scalar)
    implicit val tt = scalar.typeTag
    import com.lynxanalytics.biggraph.graph_api.Scripting._
    DynamicValue.convert(scalar.value)
  }

  private def dfToTableResult(df: org.apache.spark.sql.DataFrame, limit: Int) = {
    val schema = df.schema
    val data = df.take(limit)
    val rows = data.map { row =>
      schema.fields.zipWithIndex.flatMap {
        case (f, i) =>
          if (row.isNullAt(i)) None
          else {
            val jsValue = f.dataType match {
              case _: types.DoubleType => json.Json.toJson(row.getDouble(i))
              case _: types.StringType => json.Json.toJson(row.getString(i))
              case _: types.LongType => json.Json.toJson(row.getLong(i))
              case _ => json.Json.toJson(row.get(i).toString)
            }
            Some(f.name -> jsValue)
          }
      }.toMap
    }
    TableResult(rows = rows.toList)
  }

  def createView[T <: ViewRecipe: json.Writes](user: User, recipe: T): CheckpointResponse = {
    CheckpointResponse(ViewRecipe.saveAsCheckpoint(recipe))
  }

  private def viewToDF(user: User, checkpoint: String) = {
    val viewer = getViewer(checkpoint)
    val sqlContext = dataManager.newHiveContext()
    viewer.viewRecipe.get.createDataFrame(user, sqlContext)
  }

  def takeFromView(user: User, request: TakeFromViewRequest): TableResult = {
    val df = viewToDF(user, request.checkpoint)
    dfToTableResult(df, request.limit)
  }

  def exportViewToCSV(user: User, request: ExportViewToCSVRequest): Unit = {
    exportViewToFile(
      user, request.checkpoint, request.path, "csv", Map(
        "delimiter" -> request.delimiter,
        "quote" -> request.quote,
        "nullValue" -> "",
        "header" -> (if (request.header) "true" else "false")))
  }

  def exportViewToJson(user: User, request: ExportViewToJsonRequest): Unit = {
    exportViewToFile(user, request.checkpoint, request.path, "json")
  }

  def exportViewToORC(user: User, request: ExportViewToORCRequest): Unit = {
    exportViewToFile(user, request.checkpoint, request.path, "orc")
  }

  def exportViewToParquet(user: User, request: ExportViewToParquetRequest): Unit = {
    exportViewToFile(user, request.checkpoint, request.path, "parquet")
  }

  def exportViewToJdbc(user: User, request: ExportViewToJdbcRequest): Unit = {
    val df = viewToDF(user, request.checkpoint)
    df.write.mode(request.mode).jdbc(request.jdbcUrl, request.table, new java.util.Properties)
  }

  private def exportViewToFile(
    user: serving.User, checkpoint: String, path: String,
    format: String, options: Map[String, String] = Map()): Unit = {
    val file = HadoopFile(path)
    file.assertWriteAllowedFrom(user)
    val df = viewToDF(user, checkpoint)
    df.write.format(format).options(options).save(file.resolvedName)
  }

  def exportViewToTable(user: User, request: ExportViewToTableRequest): CheckpointResponse = {
    val df = viewToDF(user, request.checkpoint)
    val table = TableImport.importDataFrameAsync(df)
    val cp = table.saveAsCheckpoint("Created from a view via the Remote API.")
    CheckpointResponse(cp)
  }

  def computeProject(user: User, request: CheckpointRequest): Unit = {
    val viewer = getViewer(request.checkpoint)
    computeProject(viewer.editor)
  }

  private def computeProject(editor: ProjectEditor): Unit = {
    for ((name, scalar) <- editor.scalars) {
      dataManager.get(scalar)
    }

    val attributes = editor.vertexAttributes ++ editor.edgeAttributes
    for ((name, attr) <- attributes) {
      dataManager.get(attr)
    }

    val segmentations = editor.viewer.sortedSegmentations
    segmentations.foreach(s => computeProject(s.editor))
  }
}
