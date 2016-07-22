// An API that allows controlling a running LynxKite instance via JSON commands.
package com.lynxanalytics.biggraph.serving

import org.apache.spark.sql.{ DataFrame, SQLContext, types }
import play.api.libs.json
import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.frontend_operations
import com.lynxanalytics.biggraph.graph_api.TypedEntity
import com.lynxanalytics.biggraph.graph_api.SymbolPath
import com.lynxanalytics.biggraph.graph_api.TypedJson
import com.lynxanalytics.biggraph.graph_api.{ DataManager, MetaGraphManager, SymbolPath, TypedEntity }
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.serving.FrontendJson._
//import com.lynxanalytics.biggraph.controllers.ViewRecipe

object RemoteAPIProtocol {
  case class CheckpointResponse(checkpoint: String)
  case class OperationRequest(
    checkpoint: String,
    operation: String,
    parameters: Map[String, String])
  case class ProjectRequest(project: String)
  case class SaveProjectRequest(checkpoint: String, project: String)
  case class ScalarRequest(checkpoint: String, scalar: String)
  case class ProjectSQLRequest(checkpoint: String, query: String, limit: Int)
  case class GlobalSQLRequest(query: String, checkpoints: Map[String, String]) extends ViewRecipe {

    val controller = RemoteAPIServer.c

    override def createDataFrame(user: User, context: SQLContext)(implicit dataManager: DataManager, metaManager: MetaGraphManager): DataFrame = {

      for ((name, cp) <- checkpoints) controller.registerTablesOfRootProject(user, context, name + "|", cp)
      context.sql(query)
    }

    override def notes = ""
    override val name = ""
    override val privacy = ""
  }
  case class CheckpointRequest(checkpoint: String)
  case class CheckpointRequestWithLimit(checkpoint: String, limit: Int)
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
  case class ExportViewToJsonRequest(checkpoint: String, path: String)

  implicit val wCheckpointResponse = json.Json.writes[CheckpointResponse]
  implicit val rOperationRequest = json.Json.reads[OperationRequest]
  implicit val rProjectRequest = json.Json.reads[ProjectRequest]
  implicit val rSaveProjectRequest = json.Json.reads[SaveProjectRequest]
  implicit val rScalarRequest = json.Json.reads[ScalarRequest]
  implicit val rProjectSQLRequest = json.Json.reads[ProjectSQLRequest]
  implicit val rGlobalSQLRequest = json.Json.reads[GlobalSQLRequest]
  implicit val wDynamicValue = json.Json.writes[DynamicValue]
  implicit val wTableResult = json.Json.writes[TableResult]
  implicit val rCheckpointRequest = json.Json.reads[CheckpointRequest]
  implicit val rCheckpointRequestWithLimit = json.Json.reads[CheckpointRequestWithLimit]
  implicit val rDirectoryEntryRequest = json.Json.reads[DirectoryEntryRequest]
  implicit val wDirectoryEntryResult = json.Json.writes[DirectoryEntryResult]
  implicit val rExportViewToJsonRequest = json.Json.reads[ExportViewToJsonRequest]

}

object RemoteAPIServer extends JsonServer {
  import RemoteAPIProtocol._
  val userController = ProductionJsonServer.userController
  val c = new RemoteAPIController(BigGraphProductionEnvironment)
  def getScalar = jsonPost(c.getScalar)
  def getDirectoryEntry = jsonPost(c.getDirectoryEntry)
  def newProject = jsonPost(c.newProject)
  def loadProject = jsonPost(c.loadProject)
  def runOperation = jsonPost(c.runOperation)
  def saveProject = jsonPost(c.saveProject)
  def projectSQL = jsonPost(c.projectSQL)
  def globalSQL = jsonPost(c.globalSQL)
  private def importRequest[T <: GenericImportRequest: json.Writes: json.Reads] =
    jsonPost[T, CheckpointResponse](c.importRequest)
  private def createView[T <: GenericImportRequest: json.Writes: json.Reads] =
    jsonPost[T, CheckpointResponse](c.createView)
  def importJdbc = importRequest[JdbcImportRequest]
  def importHive = importRequest[HiveImportRequest]
  def importCSV = importRequest[CSVImportRequest]
  def importParquet = importRequest[ParquetImportRequest]
  def importORC = importRequest[ORCImportRequest]
  def importJson = importRequest[JsonImportRequest]
  def computeProject = jsonPost(c.computeProject)
  def createViewJdbc = createView[JdbcImportRequest]
  def createViewHive = createView[HiveImportRequest]
  def createViewCSV = createView[CSVImportRequest]
  def createViewParquet = createView[ParquetImportRequest]
  def createViewORC = createView[ORCImportRequest]
  def createViewJson = createView[JsonImportRequest]
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

  def loadProject(user: User, request: ProjectRequest): CheckpointResponse = {
    val project = controllers.ProjectFrame.fromName(request.project)
    project.assertReadAllowedFrom(user)
    val cp = project.viewer.rootCheckpoint
    CheckpointResponse(cp)
  }

  def saveProject(user: User, request: SaveProjectRequest): CheckpointResponse = {
    val entry = controllers.DirectoryEntry.fromName(request.project)
    val project = if (!entry.exists) {
      val p = entry.asNewProjectFrame()
      p.writeACL = user.email
      p.readACL = user.email
      p
    } else {
      entry.asProjectFrame
    }
    project.setCheckpoint(request.checkpoint)
    CheckpointResponse(request.checkpoint)
  }

  def getViewer(cp: String) =
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

  def projectSQL(user: User, request: ProjectSQLRequest): TableResult = {
    val sqlContext = dataManager.newHiveContext()
    registerTablesOfRootProject(user, sqlContext, "", request.checkpoint)
    val df = sqlContext.sql(request.query)
    dfToTableResult(df, request.limit)
  }

  def globalSQL(user: User, request: GlobalSQLRequest): CheckpointResponse = {
    createView(user, GlobalSQLRequest)
  }

  // Takes all the tables in the rootproject given by the checkpoint and registers all of them with prefixed name
  def registerTablesOfRootProject(user: User, sqlContext: SQLContext,
                                  prefix: String, checkpoint: String) = {
    val viewer = getViewer(checkpoint)
    for (path <- viewer.allRelativeTablePaths) {
      val df = controllers.Table(path, viewer).toDF(sqlContext)
      df.registerTempTable(prefix + "|" + path.toString)
      if (path.toString == "vertices") df.registerTempTable(prefix)
    }
    for (r <- viewer.editor.viewRecipe) {
      r.createDataFrame(user, sqlContext).registerTempTable(prefix)
    }
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

  def importRequest[T <: GenericImportRequest: json.Writes](
    user: User, request: T): CheckpointResponse = {
    val res = sqlController.doImport(user, request)
    val (cp, _, _) = FEOption.unpackTitledCheckpoint(res.id)
    CheckpointResponse(cp)
  }

  def createView[T <: ViewRecipe: json.Writes](user: User, recipe: T): CheckpointResponse = {
    val editor = new RootProjectEditor(RootProjectState.emptyState)
    editor.viewRecipe = recipe
    val cps = metaManager.checkpointRepo.checkpointState(editor.rootState, prevCheckpoint = "")
    CheckpointResponse(cps.checkpoint.get)
  }

  private def viewToDf(user: User, checkpoint: String) = {
    val viewer = getViewer(checkpoint)
    val sqlContext = dataManager.newHiveContext()
    viewer.editor.viewRecipe.get.createDataFrame(user, sqlContext)
  }

  def exportViewToTableResult(user: User, request: CheckpointRequestWithLimit): TableResult = {
    val df = viewToDf(user, request.checkpoint)
    dfToTableResult(df, request.limit)
  }

  def exportViewToJson(user: User, request: ExportViewToJsonRequest): Unit = {
    val file = HadoopFile(request.path)
    exportToFile(user, request.checkpoint, file, "json")
  }

  private def exportToFile(user: serving.User, checkpoint: String, file: HadoopFile,
                           format: String, options: Map[String, String] = Map()): Unit = {
    val df = viewToDf(user, checkpoint)
    df.write.format(format).options(options).save(file.resolvedName)
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
