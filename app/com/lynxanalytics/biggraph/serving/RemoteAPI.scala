// An API that allows controlling a running LynxKite instance via JSON commands.
package com.lynxanalytics.biggraph.serving

import org.apache.spark.sql.types
import play.api.libs.json

import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.frontend_operations
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.serving.FrontendJson._

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
  case class GlobalSQLRequest(query: String, checkpoints: Map[String, String], limit: Int)
  // Each row is a map, repeating the schema. Values may be missing for some rows.
  case class TableResult(rows: List[Map[String, json.JsValue]])
  implicit val wCheckpointResponse = json.Json.writes[CheckpointResponse]
  implicit val rOperationRequest = json.Json.reads[OperationRequest]
  implicit val rProjectRequest = json.Json.reads[ProjectRequest]
  implicit val rSaveProjectRequest = json.Json.reads[SaveProjectRequest]
  implicit val rScalarRequest = json.Json.reads[ScalarRequest]
  implicit val rProjectSQLRequest = json.Json.reads[ProjectSQLRequest]
  implicit val rGlobalSQLRequest = json.Json.reads[GlobalSQLRequest]
  implicit val wDynamicValue = json.Json.writes[DynamicValue]
  implicit val wTableResult = json.Json.writes[TableResult]
}

object RemoteAPIServer extends JsonServer {
  import RemoteAPIProtocol._
  val userController = ProductionJsonServer.userController
  val c = new RemoteAPIController(BigGraphProductionEnvironment)
  def getScalar = jsonPost(c.getScalar)
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
    registerTablesOfRootProject(sqlContext, "", request.checkpoint)
    val df = sqlContext.sql(request.query)
    dfToTableResult(df, request.limit)
  }

  def globalSQL(user: User, request: GlobalSQLRequest): TableResult = {
    val sqlContext = dataManager.newHiveContext()
    // Register tables
    for ((name, cp) <- request.checkpoints)
      registerTablesOfRootProject(sqlContext, name + "|", cp)
    val df = sqlContext.sql(request.query)
    dfToTableResult(df, request.limit)
  }

  // Takes all the tables in the rootproject given by the checkpoint and registers all of them with prefixed name
  private def registerTablesOfRootProject(sqlContext: org.apache.spark.sql.hive.HiveContext,
                                          prefix: String, checkpoint: String) = {
    val viewer = getViewer(checkpoint)
    for (path <- viewer.allRelativeTablePaths) {
      controllers.Table(path, viewer).toDF(sqlContext).registerTempTable(prefix + path.toString)
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

  def importRequest[T <: GenericImportRequest: json.Writes](user: User, request: T): CheckpointResponse = {
    val res = sqlController.doImport(user, request)
    val (cp, _, _) = FEOption.unpackTitledCheckpoint(res.id)
    CheckpointResponse(cp)
  }

  def createView[T <: GenericImportRequest: json.Writes](user: User, request: T): CheckpointResponse = {
    val res = sqlController.saveView(user, request)
    val (cp, _, _) = FEOption.unpackTitledCheckpoint(res.id)
    CheckpointResponse(cp)
  }
}
