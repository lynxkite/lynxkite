// An API that allows controlling a running LynxKite instance via JSON commands.
package com.lynxanalytics.biggraph.serving

import java.io.FileInputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.URL

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.types
import play.api.libs.json

import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import com.lynxanalytics.biggraph.BigGraphProductionEnvironment
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.frontend_operations
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving.FrontendJson._

object RemoteAPI {
  val env = BigGraphProductionEnvironment
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager = env.dataManager
  val ops = new frontend_operations.Operations(env)
  val sqlController = new SQLController(env)

  def normalize(operation: String) = operation.replace("-", "").toLowerCase
  lazy val normalizedIds = ops.operationIds.map(id => normalize(id) -> id).toMap

  case class Command(command: String, payload: json.JsObject) {
    def execute(user: User): json.JsValue = {
      log.info(s"Executing Remote API command from $user: $this")
      import RemoteAPI.JsonFormatters._
      // Convenience wrappers.
      def run[Out: json.Writes](f: ((User) => Out)): json.JsValue =
        json.Json.toJson(f(user))
      def call[In: json.Reads, Out: json.Writes](f: ((User, In) => Out)): json.JsValue =
        json.Json.toJson(f(user, payload.as[In]))
      def doImport[T <: GenericImportRequest: json.Writes: json.Reads] =
        call[T, CheckpointResponse](importRequest)
      try {
        command match {
          case "getScalar" => call(getScalar)
          case "newProject" => run(newProject)
          case "loadProject" => call(loadProject)
          case "runOperation" => call(runOperation)
          case "saveProject" => call(saveProject)
          case "sql" => call(sql)
          case "importJdbc" => doImport[JdbcImportRequest]
          case "importHive" => doImport[HiveImportRequest]
          case "importCSV" => doImport[CSVImportRequest]
          case "importParquet" => doImport[ParquetImportRequest]
          case "importORC" => doImport[ORCImportRequest]
          case "importJson" => doImport[JsonImportRequest]
        }
      } catch {
        case t: Throwable =>
          log.error(s"Error while processing $this:", t)
          json.Json.obj("error" -> t.toString, "request" -> json.Json.toJson(this))
      }
    }
  }

  case class CheckpointResponse(checkpoint: String)
  case class OperationRequest(
    checkpoint: String,
    operation: String,
    parameters: Map[String, String])
  case class ProjectRequest(project: String)
  case class SaveProjectRequest(checkpoint: String, project: String)
  case class ScalarRequest(checkpoint: String, scalar: String)
  case class SqlRequest(checkpoint: String, query: String, limit: Int)
  // Each row is a map, repeating the schema. Values may be missing for some rows.
  case class TableResult(rows: List[Map[String, json.JsValue]])
  object JsonFormatters {
    implicit val fCommand = json.Json.format[Command]
    implicit val wCheckpointResponse = json.Json.writes[CheckpointResponse]
    implicit val rOperationRequest = json.Json.reads[OperationRequest]
    implicit val rProjectRequest = json.Json.reads[ProjectRequest]
    implicit val rSaveProjectRequest = json.Json.reads[SaveProjectRequest]
    implicit val rScalarRequest = json.Json.reads[ScalarRequest]
    implicit val rSqlRequest = json.Json.reads[SqlRequest]
    implicit val wDynamicValue = json.Json.writes[DynamicValue]
    implicit val wTableResult = json.Json.writes[TableResult]
  }

  def newProject(user: User): CheckpointResponse = {
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

  def sql(user: User, request: SqlRequest): TableResult = {
    val viewer = getViewer(request.checkpoint)
    val sqlContext = dataManager.masterHiveContext.newSession
    for (path <- viewer.allRelativeTablePaths) {
      controllers.Table(path, viewer).toDF(sqlContext).registerTempTable(path.toString)
    }
    val df = sqlContext.sql(request.query)
    val schema = df.schema
    val data = df.take(request.limit)
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

  def remote(user: User, command: Command) = {
    command.execute(user)
  }
}
