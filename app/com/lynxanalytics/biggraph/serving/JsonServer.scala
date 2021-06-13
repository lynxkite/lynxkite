// The controller to receive and dispatch all JSON HTTP requests from the frontend.
package com.lynxanalytics.biggraph.serving

import java.io.{File, FileOutputStream}

import play.api.libs.json
import play.api.mvc

import scala.concurrent.Future
import com.lynxanalytics.biggraph.BigGraphProductionEnvironment
import com.lynxanalytics.biggraph.{bigGraphLogger => log}
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_util.SoftHashMap
import com.lynxanalytics.biggraph.graph_util.{HadoopFile, KiteInstanceInfo, LoggedEnvironment, Timestamp}
import com.lynxanalytics.biggraph.model
import com.lynxanalytics.biggraph.serving
import org.apache.spark.sql.types.{StructField, StructType}
import play.api.mvc.AnyContent
import play.api.mvc.Request
import play.api.mvc.ResponseHeader

abstract class JsonServer @javax.inject.Inject() (
    implicit
    ec: concurrent.ExecutionContext,
    fmt: play.api.http.FileMimeTypes,
    cfg: play.api.Configuration,
    val controllerComponents: mvc.ControllerComponents)
    extends mvc.BaseController {
  def productionMode = cfg.get[String]("play.http.secret.key").nonEmpty

  def action[A](parser: mvc.BodyParser[A], withAuth: Boolean = productionMode)(
      handler: (User, mvc.Request[A]) => mvc.Result): mvc.Action[A] = {

    asyncAction(parser, withAuth) { (user, request) =>
      Future(handler(user, request))
    }
  }

  def getUser(request: mvc.Request[_], withAuth: Boolean = productionMode): Option[User] = {
    Some(User.singleuser)
  }

  def asyncAction[A](parser: mvc.BodyParser[A], withAuth: Boolean = productionMode)(
      handler: (User, mvc.Request[A]) => Future[mvc.Result]): mvc.Action[A] = {
    Action.async(parser) { request =>
      getUser(request, withAuth) match {
        case Some(user) => handler(user, request)
        case None => Future.successful(Unauthorized)
      }
    }
  }

  def jsonPostCommon[I: json.Reads, R](
      user: User,
      request: mvc.Request[json.JsValue],
      logRequest: Boolean = true)(
      handler: (User, I) => R): R = {
    val t0 = System.currentTimeMillis
    if (logRequest) {
      log.info(s"${request.remoteAddress} $user POST ${request.path} ${request.body}")
    } else {
      log.info(s"${request.remoteAddress} $user POST ${request.path} (request body logging supressed)")
    }
    val i = request.body.as[I]
    val result = util.Try(handler(user, i))
    val dt = System.currentTimeMillis - t0
    val status = if (result.isSuccess) "success" else "failure"
    log.info(s"$dt ms to respond with $status to ${request.remoteAddress} $user POST ${request.path}")
    result.get
  }

  def jsonPost[I: json.Reads, O: json.Writes](
      handler: (User, I) => O,
      logRequest: Boolean = true) = {
    action(parse.json) { (user, request) =>
      jsonPostCommon(user, request, logRequest) { (user: User, i: I) =>
        Ok(json.Json.toJson(handler(user, i)))
      }
    }
  }

  def jsonFuturePost[I: json.Reads, O: json.Writes](
      handler: (User, I) => Future[O],
      logRequest: Boolean = true) = {
    asyncAction(parse.json) { (user, request) =>
      jsonPostCommon(user, request, logRequest) { (user: User, i: I) =>
        handler(user, i).map(o => Ok(json.Json.toJson(o)))
      }
    }
  }

  def parseJson[T: json.Reads](user: User, request: mvc.Request[mvc.AnyContent]): T = {
    // We do our own simple parsing instead of using request.getQueryString due to #2507.
    val qs = request.rawQueryString
    assert(qs.startsWith("q="), "Missing query parameter: q")
    val s = java.net.URLDecoder.decode(qs.drop(2), "utf8")
    log.info(s"${request.remoteAddress} $user GET ${request.path} $s")
    json.Json.parse(s).as[T]
  }

  def jsonQuery[I: json.Reads, R](
      user: User,
      request: mvc.Request[mvc.AnyContent])(handler: (User, I) => R): R = {
    val t0 = System.currentTimeMillis
    val result = util.Try(handler(user, parseJson(user, request)))
    val dt = System.currentTimeMillis - t0
    val status = if (result.isSuccess) "success" else "failure"
    log.info(s"$dt ms to respond with $status to ${request.remoteAddress} $user GET ${request.path}")
    result.get
  }

  def jsonGet[I: json.Reads, O: json.Writes](handler: (User, I) => O) = {
    action(parse.anyContent) { (user, request) =>
      assert(
        request.headers.get("X-Requested-With") == Some("XMLHttpRequest"),
        "Rejecting request because 'X-Requested-With: XMLHttpRequest' header is missing.")
      jsonQuery(user, request) { (user: User, i: I) =>
        Ok(json.Json.toJson(handler(user, i)))
      }
    }
  }

  // An non-authenticated, no input GET request that returns JSON.
  def jsonPublicGet[O: json.Writes](handler: => O) = {
    action(parse.anyContent, withAuth = false) { (user, request) =>
      log.info(s"${request.remoteAddress} GET ${request.path}")
      Ok(json.Json.toJson(handler))
    }
  }

  def jsonFuture[I: json.Reads, O: json.Writes](handler: (User, I) => Future[O]) = {
    asyncAction(parse.anyContent) { (user, request) =>
      assert(
        request.headers.get("X-Requested-With") == Some("XMLHttpRequest"),
        "Rejecting request because 'X-Requested-With: XMLHttpRequest' header is missing.")
      jsonQuery(user, request) { (user: User, i: I) =>
        handler(user, i).map(o => Ok(json.Json.toJson(o)))
      }
    }
  }

  def healthCheck(checkHealthy: () => Unit) = Action { request =>
    log.info(s"${request.remoteAddress} GET ${request.path}")
    checkHealthy()
    Ok("Server healthy")
  }
}

// Throw this exception to immediately return a given HTTP result.
case class ResultException(result: mvc.Result) extends Exception

case class Empty()

case class GlobalSettings(
    title: String,
    tagline: String,
    frontendConfig: String,
    workspaceParameterKinds: List[String],
    version: String,
    graphrayEnabled: Boolean,
    dataCollectionMode: String,
    defaultUIStatus: UIStatus)

object AssertNotRunningAndRegisterRunning {
  private def getPidFile(pidFilePath: String): File = {
    val pidFile = new File(pidFilePath).getAbsoluteFile
    if (pidFile.exists) {
      throw new RuntimeException(s"LynxKite is already running (or delete $pidFilePath)")
    }
    pidFile
  }

  private def getPid(): String = {

    val pidAtHostname =
      // Returns <pid>@<hostname>
      java.lang.management.ManagementFactory.getRuntimeMXBean.getName
    val atSignIndex = pidAtHostname.indexOf('@')
    pidAtHostname.take(atSignIndex)
  }

  private def writePid(pidFile: File) = {
    val pid = getPid()
    val output = new FileOutputStream(pidFile)
    try output.write(pid.getBytes)
    finally output.close()
  }

  def apply() = {
    val pidFilePath = LoggedEnvironment.envOrNone("KITE_PID_FILE")
    if (pidFilePath.isDefined) {
      val pidFile = getPidFile(pidFilePath.get)
      writePid(pidFile)
      pidFile.deleteOnExit()
    }
  }
}

object FrontendJson {

  /** Implicit JSON inception
    *
    * json.Json.toJson needs one for every incepted case class,
    * they need to be ordered so that everything is declared before use.
    */
  import model.FEModel
  import model.FEModelMeta

  implicit val rEmpty = new json.Reads[Empty] {
    def reads(j: json.JsValue) = json.JsSuccess(Empty())
  }
  implicit val wUnit = new json.Writes[Unit] {
    def writes(u: Unit) = json.Json.obj()
  }
  implicit val wStructType = new json.Writes[StructType] {
    def writes(structType: StructType): json.JsValue = json.Json.obj("schema" -> structType.map {
      case StructField(name, dataType: StructType, nullable, metaData) =>
        json.Json.obj("name" -> name, "dataType" -> this.writes(dataType), "nullable" -> nullable)
      case StructField(name, dataType, nullable, metaData) =>
        json.Json.obj("name" -> name, "dataType" -> dataType.toString(), "nullable" -> nullable)
    })
  }
  implicit val fDownloadFileRequest = json.Json.format[DownloadFileRequest]

  implicit val fFEStatus = FEStatus.format
  implicit val fFEOption = json.Json.format[FEOption]
  implicit val wFEOperationParameterMeta = json.Json.writes[FEOperationParameterMeta]
  implicit val fCustomOperationParameterMeta = json.Json.format[CustomOperationParameterMeta]
  implicit val wDynamicValue = json.Json.writes[DynamicValue]
  implicit val wFEScalar = json.Json.writes[FEScalar]
  implicit val wFEOperationMeta = json.Json.writes[FEOperationMeta]

  implicit val rFEOperationSpec = json.Json.reads[FEOperationSpec]

  implicit val rFEVertexAttributeFilter = json.Json.reads[FEVertexAttributeFilter]
  implicit val rAxisOptions = json.Json.reads[AxisOptions]
  implicit val rVertexDiagramSpec = json.Json.reads[VertexDiagramSpec]
  implicit val wFEModel = json.Json.writes[FEModel]
  implicit val wFEModelMeta = json.Json.writes[FEModelMeta]
  implicit val wFEVertex = json.Json.writes[FEVertex]
  implicit val wVertexDiagramResponse = json.Json.writes[VertexDiagramResponse]

  implicit val rBundleSequenceStep = json.Json.reads[BundleSequenceStep]
  implicit val rAggregatedAttribute = json.Json.reads[AggregatedAttribute]
  implicit val rEdgeDiagramSpec = json.Json.reads[EdgeDiagramSpec]
  implicit val wFE3DPosition = json.Json.writes[FE3DPosition]
  implicit val wFEEdge = json.Json.writes[FEEdge]
  implicit val wEdgeDiagramResponse = json.Json.writes[EdgeDiagramResponse]

  implicit val rFEGraphRequest = json.Json.reads[FEGraphRequest]
  implicit val wFEGraphResponse = json.Json.writes[FEGraphResponse]

  implicit val rCenterRequest = json.Json.reads[CenterRequest]
  implicit val wCenterResponse = json.Json.writes[CenterResponse]

  implicit val rHistogramSpec = json.Json.reads[HistogramSpec]
  implicit val wHistogramResponse = json.Json.writes[HistogramResponse]

  implicit val rScalarValueRequest = json.Json.reads[ScalarValueRequest]

  implicit val rCreateDirectoryRequest = json.Json.reads[CreateDirectoryRequest]
  implicit val rDiscardEntryRequest = json.Json.reads[DiscardEntryRequest]
  implicit val rRenameEntryRequest = json.Json.reads[RenameEntryRequest]
  implicit val rProjectAttributeFilter = json.Json.reads[ProjectAttributeFilter]
  implicit val rForkEntryRequest = json.Json.reads[ForkEntryRequest]
  implicit val rACLSettingsRequest = json.Json.reads[ACLSettingsRequest]
  implicit val rEntryListRequest = json.Json.reads[EntryListRequest]
  implicit val rEntrySearchRequest = json.Json.reads[EntrySearchRequest]
  implicit val wFEOperationCategory = json.Json.writes[FEOperationCategory]
  implicit val wFEAttribute = json.Json.writes[FEAttribute]
  implicit val wFESegmentation = json.Json.writes[FESegmentation]
  implicit val wFEProject = json.Json.writes[FEProject]
  implicit val wFEEntryListElement = json.Json.writes[FEEntryListElement]
  implicit val wEntryList = json.Json.writes[EntryList]
  implicit val wFEOperationSpec = json.Json.writes[FEOperationSpec]

  import WorkspaceJsonFormatters._
  implicit val fBoxOutputInfo = json.Json.format[BoxOutputInfo]
  implicit val rWorkspaceReference = json.Json.reads[WorkspaceReference]
  implicit val wGetWorkspaceResponse = json.Json.writes[GetWorkspaceResponse]
  implicit val rRunWorkspaceRequest = json.Json.reads[RunWorkspaceRequest]
  implicit val wRunWorkspaceResponse = json.Json.writes[RunWorkspaceResponse]
  implicit val rSetWorkspaceRequest = json.Json.reads[SetWorkspaceRequest]
  implicit val rGetOperationMetaRequest = json.Json.reads[GetOperationMetaRequest]
  implicit val rGetProjectOutputRequest = json.Json.reads[GetProjectOutputRequest]
  implicit val rGetTableOutputRequest = json.Json.reads[GetTableOutputRequest]
  implicit val wTableColumn = json.Json.writes[TableColumn]
  implicit val wGetTableOutputResponse = json.Json.writes[GetTableOutputResponse]
  implicit val rGetPlotOutputRequest = json.Json.reads[GetPlotOutputRequest]
  implicit val rGetVisualizationOutputRequest = json.Json.reads[GetVisualizationOutputRequest]
  implicit val rCreateWorkspaceRequest = json.Json.reads[CreateWorkspaceRequest]
  implicit val rBoxCatalogRequest = json.Json.reads[BoxCatalogRequest]
  implicit val wBoxCatalogResponse = json.Json.writes[BoxCatalogResponse]
  implicit val rCreateSnapshotRequest = json.Json.reads[CreateSnapshotRequest]
  implicit val rGetExportResultRequest = json.Json.reads[GetExportResultRequest]
  implicit val wGetExportResultResponse = json.Json.writes[GetExportResultResponse]
  implicit val rInstrument = json.Json.reads[Instrument]
  implicit val rGetInstrumentedStateRequest = json.Json.reads[GetInstrumentedStateRequest]
  implicit val wInstrumentState = json.Json.writes[InstrumentState]
  implicit val wGetInstrumentedStateResponse = json.Json.writes[GetInstrumentedStateResponse]
  implicit val rOpenWizardRequest = json.Json.reads[OpenWizardRequest]
  implicit val wOpenWizardResponse = json.Json.writes[OpenWizardResponse]

  implicit val rLongPollRequest = json.Json.reads[LongPollRequest]
  implicit val wStageInfo = json.Json.writes[StageInfo]
  implicit val wSparkStatusResponse = json.Json.writes[SparkStatusResponse]
  implicit val wLongPollResponse = json.Json.writes[LongPollResponse]

  implicit val fDataFrameSpec = json.Json.format[TableSpec]
  implicit val rSQLTableBrowserNodeRequest = json.Json.reads[TableBrowserNodeRequest]
  implicit val rTableBrowserNodeForBoxRequest = json.Json.reads[TableBrowserNodeForBoxRequest]
  implicit val rImportBoxRequest = json.Json.reads[ImportBoxRequest]
  implicit val rSQLQueryRequest = json.Json.reads[SQLQueryRequest]
  implicit val fSQLExportToTableRequest = json.Json.format[SQLExportToTableRequest]
  implicit val rSQLExportToCSVRequest = json.Json.reads[SQLExportToCSVRequest]
  implicit val rSQLExportToJsonRequest = json.Json.reads[SQLExportToJsonRequest]
  implicit val rSQLExportToParquetRequest = json.Json.reads[SQLExportToParquetRequest]
  implicit val rSQLExportToORCRequest = json.Json.reads[SQLExportToORCRequest]
  implicit val rSQLExportToJdbcRequest = json.Json.reads[SQLExportToJdbcRequest]
  implicit val wTableDesc = json.Json.writes[TableBrowserNode]
  implicit val wSQLTableBrowserNodeResponse = json.Json.writes[TableBrowserNodeResponse]
  implicit val wSQLColumn = json.Json.writes[SQLColumn]
  implicit val wSQLQueryResult = json.Json.writes[SQLQueryResult]
  implicit val wSQLExportToFileResult = json.Json.writes[SQLExportToFileResult]
  implicit val wImportResult = json.Json.writes[ImportResult]

  import UIStatusSerialization.fUIStatus
  implicit val wGlobalSettings = json.Json.writes[GlobalSettings]

  implicit val wFileDescriptor = json.Json.writes[FileDescriptor]
  implicit val wLogFiles = json.Json.writes[LogFiles]
  implicit val rDownloadLogFileRequest = json.Json.reads[DownloadLogFileRequest]

  implicit val rMoveToTrashRequest = json.Json.reads[MoveToTrashRequest]
  implicit val rSetCleanerMinAgeRequest = json.Json.reads[SetCleanerMinAgeRequest]
  implicit val wDataFilesStats = json.Json.writes[DataFilesStats]
  implicit val wDataFilesStatus = json.Json.writes[DataFilesStatus]

  implicit val wBackupSettings = json.Json.writes[BackupSettings]
  implicit val wBackupVersion = json.Json.writes[BackupVersion]

}

@javax.inject.Singleton
class ProductionJsonServer @javax.inject.Inject() (
    implicit
    ec: concurrent.ExecutionContext,
    fmt: play.api.http.FileMimeTypes,
    cfg: play.api.Configuration,
    cc: mvc.ControllerComponents)
    extends JsonServer {
  import FrontendJson._
  import WorkspaceJsonFormatters._

  AssertNotRunningAndRegisterRunning()

  // File upload.
  def upload = {
    action(parse.multipartFormData) { (user, request) =>
      request.body.file("file").map { upload =>
        log.info(s"upload: $user ${upload.filename} (${upload.fileSize} bytes)")
        val dataRepo = BigGraphProductionEnvironment.sparkDomain.repositoryPath
        val baseName = upload.filename.replace(" ", "_")
        val tmpName = s"$baseName.$Timestamp"
        val tmpFile = dataRepo / "tmp" / tmpName
        val md = java.security.MessageDigest.getInstance("MD5");
        val stream = new java.security.DigestOutputStream(tmpFile.create(), md)
        try java.nio.file.Files.copy(upload.ref.path, stream)
        finally stream.close()
        val digest = md.digest().map("%02x".format(_)).mkString
        val finalName = s"$digest.$baseName"
        val uploadsDir = HadoopFile("UPLOAD$")
        uploadsDir.mkdirs() // Create the directory if it does not already exist.
        val finalFile = uploadsDir / finalName
        if (finalFile.exists) {
          log.info(s"The uploaded file ($tmpFile) already exists (as $finalFile).")
        } else {
          val success = tmpFile.renameTo(finalFile)
          assert(success, s"Failed to rename $tmpFile to $finalFile.")
        }
        Ok(finalFile.symbolicName)
      }.get
    }
  }

  def oldCSVDownload = action(parse.anyContent)(Downloads.oldCSVDownload)

  def downloadFile = action(parse.anyContent) {
    (user, request) => jsonQuery(user, request)(Downloads.downloadFile)
  }

  def downloadCSV = asyncAction(parse.anyContent) { (user: User, r: mvc.Request[mvc.AnyContent]) =>
    val request = parseJson[GetTableOutputRequest](user, r)
    implicit val metaManager = workspaceController.metaManager
    val table = workspaceController.getOutput(user, request.id).table
    sqlController.downloadCSV(table, request.sampleRows)
  }

  def jsError = Action(parse.json) { request =>
    val url = (request.body \ "url").as[String]
    val stack = (request.body \ "stack").as[String]
    log.info(s"JS error at $url:\n$stack")
    Ok("logged")
  }

  // Methods called by the web framework
  //
  // Play! uses the routings in /conf/routes to execute actions

  val bigGraphController = new BigGraphController(BigGraphProductionEnvironment)
  def createDirectory = jsonPost(bigGraphController.createDirectory)
  def discardEntry = jsonPost(bigGraphController.discardEntry)
  def renameEntry = jsonPost(bigGraphController.renameEntry)
  def discardAll = jsonPost(bigGraphController.discardAll)
  def entryList = jsonGet(bigGraphController.entryList)
  def entrySearch = jsonGet(bigGraphController.entrySearch)
  def forkEntry = jsonPost(bigGraphController.forkEntry)
  def changeACLSettings = jsonPost(bigGraphController.changeACLSettings)

  val workspaceController = new WorkspaceController(BigGraphProductionEnvironment)
  def createWorkspace = jsonPost(workspaceController.createWorkspace)
  def getWorkspace = jsonGet(workspaceController.getWorkspace)
  def runWorkspace = jsonPost(workspaceController.runWorkspace)
  def createSnapshot = jsonPost(workspaceController.createSnapshot)
  def getProjectOutput = jsonGet(workspaceController.getProjectOutput)
  def getOperationMeta = jsonGet(workspaceController.getOperationMeta)
  def setWorkspace = jsonPost(workspaceController.setWorkspace)
  def setAndGetWorkspace = jsonPost(workspaceController.setAndGetWorkspace)
  def undoWorkspace = jsonPost(workspaceController.undoWorkspace)
  def redoWorkspace = jsonPost(workspaceController.redoWorkspace)
  def boxCatalog = jsonGet(workspaceController.boxCatalog)
  def getPlotOutput = jsonGet(workspaceController.getPlotOutput)
  import UIStatusSerialization.fTwoSidedUIStatus
  def getVisualizationOutput = jsonGet(workspaceController.getVisualizationOutput)
  def getExportResultOutput = jsonGet(workspaceController.getExportResultOutput)
  def getInstrumentedState = jsonGet(workspaceController.getInstrumentedState)
  def openWizard = jsonPost(workspaceController.openWizard)

  val sqlController = new SQLController(BigGraphProductionEnvironment, workspaceController.ops)
  def getTableBrowserNodes = jsonGet(sqlController.getTableBrowserNodes)
  def runSQLQuery = jsonFuture(sqlController.runSQLQuery)

  def exportSQLQueryToCSV = jsonFuturePost(sqlController.exportSQLQueryToCSV)
  def exportSQLQueryToJson = jsonFuturePost(sqlController.exportSQLQueryToJson)
  def exportSQLQueryToParquet = jsonFuturePost(sqlController.exportSQLQueryToParquet)
  def exportSQLQueryToORC = jsonFuturePost(sqlController.exportSQLQueryToORC)
  def exportSQLQueryToJdbc = jsonFuturePost(sqlController.exportSQLQueryToJdbc)

  def importBox = jsonFuturePost(importBoxExec)
  def importBoxExec(user: serving.User, request: ImportBoxRequest): Future[ImportResult] = {
    val workspaceParams =
      if (request.ref.nonEmpty) {
        val wsRef = workspaceController.ResolvedWorkspaceReference(user, request.ref.get)
        wsRef.ws.workspaceExecutionContextParameters(wsRef.params)
      } else Map[String, String]()
    sqlController.importBox(user, request.box, workspaceParams)
  }

  def getTableOutput = jsonFuture(getTableOutputData)
  def getTableOutputData(user: serving.User, request: GetTableOutputRequest): Future[GetTableOutputResponse] = {
    implicit val metaManager = workspaceController.metaManager
    val table = workspaceController.getOutput(user, request.id).table
    sqlController.getTableSample(table, request.sampleRows)
  }
  def getTableBrowserNodesForBox =
    jsonGet(sqlController.getTableBrowserNodesForBox(workspaceController))

  val sparkClusterController =
    new SparkClusterController(BigGraphProductionEnvironment, workspaceController)
  def longPoll = jsonFuture(sparkClusterController.longPoll)
  def sparkCancelJobs = jsonPost(sparkClusterController.sparkCancelJobs)
  def restartApplication = jsonPost { (user, request: Empty) =>
    assert(user.isAdmin, "Restart is restricted to administrator users.")
    log.error(s"Restart requested by $user. Shutting down.")
    // Docker or Supervisor or a while loop is expected to start us up again after we exit.
    System.exit(1)
  }

  def getThreadDump = {
    action(parse.anyContent) { (user, request) =>
      assert(user.isAdmin, "Thread dump is restricted to administrator users.")
      log.info(s"GET ${request.path}")
      Ok(ThreadDumper.get)
    }
  }

  def sparkHealthCheck = healthCheck(sparkClusterController.checkSparkOperational)

  val drawingController = new GraphDrawingController(BigGraphProductionEnvironment)
  def complexView = jsonGet(drawingController.getComplexView)
  def center = jsonFuture(drawingController.getCenter)
  def histo = jsonFuture(drawingController.getHistogram)
  def scalarValue = jsonFuture(drawingController.getScalarValue)
  def model = jsonFuture(drawingController.getModel)
  def triggerBox = jsonFuturePost(triggerBoxExec)
  def triggerBoxExec(
      user: serving.User,
      request: GetOperationMetaRequest): Future[Unit] = {
    workspaceController.getOperation(user, request).asInstanceOf[TriggerableOperation]
      .trigger(workspaceController, drawingController)
  }

  val cleanerController = new CleanerController(BigGraphProductionEnvironment, workspaceController.ops)
  def getDataFilesStatus = jsonGet(cleanerController.getDataFilesStatus)
  def moveToCleanerTrash = jsonPost(cleanerController.moveToCleanerTrash)
  def emptyCleanerTrash = jsonPost(cleanerController.emptyCleanerTrash)
  def setCleanerMinAge = jsonPost(cleanerController.setCleanerMinAge)

  val logController = new LogController()
  def getLogFiles = jsonGet(logController.getLogFiles)
  def forceLogRotate = jsonPost(logController.forceLogRotate)
  def downloadLogFile = action(parse.anyContent) {
    (user, request) => jsonQuery(user, request)(logController.downloadLogFile)
  }

  val version = KiteInstanceInfo.kiteVersion

  def getGlobalSettings = jsonPublicGet {
    GlobalSettings(
      title = LoggedEnvironment.envOrElse("KITE_TITLE", "LynxKite"),
      tagline = LoggedEnvironment.envOrElse("KITE_TAGLINE", "The Complete Graph Data Science Platform"),
      frontendConfig = LoggedEnvironment.envOrElse("KITE_FRONTEND_CONFIG", "{}"),
      workspaceParameterKinds = CustomOperationParameterMeta.validKinds,
      version = version,
      graphrayEnabled = graphrayEnabled,
      dataCollectionMode = LoggedEnvironment.envOrElse("KITE_DATA_COLLECTION", "optional"),
      defaultUIStatus = UIStatus.default,
    )
  }

  val copyController = new CopyController(BigGraphProductionEnvironment, sparkClusterController)
  def copyEphemeral = jsonPost(copyController.copyEphemeral)
  def getBackupSettings = jsonGet(copyController.getBackupSettings)
  def backup = jsonGet(copyController.backup)

  val graphrayCache = new SoftHashMap[Int, Array[Byte]]()
  val graphrayEnabled: Boolean = {
    import scala.sys.process._
    // Try rendering an empty picture to see if it works.
    val config = new java.io.ByteArrayInputStream(
      "{\"vs\":[],\"es\":[],\"width\":10,\"height\":10,\"quality\":2}".getBytes)
    val output = new java.io.ByteArrayOutputStream()
    ("python3 -m graphray" #< config #> output).! == 0
  }
  def graphray = Action { request: Request[AnyContent] =>
    getUser(request) match {
      case None => Unauthorized
      case Some(user) =>
        log.info(s"$user ${request.method} ${request.path}")
        if (request.method == "POST") {
          import scala.sys.process._
          import java.nio.file._
          val config = new java.io.ByteArrayInputStream(request.body.asJson.get.toString().getBytes)
          val hash = config.hashCode()
          graphrayCache.getOrElseUpdate(
            hash, {
              val image = new java.io.ByteArrayOutputStream()
              ("python3 -m graphray" #< config #> image).!
              image.toByteArray
            })
          Ok(hash.toString)
        } else {
          val hash = request.queryString("q").head.toInt
          graphrayCache.get(hash) match {
            case Some(image) => Ok(image).as("image/png")
            case None => NotFound("Image not rendered. Post config first.")
          }
        }
    }
  }

  implicit val metaManager = workspaceController.metaManager
}
