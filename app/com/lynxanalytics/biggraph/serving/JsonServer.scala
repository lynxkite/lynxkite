// The controller to receive and dispatch all JSON HTTP requests from the frontend.
package com.lynxanalytics.biggraph.serving

import play.api.libs.json
import play.api.mvc
import play.Play
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import com.lynxanalytics.biggraph.BigGraphProductionEnvironment
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.protection.Limitations

import java.io.File

class JsonServer extends mvc.Controller {
  def testMode = play.api.Play.maybeApplication == None
  def productionMode = !testMode && play.api.Play.current.configuration.getString("application.secret").nonEmpty

  def action[A](parser: mvc.BodyParser[A], withAuth: Boolean = productionMode)(
    block: (User, mvc.Request[A]) => mvc.Result): mvc.Action[A] = {

    asyncAction(parser, withAuth) { (user, request) =>
      Future(block(user, request))
    }
  }

  def asyncAction[A](parser: mvc.BodyParser[A], withAuth: Boolean = productionMode)(
    block: (User, mvc.Request[A]) => Future[mvc.Result]): mvc.Action[A] = {
    if (withAuth) {
      // TODO: Redirect HTTP to HTTPS. (#1400)
      mvc.Action.async(parser) { request =>
        UserProvider.get(request) match {
          case Some(user) => block(user, request)
          case None => Future.successful(Unauthorized)
        }
      }
    } else {
      // No authentication in development mode.
      mvc.Action.async(parser) { request => block(User.fake, request) }
    }
  }

  def jsonPost[I: json.Reads, O: json.Writes](
    handler: (User, I) => O,
    logRequest: Boolean = true) = {
    val t0 = System.currentTimeMillis
    action(parse.json) { (user, request) =>
      if (logRequest) {
        log.info(s"$user POST ${request.path} ${request.body}")
      } else {
        log.info(s"$user POST ${request.path} (request body logging supressed)")
      }
      val i = request.body.as[I]
      val result = util.Try(Ok(json.Json.toJson(handler(user, i))))
      val dt = System.currentTimeMillis - t0
      val status = if (result.isSuccess) "success" else "failure"
      log.info(s"$dt ms to respond with $status to $user POST ${request.path}")
      result.get
    }
  }

  def jsonQuery[I: json.Reads, R](
    user: User,
    request: mvc.Request[mvc.AnyContent])(handler: (User, I) => R): R = {
    val t0 = System.currentTimeMillis
    val key = "q"
    val value = request.getQueryString(key)
    assert(value.nonEmpty, s"Missing query parameter $key.")
    val s = value.get
    log.info(s"$user GET ${request.path} $s")
    val i = json.Json.parse(s).as[I]
    val result = util.Try(handler(user, i))
    val dt = System.currentTimeMillis - t0
    val status = if (result.isSuccess) "success" else "failure"
    log.info(s"$dt ms to respond with $status to $user GET ${request.path}")
    result.get
  }

  def jsonGet[I: json.Reads, O: json.Writes](handler: (User, I) => O) = {
    action(parse.anyContent) { (user, request) =>
      jsonQuery(user, request) { (user: User, i: I) =>
        try {
          Ok(json.Json.toJson(handler(user, i)))
        } catch {
          case flying: FlyingResult => flying.result
        }
      }
    }
  }

  // An non-authenticated, no input GET request that returns JSon.
  def jsonPublicGet[O: json.Writes](handler: => O) = {
    action(parse.anyContent, withAuth = false) { (user, request) =>
      try {
        Ok(json.Json.toJson(handler))
      } catch {
        case flying: FlyingResult => flying.result
      }
    }
  }

  def jsonFuture[I: json.Reads, O: json.Writes](handler: (User, I) => Future[O]) = {
    asyncAction(parse.anyContent) { (user, request) =>
      jsonQuery(user, request) { (user: User, i: I) =>
        handler(user, i).map(o => Ok(json.Json.toJson(o)))
      }
    }
  }

  def healthCheck(checkHealthy: () => Unit) = mvc.Action { request =>
    checkHealthy()
    Ok("Server healthy")
  }
}

case class Empty(
  fake: Int = 0) // Needs fake field as JSON inception doesn't work otherwise.

case class GlobalSettings(
  hasAuth: Boolean,
  title: String,
  tagline: String,
  version: String)

object ProductionJsonServer extends JsonServer {
  // We check if licence is still valid.
  if (Limitations.isExpired()) {
    val message = "Your licence has expired, please contact Lynx Analytics for a new licence."
    println(message)
    log.error(message)
    System.exit(1)
  }

  /**
   * Implicit JSON inception
   *
   * json.Json.toJson needs one for every incepted case class,
   * they need to be ordered so that everything is declared before use.
   */

  // TODO: do this without a fake field, e.g. by not using inception.
  implicit val rEmpty = json.Json.reads[Empty]
  implicit val wUnit = new json.Writes[Unit] {
    def writes(u: Unit) = json.Json.obj()
  }

  implicit val wFEStatus = json.Json.writes[FEStatus]
  implicit val wUIValue = json.Json.writes[UIValue]
  implicit val wUIValues = json.Json.writes[UIValues]
  implicit val wFEOperationParameterMeta = json.Json.writes[FEOperationParameterMeta]
  implicit val wFEOperationMeta = json.Json.writes[FEOperationMeta]

  implicit val rFEOperationSpec = json.Json.reads[FEOperationSpec]

  implicit val rSparkStatusRequest = json.Json.reads[SparkStatusRequest]
  implicit val rSetClusterNumInstanceRequest = json.Json.reads[SetClusterNumInstanceRequest]
  implicit val wStageInfo = json.Json.writes[StageInfo]
  implicit val wSparkStatusResponse = json.Json.writes[SparkStatusResponse]
  implicit val wSparkClusterStatusResponse = json.Json.writes[SparkClusterStatusResponse]

  implicit val rFEVertexAttributeFilter = json.Json.reads[FEVertexAttributeFilter]
  implicit val rAxisOptions = json.Json.reads[AxisOptions]
  implicit val rVertexDiagramSpec = json.Json.reads[VertexDiagramSpec]
  implicit val wDynamicValue = json.Json.writes[DynamicValue]
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

  implicit val rCreateProjectRequest = json.Json.reads[CreateProjectRequest]
  implicit val rDiscardProjectRequest = json.Json.reads[DiscardProjectRequest]
  implicit val rRenameProjectRequest = json.Json.reads[RenameProjectRequest]
  implicit val rProjectRequest = json.Json.reads[ProjectRequest]
  implicit val rProjectOperationRequest = json.Json.reads[ProjectOperationRequest]
  implicit val rSubProjectOperation = json.Json.reads[SubProjectOperation]
  implicit val rProjectAttributeFilter = json.Json.reads[ProjectAttributeFilter]
  implicit val rProjectFilterRequest = json.Json.reads[ProjectFilterRequest]
  implicit val rForkProjectRequest = json.Json.reads[ForkProjectRequest]
  implicit val rUndoProjectRequest = json.Json.reads[UndoProjectRequest]
  implicit val rRedoProjectRequest = json.Json.reads[RedoProjectRequest]
  implicit val rProjectSettingsRequest = json.Json.reads[ProjectSettingsRequest]
  implicit val rHistoryRequest = json.Json.reads[HistoryRequest]
  implicit val rAlternateHistory = json.Json.reads[AlternateHistory]
  implicit val rSaveHistoryRequest = json.Json.reads[SaveHistoryRequest]
  implicit val rSaveWorkflowRequest = json.Json.reads[SaveWorkflowRequest]
  implicit val rProjectListRequest = json.Json.reads[ProjectListRequest]
  implicit val wOperationCategory = json.Json.writes[OperationCategory]
  implicit val wFEAttribute = json.Json.writes[FEAttribute]
  implicit val wFESegmentation = json.Json.writes[FESegmentation]
  implicit val wFEProject = json.Json.writes[FEProject]
  implicit val wFEProjectListElement = json.Json.writes[FEProjectListElement]
  implicit val wProjectList = json.Json.writes[ProjectList]
  implicit val wFEOperationSpec = json.Json.writes[FEOperationSpec]
  implicit val wSubProjectOperation = json.Json.writes[SubProjectOperation]
  implicit val wProjectHistoryStep = json.Json.writes[ProjectHistoryStep]
  implicit val wProjectHistory = json.Json.writes[ProjectHistory]

  implicit val wDemoModeStatusResponse = json.Json.writes[DemoModeStatusResponse]

  implicit val rChangeUserPasswordRequest = json.Json.reads[ChangeUserPasswordRequest]
  implicit val rCreateUserRequest = json.Json.reads[CreateUserRequest]
  implicit val wUser = json.Json.writes[User]
  implicit val wUserList = json.Json.writes[UserList]

  implicit val wGlobalSettings = json.Json.writes[GlobalSettings]

  implicit val rMarkDeletedRequest = json.Json.reads[MarkDeletedRequest]
  implicit val wDataFilesStats = json.Json.writes[DataFilesStats]
  implicit val wDataFilesStatus = json.Json.writes[DataFilesStatus]

  // File upload.
  def upload = {
    action(parse.multipartFormData) { (user, request) =>
      val upload: mvc.MultipartFormData.FilePart[play.api.libs.Files.TemporaryFile] =
        request.body.file("file").get
      try {
        val size = upload.ref.file.length
        log.info(s"upload: $user ${upload.filename} ($size bytes)")
        val dataRepo = BigGraphProductionEnvironment.dataManager.repositoryPath
        val baseName = upload.filename.replace(" ", "_")
        val tmpName = s"$baseName.$Timestamp"
        val tmpFile = dataRepo / "tmp" / tmpName
        val md = java.security.MessageDigest.getInstance("MD5");
        val stream = new java.security.DigestOutputStream(tmpFile.create(), md)
        try java.nio.file.Files.copy(upload.ref.file.toPath, stream)
        finally stream.close()
        val digest = md.digest().map("%02x".format(_)).mkString
        val finalName = s"$baseName.$digest"
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
      } finally upload.ref.clean() // Delete temporary file.
    }
  }

  def download = action(parse.anyContent) { (user, request) =>
    import play.api.libs.concurrent.Execution.Implicits._
    import scala.collection.JavaConversions._
    log.info(s"download: $user ${request.path}")
    val path = HadoopFile(request.getQueryString("path").get)
    val name = request.getQueryString("name").get
    // For now this is about CSV downloads. We want to read the "header" file and then the "data" directory.
    val files = Seq(path / "header") ++ (path / "data" / "*").list
    val length = files.map(_.length).sum
    log.info(s"downloading $length bytes: $files")
    val stream = new java.io.SequenceInputStream(files.view.map(_.open).iterator)
    mvc.Result(
      header = mvc.ResponseHeader(200, Map(
        CONTENT_LENGTH -> length.toString,
        CONTENT_DISPOSITION -> s"attachment; filename=$name.csv")),
      body = play.api.libs.iteratee.Enumerator.fromStream(stream)
    )
  }

  def logs = action(parse.anyContent) { (user, request) =>
    assert(user.isAdmin, "Only admins can access the server logs")
    val logDir = Play.application.getFile("logs")
    assert(logDir.exists, "Application log directory not found")
    assert(logDir.isDirectory, "'logs' is not a directory")
    val logFileNames = logDir.listFiles
      .filter(_.isFile)
      .map { file => file.getName }
      .filter(_.startsWith("application"))
    assert(logFileNames.size > 0, "No application log file found")
    val logFile = new File(logDir, logFileNames.max)
    log.info(s"$user has downloaded log file $logFile")
    Ok.sendFile(logFile)
  }

  def jsError = mvc.Action(parse.json) { request =>
    val url = (request.body \ "url").as[String]
    val stack = (request.body \ "stack").as[String]
    log.info(s"JS error at $url:\n$stack")
    Ok("logged")
  }

  // Methods called by the web framework
  //
  // Play! uses the routings in /conf/routes to execute actions

  val bigGraphController = new BigGraphController(BigGraphProductionEnvironment)
  def createProject = jsonPost(bigGraphController.createProject)
  def discardProject = jsonPost(bigGraphController.discardProject)
  def renameProject = jsonPost(bigGraphController.renameProject)
  def projectOp = jsonPost(bigGraphController.projectOp)
  def project = jsonGet(bigGraphController.project)
  def projectList = jsonGet(bigGraphController.projectList)
  def filterProject = jsonPost(bigGraphController.filterProject)
  def forkProject = jsonPost(bigGraphController.forkProject)
  def undoProject = jsonPost(bigGraphController.undoProject)
  def redoProject = jsonPost(bigGraphController.redoProject)
  def changeProjectSettings = jsonPost(bigGraphController.changeProjectSettings)
  def getHistory = jsonGet(bigGraphController.getHistory)
  def validateHistory = jsonPost(bigGraphController.validateHistory)
  def saveHistory = jsonPost(bigGraphController.saveHistory)
  def saveWorkflow = jsonPost(bigGraphController.saveWorkflow)

  val sparkClusterController = new SparkClusterController(BigGraphProductionEnvironment)
  def getClusterStatus = jsonGet(sparkClusterController.getClusterStatus)
  def setClusterNumInstances = jsonGet(sparkClusterController.setClusterNumInstances)
  def sparkStatus = jsonFuture(sparkClusterController.sparkStatus)
  def sparkCancelJobs = jsonPost(sparkClusterController.sparkCancelJobs)
  def sparkHealthCheck = healthCheck(sparkClusterController.checkSparkOperational)

  val drawingController = new GraphDrawingController(BigGraphProductionEnvironment)
  def complexView = jsonGet(drawingController.getComplexView)
  def center = jsonGet(drawingController.getCenter)
  def histo = jsonGet(drawingController.getHistogram)
  def scalarValue = jsonGet(drawingController.getScalarValue)

  val demoModeController = new DemoModeController(BigGraphProductionEnvironment)
  def demoModeStatus = jsonGet(demoModeController.demoModeStatus)
  def enterDemoMode = jsonGet(demoModeController.enterDemoMode)
  def exitDemoMode = jsonGet(demoModeController.exitDemoMode)

  def getUsers = jsonGet(UserProvider.getUsers)
  def changeUserPassword = jsonPost(UserProvider.changeUserPassword, logRequest = false)
  def createUser = jsonPost(UserProvider.createUser, logRequest = false)

  val cleanerController = new CleanerController(BigGraphProductionEnvironment)
  def getDataFilesStatus = jsonGet(cleanerController.getDataFilesStatus)
  def markFilesDeleted = jsonPost(cleanerController.markFilesDeleted)
  def deleteMarkedFiles = jsonPost(cleanerController.deleteMarkedFiles)

  lazy val version = try {
    scala.io.Source.fromFile(util.Properties.userDir + "/version").mkString
  } catch {
    case e: java.io.IOException => ""
  }
  def getGlobalSettings = jsonPublicGet {
    GlobalSettings(
      hasAuth = productionMode,
      title = util.Properties.envOrElse("KITE_TITLE", "LynxKite"),
      tagline = util.Properties.envOrElse("KITE_TAGLINE", "Graph analytics for the brave"),
      version = version)
  }

  val copyController = new CopyController(BigGraphProductionEnvironment)
  def copyEphemeral = jsonPost(copyController.copyEphemeral)
}

// Throw FlyingResult anywhere to generate non-200 HTTP responses.
class FlyingResult(val result: mvc.Result) extends Exception(result.toString)
