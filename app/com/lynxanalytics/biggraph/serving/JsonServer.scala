package com.lynxanalytics.biggraph.serving

import play.api.mvc
import play.api.libs.json
import play.api.libs.json._
import com.lynxanalytics.biggraph.BigGraphProductionEnvironment
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.controllers._
import play.api.libs.functional.syntax.toContraFunctorOps
import play.api.libs.json.Json.toJsFieldJsValueWrapper

object LynxUser extends securesocial.core.Authorization {
  def isAuthorized(user: securesocial.core.Identity) = user.email.get.endsWith("@lynxanalytics.com")
}

class JsonServer extends mvc.Controller with securesocial.core.SecureSocial {
  def testMode = play.api.Play.maybeApplication == None
  def productionMode = !testMode && play.api.Play.current.configuration.getString("application.secret").nonEmpty
  def action[A](parser: mvc.BodyParser[A])(block: mvc.Request[A] => mvc.Result): mvc.Action[A] = {
    // Turn off authentication in development mode.
    if (productionMode) {
      SecuredAction(authorize = LynxUser, ajaxCall = true)(parser)(block(_))
    } else {
      mvc.Action(parser)(block(_))
    }
  }

  def jsonPost[I: json.Reads, O: json.Writes](handler: I => O) = {
    action(parse.json) { request =>
      log.info(s"POST ${request.path} ${request.body}")
      request.body.validate[I].fold(
        errors => jsonBadRequest(errors),
        result => Ok(json.Json.toJson(handler(result))))
    }
  }

  def jsonGet[I: json.Reads, O: json.Writes](handler: I => O, key: String = "q") = {
    action(parse.anyContent) { request =>
      request.getQueryString(key) match {
        case Some(s) =>
          log.info(s"GET ${request.path} $s")
          Json.parse(s).validate[I].fold(
            errors => jsonBadRequest(errors),
            result => Ok(json.Json.toJson(handler(result))))
        case None => BadRequest(json.Json.obj(
          "status" -> "Error",
          "message" -> "Bad query string",
          "details" -> "You need to specify query parameter %s with a JSON value".format(key)))
      }
    }
  }

  def jsonBadRequest(
    details: Seq[(play.api.libs.json.JsPath, Seq[play.api.data.validation.ValidationError])]) = {
    log.error(s"Bad JSON: $details")
    BadRequest(json.Json.obj(
      "status" -> "Error",
      "message" -> "Bad JSON",
      "details" -> json.JsError.toFlatJson(details)))
  }
}

case class Empty(
  fake: Int = 0) // Needs fake field as JSON inception doesn't work otherwise.

object ProductionJsonServer extends JsonServer {
  /**
   * Implicit JSON inception
   *
   * json.Json.toJson needs one for every incepted case class,
   * they need to be ordered so that everything is declared before use.
   */

  // TODO: do this without a fake field, e.g. by not using inception.
  implicit val rEmpty = json.Json.reads[Empty]
  implicit val wUnit = new json.Writes[Unit] {
    def writes(u: Unit) = Json.obj()
  }

  implicit val rVertexSetRequest = json.Json.reads[VertexSetRequest]
  implicit val wFEStatus = json.Json.writes[FEStatus]
  implicit val wUIValue = json.Json.writes[UIValue]
  implicit val wFEOperationParameterMeta = json.Json.writes[FEOperationParameterMeta]
  implicit val wFEOperationMeta = json.Json.writes[FEOperationMeta]
  implicit val wFEEdgeBundle = json.Json.writes[FEEdgeBundle]
  implicit val wFEVertexSet = json.Json.writes[FEVertexSet]

  implicit val rFEOperationSpec = json.Json.reads[FEOperationSpec]

  implicit val rSetClusterNumInstanceRequest = json.Json.reads[SetClusterNumInstanceRequest]
  implicit val wSparkClusterStatusResponse = json.Json.writes[SparkClusterStatusResponse]

  implicit val rFEVertexAttributeFilter = json.Json.reads[FEVertexAttributeFilter]
  implicit val rVertexDiagramSpec = json.Json.reads[VertexDiagramSpec]
  implicit val wFEVertex = json.Json.writes[FEVertex]
  implicit val wVertexDiagramResponse = json.Json.writes[VertexDiagramResponse]

  implicit val rBundleSequenceStep = json.Json.reads[BundleSequenceStep]
  implicit val rEdgeDiagramSpec = json.Json.reads[EdgeDiagramSpec]
  implicit val wFEEdge = json.Json.writes[FEEdge]
  implicit val wEdgeDiagramResponse = json.Json.writes[EdgeDiagramResponse]

  implicit val rFEGraphRequest = json.Json.reads[FEGraphRequest]
  implicit val wFEGraphResponse = json.Json.writes[FEGraphResponse]

  implicit val rCenterRequest = json.Json.reads[CenterRequest]
  implicit val wCenterResponse = json.Json.writes[CenterResponse]

  implicit val rHistogramSpec = json.Json.reads[HistogramSpec]
  implicit val wHistogramResponse = json.Json.writes[HistogramResponse]

  implicit val rScalarValueRequest = json.Json.reads[ScalarValueRequest]
  implicit val wScalarValueResponse = json.Json.writes[ScalarValueResponse]

  implicit val rCreateProjectRequest = json.Json.reads[CreateProjectRequest]
  implicit val rDiscardProjectRequest = json.Json.reads[DiscardProjectRequest]
  implicit val rProjectRequest = json.Json.reads[ProjectRequest]
  implicit val rProjectOperationRequest = json.Json.reads[ProjectOperationRequest]
  implicit val rProjectFilterRequest = json.Json.reads[ProjectFilterRequest]
  implicit val rForkProjectRequest = json.Json.reads[ForkProjectRequest]
  implicit val rUndoProjectRequest = json.Json.reads[UndoProjectRequest]
  implicit val rRedoProjectRequest = json.Json.reads[RedoProjectRequest]
  implicit val wOperationCategory = json.Json.writes[OperationCategory]
  implicit val wFEAttribute = json.Json.writes[FEAttribute]
  implicit val wFESegmentation = json.Json.writes[FESegmentation]
  implicit val wFEProject = json.Json.writes[FEProject]
  implicit val wSplash = json.Json.writes[Splash]

  // File upload.
  def upload = {
    action(parse.multipartFormData) { request =>
      val upload = request.body.file("file").get
      log.info(s"upload: ${upload.filename}")
      val dataRepo = BigGraphProductionEnvironment.dataManager.repositoryPath
      val output = dataRepo / "uploads" / upload.filename.replace(" ", "_")
      val stream = output.create()
      try java.nio.file.Files.copy(upload.ref.file.toPath, stream)
      finally stream.close()
      Ok(output.fullString)
    }
  }

  // Methods called by the web framework
  //
  // Play! uses the routings in /conf/routes to execute actions

  val bigGraphController = new BigGraphController(BigGraphProductionEnvironment)
  def vertexSetGet = jsonGet(bigGraphController.vertexSet)
  def applyOpGet = jsonGet(bigGraphController.applyOp)
  def startingOperationsGet = jsonGet(bigGraphController.startingOperations)
  def startingVertexSetsGet = jsonGet(bigGraphController.startingVertexSets)
  def createProject = jsonPost(bigGraphController.createProject)
  def discardProject = jsonPost(bigGraphController.discardProject)
  def projectOp = jsonPost(bigGraphController.projectOp)
  def project = jsonGet(bigGraphController.project)
  def splash = jsonGet(bigGraphController.splash)
  def filterProject = jsonPost(bigGraphController.filterProject)
  def forkProject = jsonPost(bigGraphController.forkProject)
  def undoProject = jsonPost(bigGraphController.undoProject)
  def redoProject = jsonPost(bigGraphController.redoProject)

  val sparkClusterController = new SparkClusterController(BigGraphProductionEnvironment)
  def getClusterStatus = jsonGet(sparkClusterController.getClusterStatus)
  def setClusterNumInstances = jsonGet(sparkClusterController.setClusterNumInstances)

  val drawingController = new GraphDrawingController(BigGraphProductionEnvironment)
  def vertexDiagram = jsonGet(drawingController.getVertexDiagram)
  def edgeDiagram = jsonGet(drawingController.getEdgeDiagram)
  def complexView = jsonGet(drawingController.getComplexView)
  def center = jsonGet(drawingController.getCenter)
  def histo = jsonGet(drawingController.getHistogram)
  def scalarValue = jsonGet(drawingController.getScalarValue)
}
