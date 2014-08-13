package com.lynxanalytics.biggraph.serving

import play.api.mvc
import play.api.libs.json
import play.api.libs.json._
import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.controllers._
import play.api.libs.functional.syntax.toContraFunctorOps
import play.api.libs.json.Json.toJsFieldJsValueWrapper

class JsonServer extends mvc.Controller {
  def jsonPost[I: json.Reads, O: json.Writes](action: I => O) = {
    bigGraphLogger.info("JSON POST event received, function: " + action.getClass.toString())
    mvc.Action(parse.json) {
      request =>
        request.body.validate[I].fold(
          errors => {
            log.error(errors.toString)
            JsonBadRequest("Error", "Bad JSON", errors)
          },
          result => Ok(json.Json.toJson(action(result))))
    }
  }

  def jsonGet[I: json.Reads, O: json.Writes](action: I => O, key: String = "q") = {
    mvc.Action { request =>
      bigGraphLogger.info("JSON GET event received, function: %s, query key: %s"
        .format(action.getClass.toString(), key))
      request.getQueryString(key) match {
        case Some(s) => Json.parse(s).validate[I].fold(
          errors => {
            log.error(errors.toString)
            JsonBadRequest("Error", "Bad JSON", errors)
          },
          result => Ok(json.Json.toJson(action(result))))
        case None => BadRequest(json.Json.obj(
          "status" -> "Error",
          "message" -> "Bad query string",
          "details" -> "You need to specify query parameter %s with a JSON value".format(key)))
      }
    }
  }

  def JsonBadRequest(
    status: String,
    message: String,
    details: Seq[(play.api.libs.json.JsPath, Seq[play.api.data.validation.ValidationError])]) = {
    bigGraphLogger.error("Bad request: " + message)
    BadRequest(json.Json.obj(
      "status" -> status,
      "message" -> message,
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
  implicit val wEmpty = json.Json.writes[Empty]

  implicit val rVertexSetRequest = json.Json.reads[VertexSetRequest]
  implicit val wFEStatus = json.Json.writes[FEStatus]
  implicit val wUIValue = json.Json.writes[UIValue]
  implicit val wFEOperationParameterMeta = json.Json.writes[FEOperationParameterMeta]
  implicit val wFEOperationMeta = json.Json.writes[FEOperationMeta]
  implicit val wFEEdgeBundle = json.Json.writes[FEEdgeBundle]
  implicit val wFEVertexSet = json.Json.writes[FEVertexSet]

  implicit val rFEOperationSpec = json.Json.reads[FEOperationSpec]

  implicit val rGraphStatsRequest = json.Json.reads[GraphStatsRequest]
  implicit val wGraphStatsResponse = json.Json.writes[GraphStatsResponse]

  implicit val rSaveGraphAsCSVRequest = json.Json.reads[SaveGraphAsCSVRequest]
  implicit val wSaveGraphAsCSVResponse = json.Json.writes[SaveGraphAsCSVResponse]

  implicit val rSetClusterNumInstanceRequest = json.Json.reads[SetClusterNumInstanceRequest]
  implicit val wSparkClusterStatusResponse = json.Json.writes[SparkClusterStatusResponse]

  implicit val rSaveGraphRequest = json.Json.reads[SaveGraphRequest]

  implicit val rFEVertexAttributeFilter = json.Json.reads[FEVertexAttributeFilter]
  implicit val rVertexDiagramSpec = json.Json.reads[VertexDiagramSpec]
  implicit val wFEVertex = json.Json.writes[FEVertex]
  implicit val wVertexDiagramResponse = json.Json.writes[VertexDiagramResponse]

  implicit val rBundleSequenceStep = json.Json.reads[BundleSequenceStep]
  implicit val rEdgeDiagramSpec = json.Json.reads[EdgeDiagramSpec]
  implicit val wFEEdge = json.Json.writes[FEEdge]
  implicit val wEdgeDiagramResponse = json.Json.writes[EdgeDiagramResponse]

  implicit val rFEGraphRequest = json.Json.reads[FEGraphRequest]
  implicit val wFEGraphRespone = json.Json.writes[FEGraphRespone]

  implicit val rCreateProjectRequest = json.Json.reads[CreateProjectRequest]
  implicit val rProjectRequest = json.Json.reads[ProjectRequest]
  implicit val rProjectOperationRequest = json.Json.reads[ProjectOperationRequest]
  implicit val rProjectFilterRequest = json.Json.reads[ProjectFilterRequest]
  implicit val wOperationCategory = json.Json.writes[OperationCategory]
  implicit val wFEProject = json.Json.writes[FEProject]
  implicit val wSplash = json.Json.writes[Splash]

  // Methods called by the web framework
  //
  // Play! uses the routings in /conf/routes to execute actions

  val bigGraphController = new BigGraphController(BigGraphProductionEnvironment)
  def vertexSetGet = jsonGet(bigGraphController.vertexSet)
  def applyOpGet = jsonGet(bigGraphController.applyOp)
  def startingOperationsGet = jsonGet(bigGraphController.startingOperations)
  def startingVertexSetsGet = jsonGet(bigGraphController.startingVertexSets)
  def createProject = jsonPost(bigGraphController.createProject)
  def projectOp = jsonPost(bigGraphController.projectOp)
  def project = jsonGet(bigGraphController.project)
  def splash = jsonGet(bigGraphController.splash)
  def filterProject = jsonPost(bigGraphController.filterProject)

  val graphStatsController = new GraphStatsController(BigGraphProductionEnvironment)
  def graphStatsGet = jsonGet(graphStatsController.getStats)

  //val graphExportController = new GraphExportController(BigGraphProductionEnvironment)
  //def saveGraphAsCSV = jsonGet(graphExportController.saveGraphAsCSV)

  val sparkClusterController = new SparkClusterController(BigGraphProductionEnvironment)
  def getClusterStatus = jsonGet(sparkClusterController.getClusterStatus)
  def setClusterNumInstances = jsonGet(sparkClusterController.setClusterNumInstances)

  val persistenceController = new PersistenceController(BigGraphProductionEnvironment)
  def saveGraph = jsonGet(persistenceController.saveGraph)

  val drawingController = new GraphDrawingController(BigGraphProductionEnvironment)
  def vertexDiagram = jsonGet(drawingController.getVertexDiagram)
  def edgeDiagram = jsonGet(drawingController.getEdgeDiagram)
  def complexView = jsonGet(drawingController.getComplexView)
}
