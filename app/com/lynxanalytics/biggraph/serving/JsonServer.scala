package com.lynxanalytics.biggraph.serving

import play.api.mvc
import play.api.libs.json
import play.api.libs.json._
import com.lynxanalytics.biggraph._
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
          errors => JsonBadRequest("Error", "Bad JSON", errors),
          result => Ok(json.Json.toJson(action(result))))
    }
  }

  def jsonGet[I: json.Reads, O: json.Writes](action: I => O, key: String = "q") = {
    mvc.Action { request =>
      bigGraphLogger.info("JSON GET event received, function: %s, query key: %s"
        .format(action.getClass.toString(), key))
      request.getQueryString(key) match {
        case Some(s) => Json.parse(s).validate[I].fold(
          errors => JsonBadRequest("Error", "Bad JSON", errors),
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

  implicit val rBigGraphRequest = json.Json.reads[BigGraphRequest]
  implicit val wUIValue = json.Json.writes[UIValue]
  implicit val wFEOperationParameterMeta = json.Json.writes[FEOperationParameterMeta]
  implicit val wFEOperationMeta = json.Json.writes[FEOperationMeta]
  implicit val wFEEdgeBundle = json.Json.writes[FEEdgeBundle]
  implicit val wFEVertexSet = json.Json.writes[FEVertexSet]

  implicit val rFEOperationSpec = json.Json.reads[FEOperationSpec]
  implicit val rDeriveBigGraphRequest = json.Json.reads[DeriveBigGraphRequest]

  implicit val rGraphStatsRequest = json.Json.reads[GraphStatsRequest]
  implicit val wGraphStatsResponse = json.Json.writes[GraphStatsResponse]

  implicit val rSaveGraphAsCSVRequest = json.Json.reads[SaveGraphAsCSVRequest]
  implicit val wSaveGraphAsCSVResponse = json.Json.writes[SaveGraphAsCSVResponse]

  implicit val rSetClusterNumInstanceRequest = json.Json.reads[SetClusterNumInstanceRequest]
  implicit val wSparkClusterStatusResponse = json.Json.writes[SparkClusterStatusResponse]

  implicit val rSaveGraphRequest = json.Json.reads[SaveGraphRequest]

  // Methods called by the web framework
  //
  // Play! uses the routings in /conf/routes to execute actions

  val bigGraphController = new BigGraphController(BigGraphProductionEnvironment)
  def bigGraphGet = jsonGet(bigGraphController.getGraph)
  def deriveBigGraphGet = jsonGet(bigGraphController.deriveGraph)
  def startingOperationsGet = jsonGet(bigGraphController.startingOperations)

  val graphStatsController = new GraphStatsController(BigGraphProductionEnvironment)
  def graphStatsGet = jsonGet(graphStatsController.getStats)

  val graphExportController = new GraphExportController(BigGraphProductionEnvironment)
  def saveGraphAsCSV = jsonGet(graphExportController.saveGraphAsCSV)

  val sparkClusterController = new SparkClusterController(BigGraphProductionEnvironment)
  def getClusterStatus = jsonGet(sparkClusterController.getClusterStatus)
  def setClusterNumInstances = jsonGet(sparkClusterController.setClusterNumInstances)

  val persistenceController = new PersistenceController(BigGraphProductionEnvironment)
  def saveGraph = jsonGet(persistenceController.saveGraph)
}
