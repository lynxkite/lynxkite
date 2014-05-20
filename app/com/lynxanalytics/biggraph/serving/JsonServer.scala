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
  def jsonPost[I : json.Reads, O : json.Writes](action: I => O) = {
    bigGraphLogger.info("JSON POST event received")
    mvc.Action(parse.json) {
      request => request.body.validate[I].fold(
        errors => JsonBadRequest("Error", "Bad JSON", errors),
        result => Ok(json.Json.toJson(action(result))))
    }
  }

  def jsonGet[I : json.Reads, O : json.Writes](action: I => O, key: String) = {
    mvc.Action { request =>
      bigGraphLogger.info("JSON GET event received")
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

case class EmptyRequest(
  fake: Int = 0)  // Needs fake field as JSON inception doesn't work otherwise.

object ProductionJsonServer extends JsonServer {
  /**
   * Implicit JSON inception
   *
   * json.Json.toJson needs one for every incepted case class,
   * they need to be ordered so that everything is declared before use.
   */

  implicit val rEmptyRequest = json.Json.reads[EmptyRequest]

  implicit val rBigGraphRequest = json.Json.reads[controllers.BigGraphRequest]
  implicit val wGraphBasicData = json.Json.writes[controllers.GraphBasicData]
  implicit val wFEOperationParameterMeta = json.Json.writes[controllers.FEOperationParameterMeta]
  implicit val wFEOperationMeta = json.Json.writes[controllers.FEOperationMeta]
  implicit val wBigGraphResponse = json.Json.writes[controllers.BigGraphResponse]

  implicit val rFEOperationSpec = json.Json.reads[controllers.FEOperationSpec]
  implicit val rDeriveBigGraphRequest = json.Json.reads[controllers.DeriveBigGraphRequest]

  implicit val rGraphStatsRequest = json.Json.reads[controllers.GraphStatsRequest]
  implicit val wGraphStatsResponse = json.Json.writes[controllers.GraphStatsResponse]

  // Methods called by the web framework
  //
  // Play! uses the routings in /conf/routes to execute actions

  val bigGraphController = new BigGraphController(BigGraphProductionEnviroment)
  def bigGraphGet = jsonGet(bigGraphController.getGraph, "q")
  def deriveBigGraphGet = jsonGet(bigGraphController.deriveGraph, "q")
  def startingOperationsGet = jsonGet(bigGraphController.startingOperations, "q")

  val graphStatsController = new GraphStatsController(BigGraphProductionEnviroment)
  def graphStatsGet = jsonGet(graphStatsController.getStats, "q")
}
