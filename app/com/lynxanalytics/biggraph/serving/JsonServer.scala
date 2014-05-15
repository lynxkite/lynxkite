package com.lynxanalytics.biggraph.serving

import play.api.mvc
import play.api.libs.json
import play.api.libs.json._
import com.lynxanalytics.biggraph.BigGraphProductionEnviroment
import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.controllers._
import play.api.libs.functional.syntax.toContraFunctorOps
import play.api.libs.json.Json.toJsFieldJsValueWrapper

class JsonServer extends mvc.Controller {

  def jsonPost[I : json.Reads, O : json.Writes](action: I => O) =
    mvc.Action(parse.json) {
      request => request.body.validate[I].fold(
        errors => BadRequest(json.Json.obj(
          "status" -> "Error",
          "message" -> "Bad JSON",
          "details" -> json.JsError.toFlatJson(errors))),
        result => Ok(json.Json.toJson(action(result))))
    }

  def jsonGet[I : json.Reads, O : json.Writes](action: I => O, key: String) =
    mvc.Action { request =>
      request.getQueryString(key) match {
        case Some(s) => Json.parse(s).validate[I].fold(
            errors => BadRequest(json.Json.obj(
              "status" -> "Error",
              "message" -> "Bad JSON",
              "details" -> json.JsError.toFlatJson(errors))),
            result => Ok(json.Json.toJson(action(result))))
        case None => BadRequest(json.Json.obj(
              "status" -> "Error",
              "message" -> "Bad query string",
              "details" -> "You need to specify query parameter %s with a JSON value".format(key)))
      }
    }
}

object ProductionJsonServer extends JsonServer {
 /**
 * Implicit JSON inception
 *
 * json.Json.toJson needs one for every incepted case class,
 * they need to be ordered so that everything is declared before use.
 */

  implicit val rBigGraphRequest = json.Json.reads[controllers.BigGraphRequest]
  implicit val wGraphBasicData = json.Json.writes[controllers.GraphBasicData]
  implicit val wFEOperationParameter = json.Json.writes[controllers.FEOperationParameter]
  implicit val wFEOperation = json.Json.writes[controllers.FEOperation]
  implicit val wBigGraphResponse = json.Json.writes[controllers.BigGraphResponse]

  implicit val rGraphStatsRequest = json.Json.reads[controllers.GraphStatsRequest]
  implicit val wGraphStatsResponse = json.Json.writes[controllers.GraphStatsResponse]

 /**
 * Methods called by the web framework
 *
 * Play! uses the routings in /conf/routes to execute actions
 */

  val bigGraphController = new controllers.BigGraphController(BigGraphProductionEnviroment)
  def bigGraphGet = jsonGet(bigGraphController.getGraph, "q")

  val graphStatsController = new controllers.GraphStatsController(BigGraphProductionEnviroment)
  def graphStatsGet = jsonGet(graphStatsController.getStats, "q")
}
