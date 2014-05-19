package com.lynxanalytics.biggraph.serving

import play.api.mvc
import play.api.libs.json
import play.api.libs.json._
import com.lynxanalytics.biggraph.BigGraphProductionEnviroment
import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.controllers._
import play.api.libs.functional.syntax.toContraFunctorOps
import play.api.libs.json.Json.toJsFieldJsValueWrapper
import org.slf4j.LoggerFactory

class JsonServer extends mvc.Controller {
  def jsonPost[I : json.Reads, O : json.Writes](action: I => O) = {
    JsonServer.jsonLogger.info("JSON POST event received")
    mvc.Action(parse.json) {
      request => request.body.validate[I].fold(
        errors => JsonBadRequest("Error", "Invalid JSON key", errors),
        result => Ok(json.Json.toJson(action(result))))
    }
  }

  def jsonGet[I : json.Reads, O : json.Writes](action: I => O, key: String) = {
    mvc.Action { request =>
      JsonServer.jsonLogger.info("JSON GET event received")
      request.getQueryString(key) match {
        case Some(s) => Json.parse(s).validate[I].fold(
            errors => JsonBadRequest("Error", "Invalid JSON key", errors),
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
    JsonServer.jsonLogger.error("Bad request: " + message)
    BadRequest(json.Json.obj(
      "status" -> status,
      "message" -> message,
      "details" -> json.JsError.toFlatJson(details)))
  }
}

object JsonServer {
  val jsonLogger = LoggerFactory.getLogger("JSON logger")
}

object ProductionJsonServer extends JsonServer {
 /**
 * Implicit JSON inception
 *
 * json.Json.toJson needs one for every incepted case class,
 * they need to be ordered so that everything is declared before use.
 */

  implicit val rBigGraph = json.Json.reads[controllers.BigGraphRequest]
  implicit val wGraphMeta = json.Json.writes[controllers.GraphBasicData]
  implicit val wBigGraph = json.Json.writes[controllers.BigGraphResponse]

  implicit val rGraphStats = json.Json.reads[controllers.GraphStatsRequest]
  implicit val wGraphStats = json.Json.writes[controllers.GraphStatsResponse]

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
