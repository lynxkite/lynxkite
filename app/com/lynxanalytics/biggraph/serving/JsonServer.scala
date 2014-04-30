package com.lynxanalytics.biggraph.serving

import play.api.mvc
import play.api.libs.json
import play.api.libs.json._
import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.controllers._
import play.api.libs.functional.syntax.toContraFunctorOps
import play.api.libs.json.Json.toJsFieldJsValueWrapper

object JsonServer extends mvc.Controller {

/**
 * Implicit JSON inception
 *
 * json.Json.toJson needs one for every incepted case class,
 * they need to be ordered so that everything is declared before use.
 */

  implicit val rTestPost = json.Json.reads[controllers.TestPostRequest]
  implicit val wTestPost = json.Json.writes[controllers.TestPostResponse]

/**
 * Actions called by the web framework
 *
 * Play! uses the routings in /conf/routes to execute actions
 */

  def jsonPost[I : json.Reads, O : json.Writes](action: I => O) = mvc.Action(parse.json) {
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
        case Some(s) =>
          Json.parse(s).validate[I].fold(
            errors => BadRequest(json.Json.obj(
              "status" -> "Error",
              "message" -> "Bad JSON",
              "details" -> json.JsError.toFlatJson(errors))),
            result => Ok(json.Json.toJson(action(result))))
        case None => Ok(json.JsNull) // whatever we want to respond
      }
    }

  def testPost = jsonPost(controllers.TestController.process)
  def testGet = jsonGet(controllers.TestController.process, "q")

}
