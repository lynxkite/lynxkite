// Same as controllers.Assets, except with a custom 404 handler.
package com.lynxanalytics.biggraph.serving

import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.mvc._
import play.Play

import java.text.SimpleDateFormat
import java.util.Calendar
import java.io.File

object Assets extends Controller {
  def at(path: String, file: String): Action[AnyContent] = {
    val action = controllers.Assets.at(path, file)
    Action.async { request =>
      action(request) map { result =>
        if (result.header.status == NOT_FOUND) notFound(request)
        else result
      }
    }
  }

  def notFound(request: Request[AnyContent]) = {
    // Redirect e.g. /project/x to /#/project/x.
    // Absolutely bogus URLs get redirected then to the main page by Angular.
    Redirect("/#" + request.path)
  }

  private val format = new SimpleDateFormat("yyyyMMdd")

  def appLog(): Action[AnyContent] = Action { request =>
    val dateString = format.format(Calendar.getInstance().getTime())
    val logFile = new File(Play.application.getFile("logs"),
      s"application-$dateString.log")

    if (logFile.exists) {
      Ok.sendFile(logFile, inline = true)
    } else {
      // This should never happen unless the log is missing.
      NotFound
    }
  }
}
