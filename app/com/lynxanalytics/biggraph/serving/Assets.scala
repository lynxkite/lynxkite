// Same as controllers.Assets, except with a custom 404 handler.
package com.lynxanalytics.biggraph.serving

import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.mvc._

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
}
