// Same as controllers.Assets, except with a custom 404 handler.
package com.lynxanalytics.lynxkite.serving

import play.api.mvc._

class Assets @javax.inject.Inject() (
    implicit
    ec: concurrent.ExecutionContext,
    assets: controllers.Assets,
    val controllerComponents: ControllerComponents)
    extends BaseController {
  def at(path: String, file: String): Action[AnyContent] = {
    val action = assets.at(path, file)
    Action.async { request =>
      action(request) map { result =>
        if (result.header.status == NOT_FOUND) notFound(request)
        else result
      }
    }
  }

  def notFound(request: Request[AnyContent]) = {
    val static = new java.io.File("./static").getCanonicalPath
    val requested = new java.io.File("./static/" + request.path).getCanonicalPath
    val file = new java.io.File(requested)
    // To be safe against "../../../etc/passwd" attacks we check that the normalized (canonical)
    // path is still inside the "static" directory.
    if (requested.startsWith(static) && file.exists) {
      // Send file from ./static if it exists.
      Ok.sendFile(file)
    } else {
      // Redirect e.g. /project/x to /#/project/x.
      // Absolutely bogus URLs get redirected then to the main page by Angular.
      Redirect("/#" + request.path)
    }
  }
}
