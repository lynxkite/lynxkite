// Custom global failure handlers for Play Framework.
import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import play.filters.gzip.GzipFilter
import play.filters.headers.SecurityHeadersFilter
import play.twirl.api.HtmlFormat.escape

import com.lynxanalytics.biggraph.serving

object Global extends WithFilters(new GzipFilter(), SecurityHeadersFilter()) with GlobalSettings {
  override def onBadRequest(request: RequestHeader, error: String) = {
    concurrent.Future.successful(BadRequest(escape(error)))
  }

  override def onError(request: RequestHeader, throwable: Throwable) = {
    concurrent.Future.successful(InternalServerError(escape(
      serving.Utils.formatThrowable(throwable))))
  }

  override def onHandlerNotFound(request: RequestHeader) = {
    concurrent.Future.successful(NotFound(escape(request.toString)))
  }

  override def onStart(app: Application) = {
    serving.ProductionJsonServer
    scala.util.Properties.envOrNone("KITE_READY_PIPE").foreach(pipeName =>
      org.apache.commons.io.FileUtils.writeStringToFile(
        new java.io.File(pipeName),
        "ready\n",
        "utf8"))
    println("LynxKite is running.")
  }
}
