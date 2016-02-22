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

  def notifyStarterScript(msg: String) = {
    scala.util.Properties.envOrNone("KITE_READY_PIPE").foreach(pipeName =>
      org.apache.commons.io.FileUtils.writeStringToFile(
        new java.io.File(pipeName),
        msg + "\n",
        "utf8"))
  }

  override def onStart(app: Application) = {
    try {
      serving.ProductionJsonServer
    } catch {
      case t: Throwable =>
        val exceptionMessage = t.getMessage.replace('\n', ' ')
        notifyStarterScript("failed: " + exceptionMessage)
        throw t
    }
    notifyStarterScript("ready")
    println("LynxKite is running.")
  }
}
