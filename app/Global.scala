// Custom global failure handlers for Play Framework.

import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import play.filters.gzip.GzipFilter
import play.filters.headers.SecurityHeadersFilter
import play.twirl.api.Html
import play.twirl.api.HtmlFormat.escape
import play.api.libs.concurrent.Execution.Implicits._
import scala.concurrent.Await
import scala.concurrent.duration._

import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment

object Global extends WithFilters(new GzipFilter(), SecurityHeadersFilter()) with GlobalSettings {
  // Need to escape errors for non-XHR requests to avoid XSS.
  // For XHR requests an unescaped error message is easier to handle.
  private def escapeIfNeeded(error: String, headers: Headers): Html = {
    if (headers.get("X-Requested-With") == Some("XMLHttpRequest")) Html(error)
    else escape(error)
  }
  override def onBadRequest(request: RequestHeader, error: String) = {
    concurrent.Future.successful(BadRequest(escapeIfNeeded(error, request.headers)))
  }

  override def onError(request: RequestHeader, throwable: Throwable) = {
    concurrent.Future.successful(InternalServerError(
      escapeIfNeeded(serving.Utils.formatThrowable(throwable), request.headers)))
  }

  override def onHandlerNotFound(request: RequestHeader) = {
    concurrent.Future.successful(NotFound(escapeIfNeeded(request.toString, request.headers)))
  }

  def startedDirectly(): Unit = {
    serving.ProductionJsonServer
    println("LynxKite is running.")
  }

  def notifyStarterScript(msg: String, success: Boolean): Unit = {
    val notifier =
      scala.concurrent.Future[Unit] {
        val pipeName = LoggedEnvironment.envOrNone("KITE_READY_PIPE").get
        org.apache.commons.io.FileUtils.writeStringToFile(
          new java.io.File(pipeName), msg + "\n", "utf8")
      }
    try {
      Await.result(notifier, 5.seconds)
      if (!success) System.exit(1)
    } catch {
      case _: Throwable =>
        log.info("Timeout - starter script could have been killed")
        if (!success) System.exit(1)
    }
  }

  def startedByStarterScriptInTheBackground(): Unit = {
    try {
      serving.ProductionJsonServer
      notifyStarterScript("ready", success = true)
    } catch {
      case t: ExceptionInInitializerError =>
        val exceptionMessage = Option(t.getCause).map(_.toString.replace('\n', ' ')).getOrElse(t.toString)
        notifyStarterScript("failed: " + exceptionMessage, success = false)
      case t: Throwable =>
        val exceptionMessage = Option(t.getMessage).map(_.replace('\n', ' ')).getOrElse(t.toString)
        notifyStarterScript("failed: " + exceptionMessage, success = false)
    }
  }

  override def onStart(app: Application) = {
    if (LoggedEnvironment.envOrNone("KITE_READY_PIPE").isEmpty) {
      startedDirectly()
    } else {
      startedByStarterScriptInTheBackground()
    }
  }
}
