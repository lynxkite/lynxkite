// Custom global failure handlers for Play Framework.

import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import play.filters.gzip.GzipFilter
import play.filters.headers.SecurityHeadersFilter
import play.twirl.api.HtmlFormat.escape
import play.api.libs.concurrent.Execution.Implicits._
import scala.concurrent.Await
import scala.concurrent.duration._

import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment

object Global extends WithFilters(new GzipFilter(), SecurityHeadersFilter()) with GlobalSettings {
  override def onBadRequest(request: RequestHeader, error: String) = {
    concurrent.Future.successful(BadRequest(error))
  }

  override def onError(request: RequestHeader, throwable: Throwable) = {
    concurrent.Future.successful(InternalServerError(serving.Utils.formatThrowable(throwable)))
  }

  override def onHandlerNotFound(request: RequestHeader) = {
    concurrent.Future.successful(NotFound(request.toString))
  }

  def notifyStarterScript(msg: String): Unit = {
    val notifier =
      scala.concurrent.Future[Unit] {
        LoggedEnvironment.envOrNone("KITE_READY_PIPE").foreach(pipeName =>
          org.apache.commons.io.FileUtils.writeStringToFile(
            new java.io.File(pipeName),
            msg + "\n",
            "utf8"))
      }
    try {
      Await.result(notifier, 5.seconds)
    } catch {
      case _: Throwable => log.info("Timeout - starter script could have been killed")
    }
  }

  override def onStart(app: Application) = {
    try {
      serving.ProductionJsonServer
    } catch {
      case t: ExceptionInInitializerError =>
        val exceptionMessage = Option(t.getCause).map(_.toString.replace('\n', ' ')).getOrElse(t.toString)
        notifyStarterScript("failed: " + exceptionMessage)
        throw t
      case t: Throwable =>
        val exceptionMessage = Option(t.getMessage).map(_.replace('\n', ' ')).getOrElse(t.toString)
        notifyStarterScript("failed: " + exceptionMessage)
        throw t
    }
    notifyStarterScript("ready")
    println("LynxKite is running.")
  }
}
