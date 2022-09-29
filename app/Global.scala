// Custom global failure handlers for Play Framework.

import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import play.twirl.api.Html
import play.twirl.api.HtmlFormat.escape
import scala.concurrent.Future
import scala.concurrent.duration._

import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.{bigGraphLogger => log}
import com.lynxanalytics.biggraph.graph_util.Environment

@javax.inject.Singleton
class ErrorHandler extends http.HttpErrorHandler {
  // Need to escape errors for non-XHR requests to avoid XSS.
  // For XHR requests an unescaped error message is easier to handle.
  private def escapeIfNeeded(error: String, headers: Headers): Html = {
    if (headers.get("X-Requested-With") == Some("XMLHttpRequest")) Html(error)
    else escape(error)
  }
  def onClientError(request: RequestHeader, statusCode: Int, error: String): Future[Result] = {
    if (statusCode == play.api.http.Status.NOT_FOUND) {
      Future.successful(NotFound(escapeIfNeeded(request.toString, request.headers)))
    } else {
      Future.successful(BadRequest(escapeIfNeeded(error, request.headers)))
    }
  }

  @annotation.tailrec
  private def getResult(throwable: Throwable): Option[Result] = {
    throwable match {
      case serving.ResultException(result) => Some(result)
      case _ if throwable.getCause != null => getResult(throwable.getCause)
      case _ => None
    }
  }

  def onServerError(request: RequestHeader, throwable: Throwable): Future[Result] = {
    log.error("server error", throwable)
    Future.successful {
      getResult(throwable) match {
        case Some(status) => status
        case None => InternalServerError(
            escapeIfNeeded(serving.Utils.formatThrowable(throwable), request.headers))
      }
    }
  }
}
