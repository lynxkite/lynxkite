// Custom global failure handlers for Play Framework.
import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import play.filters.gzip.GzipFilter
import play.filters.headers.SecurityHeadersFilter
import play.twirl.api.HtmlFormat.escape

object Global extends WithFilters(new GzipFilter(), SecurityHeadersFilter()) with GlobalSettings {
  override def onBadRequest(request: RequestHeader, error: String) = {
    concurrent.Future.successful(BadRequest(escape(error)))
  }

  override def onError(request: RequestHeader, throwable: Throwable) = {
    concurrent.Future.successful(InternalServerError(escape(format(throwable))))
  }

  override def onHandlerNotFound(request: RequestHeader) = {
    concurrent.Future.successful(NotFound(escape(request.toString)))
  }

  private def rootCause(t: Throwable): Throwable = Option(t.getCause).map(rootCause(_)).getOrElse(t)

  private val assertionFailed = "^assertion failed: ".r

  private def format(t: Throwable): String = rootCause(t) match {
    // Trim "assertion failed: " from AssertionErrors.
    case e: AssertionError => assertionFailed.replaceFirstIn(e.getMessage, "")
    case e => e.toString
  }
}
