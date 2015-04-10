// Custom global failure handlers for Play Framework.
import play.api._
import play.api.mvc._
import play.api.mvc.Results._

object Global extends GlobalSettings {
  override def onBadRequest(request: RequestHeader, error: String) = {
    concurrent.Future.successful(BadRequest(error))
  }
  override def onError(request: RequestHeader, throwable: Throwable) = {
    concurrent.Future.successful(InternalServerError(format(throwable)))
  }
  override def onHandlerNotFound(request: RequestHeader) = {
    concurrent.Future.successful(NotFound(request.toString))
  }
  private def rootCause(t: Throwable): Throwable = Option(t.getCause).map(rootCause(_)).getOrElse(t)
  private val assertionFailed = "^assertion failed: ".r
  private def format(t: Throwable): String = rootCause(t) match {
    // Trim "assertion failed: " from AssertionErrors.
    case e: AssertionError => assertionFailed.replaceFirstIn(e.getMessage, "")
    case e => e.toString
  }
}
