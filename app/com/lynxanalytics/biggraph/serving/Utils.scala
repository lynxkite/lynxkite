package com.lynxanalytics.biggraph.serving

object Utils {
  private def rootCause(t: Throwable): Throwable = Option(t.getCause).map(rootCause(_)).getOrElse(t)

  private val assertionFailed = "^assertion failed: ".r

  def formatThrowable(t: Throwable): String = rootCause(t) match {
    // Trim "assertion failed: " from AssertionErrors.
    case e: AssertionError => assertionFailed.replaceFirstIn(e.getMessage, "")
    case e => e.toString
  }
}
