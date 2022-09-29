package com.lynxanalytics.lynxkite.serving

object Utils {
  private def causes(t: Throwable): List[Throwable] = {
    t :: (Option(t.getCause) match {
      case None => Nil
      case Some(t) => causes(t)
    })
  }
  private val assertionFailed = "^assertion failed: ".r
  private val afterFirstLine = "(?s)\n.*".r

  def formatThrowable(t: Throwable): String = {
    val cs = causes(t)
    val assertion = cs.collectFirst { case c: AssertionError => c }
    assertion.map { t =>
      // If we have an assertion, that should explain everything on its own.
      assertionFailed.replaceFirstIn(t.getMessage, "")
    }.getOrElse {
      // Otherwise give a condensed version of the stack trace.
      cs.flatMap { t =>
        Option(t.getMessage).map { msg => afterFirstLine.replaceFirstIn(msg, "") }
      }.mkString("\ncaused by:\n")
    }
  }
}
