// A special log rotate policy that rolls log files in one day,
// but also allows the user to enforce a log rotation.
// This will only work if the logger is configured to use
// at least sec precision: "yyyyMMdd_HHmmss"

package com.lynxanalytics.biggraph.graph_util

import ch.qos.logback.core.rolling.TimeBasedRollingPolicy
import ch.qos.logback.core.rolling.DefaultTimeBasedFileNamingAndTriggeringPolicy

object ForcibleAndDayBasedRollingPolicy {
  var forceRotation = false
}

class ForcibleAndDayBasedTriggeringPolicy[E] extends DefaultTimeBasedFileNamingAndTriggeringPolicy[E] {
  val secondsInOneDay = 24L * 60 * 60
  val oneDayInMilliseconds = 1000 * secondsInOneDay

  override def isTriggeringEvent(activeFile: java.io.File, event: E): Boolean = {
    if (ForcibleAndDayBasedRollingPolicy.forceRotation) {
      nextCheck = -1L
      ForcibleAndDayBasedRollingPolicy.forceRotation = false
    }
    val triggered = super.isTriggeringEvent(activeFile, event)
    assert(nextCheck != -1L, "DefaultTimeBasedFileNamingAndTriggeringPolicy didn't call computeNextCheck!!!")
    triggered
  }
  override def computeNextCheck(): Unit = {
    nextCheck = getCurrentTime() + oneDayInMilliseconds
  }
}

class ForcibleAndDayBasedRollingPolicy[E] extends TimeBasedRollingPolicy[E] {
  setTimeBasedFileNamingAndTriggeringPolicy(new ForcibleAndDayBasedTriggeringPolicy[E])
}
