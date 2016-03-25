// A special log rotate policy that rolls log files in one day,
// but also allows the user to enforce a log rotation.
// This will only work if the logger is configured to use
// at least sec precision: "yyyyMMdd_HHmmss"

package com.lynxanalytics.biggraph.graph_util

import ch.qos.logback.core.rolling.TimeBasedRollingPolicy
import ch.qos.logback.core.rolling.DefaultTimeBasedFileNamingAndTriggeringPolicy

object DayBasedForcibleRollingPolicy {
  var forceRotation = false
}

class DayBasedForcibleTriggeringPolicy[E] extends DefaultTimeBasedFileNamingAndTriggeringPolicy[E] {
  val oneDayInSeconds = 24L * 60 * 60
  val oneDayInMilliseconds = 1000 * oneDayInSeconds

  // Unfortunately, super.isTriggeringEvent manipulates state :(
  // Maybe it would be better to re-write it from scratch, but
  // that would be very painful.
  override def isTriggeringEvent(activeFile: java.io.File, event: E): Boolean = {
    if (DayBasedForcibleRollingPolicy.forceRotation) {
      nextCheck = -1L
      DayBasedForcibleRollingPolicy.forceRotation = false
    }
    val triggered = super.isTriggeringEvent(activeFile, event)
    assert(nextCheck != -1L, "DefaultTimeBasedFileNamingAndTriggeringPolicy didn't call computeNextCheck!!!")
    triggered
  }

  override def computeNextCheck(): Unit = {
    nextCheck = getCurrentTime() + oneDayInMilliseconds
  }
}

class DayBasedForcibleRollingPolicy[E] extends TimeBasedRollingPolicy[E] {
  setTimeBasedFileNamingAndTriggeringPolicy(new DayBasedForcibleTriggeringPolicy[E])
}
