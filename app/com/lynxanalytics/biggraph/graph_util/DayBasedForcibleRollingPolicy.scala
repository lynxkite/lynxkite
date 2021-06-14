// A special log rotate policy that rolls log files in one day,
// but also allows the user to enforce a log rotation.
// This will only work if the logger is configured to use
// at least sec precision: "yyyyMMdd_HHmmss"

package com.lynxanalytics.biggraph.graph_util

import ch.qos.logback.core.rolling.TimeBasedRollingPolicy
import ch.qos.logback.core.rolling.DefaultTimeBasedFileNamingAndTriggeringPolicy

import com.lynxanalytics.biggraph.{bigGraphLogger => log}

object DayBasedForcibleRollingPolicy {
  private var forceRotation = false

  def isForced = forceRotation
  def reset() = forceRotation = false

  // The main API for the logger: Calling DayBasedForcibleRollingPolicy.triggerRotation() will
  // enforce a log rotation.
  def triggerRotation() = synchronized {
    forceRotation = true
    log.info("Triggering rollover")
  }
}

class DayBasedForcibleTriggeringPolicy[E] extends DefaultTimeBasedFileNamingAndTriggeringPolicy[E] {
  val oneDayInSeconds = 24L * 60 * 60
  val oneDayInMilliseconds = 1000 * oneDayInSeconds

  // We're utilizing the following two members of our base class:
  //
  // nextCheck: timestamp (a' la: System.currentTimeMillis)
  //   super.isTriggeringEvent returns true iff the current timestamp exceeds this.
  //
  // computeNextCheck(): the method used by super.isTriggeringEvent to compute the next value of nextCheck
  //    when the current timestamp exceeds nextCheck.
  //

  // isTriggeringEvent returns true if the log should be rotated and false otherwise.
  // As a side effect, it can also manipulate nextCheck.
  override def isTriggeringEvent(activeFile: java.io.File, event: E): Boolean = {
    if (DayBasedForcibleRollingPolicy.isForced) {
      nextCheck = -1L
      DayBasedForcibleRollingPolicy.reset()
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
