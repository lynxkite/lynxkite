// Guaranteed-unique timestamp.
package com.lynxanalytics.biggraph.graph_util

object Timestamp {
  private var lastTime = 0L
  // Returns a millisecond timestamp as a string. It is guaranteed to be unique
  // for each call.
  override def toString: String = this.synchronized {
    val time = scala.compat.Platform.currentTime
    val fixed = if (lastTime < time) time else lastTime + 1
    lastTime = fixed
    return "%013d".format(fixed)
  }
}
