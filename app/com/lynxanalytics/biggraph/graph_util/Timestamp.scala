// Guaranteed-unique timestamp.
package com.lynxanalytics.biggraph.graph_util

object Timestamp {
  private var lastTime = 0L
  // Returns a millisecond timestamp as a string. It is guaranteed to be unique
  // for each call.
  def toLong: Long = this.synchronized {
    val time = System.currentTimeMillis
    val fixed = if (lastTime < time) time else lastTime + 1
    lastTime = fixed
    fixed
  }
  override def toString: String = {
    return "%013d".format(toLong)
  }
}
