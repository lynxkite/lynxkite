// Guaranteed-unique timestamp.
package com.lynxanalytics.lynxkite.graph_util

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

  // A guaranteed unique human-readable timestamp that can be used as a filename.
  private var lastHuman = ""
  private var humanCounter = 0
  private val humanFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def human: String = synchronized {
    val base = humanFormat.format(new java.util.Date)
    if (base == lastHuman) {
      humanCounter += 1
      s"$base ($humanCounter)"
    } else {
      lastHuman = base
      humanCounter = 0
      base
    }
  }
}
