// OperationLogger will log how long an operation took

package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import sun.misc.Signal
import sun.misc.SignalHandler

object PerformanceLoggerContext {
  private var phase = 0L

  def getPhase() = synchronized {
    s"phase${phase}"
  }

  Signal.handle(new Signal("USR2"), new SignalHandler {
    override def handle(signal: Signal): Unit = {
      this.synchronized {
        phase = phase + 1
      }
    }
  })
}

class PerformanceLoggerContext(msg: String) {
  protected val phase = PerformanceLoggerContext.getPhase()
  protected val startTime = System.currentTimeMillis()

  def write(): Unit = {
    val elapsed = System.currentTimeMillis() - startTime
    log.info(s"${phase} elapsed: ${elapsed} ${msg}")
  }
}
