// OperationLogger will log how long an operation took

package com.lynxanalytics.biggraph.graph_api

import java.util.UUID

import com.lynxanalytics.biggraph.graph_util.{ KiteInstanceInfo, LoggedEnvironment }
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.Timestamp
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

class PerformanceLoggerContext(marker: String, msg: => String) {
  protected val kiteVersion = KiteInstanceInfo.kiteVersion
  protected val sparkVersion = KiteInstanceInfo.sparkVersion
  protected val instanceName = KiteInstanceInfo.instanceName
  protected val phase = PerformanceLoggerContext.getPhase()
  protected val startTime = System.currentTimeMillis()

  def write(): Unit = {
    val elapsed = System.currentTimeMillis() - startTime
    log.info(s"${marker}\t${phase}\t${elapsed}\t${msg}")
  }
}
