// OperationLogger will log how long an operation took

package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.graph_util.{ KiteInstanceInfo, LoggedEnvironment }
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.Timestamp
import sun.misc.Signal
import sun.misc.SignalHandler

object OperationLogger {
  private var unique = 0L
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

class OperationLogger(
    instance: MetaGraphOperationInstance,
    domainName: String) {
  private val marker = "OPERATION_LOGGER_MARKER"
  private val kiteVersion = KiteInstanceInfo.kiteVersion
  private val sparkVersion = KiteInstanceInfo.sparkVersion
  private val instanceName = KiteInstanceInfo.instanceName
  private val phase = OperationLogger.getPhase()
  private val startTime = System.currentTimeMillis()
  def write(): Unit = {
    val elapsed = System.currentTimeMillis() - startTime
    log.info(s"${marker}\t${phase}\t${elapsed}\t${domainName}\t${instance}")
  }
}
