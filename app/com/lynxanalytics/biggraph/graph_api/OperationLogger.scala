// OperationLogger will log how long an operation took

package com.lynxanalytics.biggraph.graph_api

import java.util.UUID

import com.lynxanalytics.biggraph.graph_util.{ KiteInstanceInfo, LoggedEnvironment }
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.Timestamp
import sun.misc.Signal
import sun.misc.SignalHandler

object PerfLoggerPhase {
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

abstract class PerformanceLogger {
  def write(): Unit
  protected def elapsed() = System.currentTimeMillis() - startTime
  protected val marker: String
  protected val kiteVersion = KiteInstanceInfo.kiteVersion
  protected val sparkVersion = KiteInstanceInfo.sparkVersion
  protected val instanceName = KiteInstanceInfo.instanceName
  protected val phase = PerfLoggerPhase.getPhase()
  protected val startTime = System.currentTimeMillis()
}

class OperationLogger(
    instance: MetaGraphOperationInstance,
    domainName: String) extends PerformanceLogger {
  protected val marker: String = "OPERATION_LOGGER_MARKER"

  def write(): Unit = {
    log.info(s"${marker}\t${phase}\t${elapsed()}\t${domainName}\t${instance}")
  }
}

class RelocationLogger(
    guid: UUID,
    src: Domain,
    dst: Domain) extends PerformanceLogger {
  protected val marker = "RELOCATION_LOGGER_MARKER"
  def write(): Unit = {
    log.info(s"${marker}\t${phase}\t${elapsed()}\t${guid}\t${src}->${dst}")
  }
}
