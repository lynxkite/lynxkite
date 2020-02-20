// OperationLogger will log how long an operation took

package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.graph_util.KiteInstanceInfo
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

import scala.concurrent.ExecutionContextExecutorService

object OperationLogger {
  private var counter = 0
  def next() = synchronized {
    counter = counter + 1
    counter
  }
}

class OperationLogger(
    instance: MetaGraphOperationInstance,
    domainName: String,
    implicit val ec: ExecutionContextExecutorService) {
  private val marker = "OPERATION_LOGGER_MARKER"
  private val kiteVersion = KiteInstanceInfo.kiteVersion
  private val sparkVersion = KiteInstanceInfo.sparkVersion
  private val instanceName = KiteInstanceInfo.instanceName
  private val startTime = System.currentTimeMillis()
  def register(computation: => SafeFuture[Unit]): SafeFuture[Unit] = {
    computation.andThen {
      case _ => write()
    }
    computation
  }
  def write(): Unit = {
    val elapsed = System.currentTimeMillis() - startTime
    log.info(s"$marker ${OperationLogger.next()} $startTime $elapsed $domainName $instance")
  }
}
