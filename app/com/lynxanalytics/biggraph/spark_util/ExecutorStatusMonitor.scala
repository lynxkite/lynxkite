// Periodically logs memory and storage usage stats of executors.
package com.lynxanalytics.biggraph.spark_util

import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import org.apache.spark
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

class ExecutorStatusMonitor(
    sc: spark.SparkContext) extends Thread("executor-status-monitor") {

  private val checkPeriod =
    LoggedEnvironment.envOrElse("KITE_EXECUTOR_STATUS_MONITOR_PERIOD_MILLIS", "60000").toLong

  private def logStorageStatus(): Unit = {
    val storageStatus = sc.getExecutorStorageStatus.toSeq
    storageStatus.foreach {
      x =>
        val diskUsed = x.diskUsed
        val memUsed = x.memUsed
        val offHeapUsed = x.offHeapUsed
        log.info(s"diskUsed: $diskUsed memUsed: $memUsed offHeapUsed: $offHeapUsed")
    }
  }

  private def logMemoryStatus(): Unit = {
    val memoryStatus = sc.getExecutorMemoryStatus.toSeq
    memoryStatus.foreach {
      case (e, m) =>
        log.info(s"executor: $e   max memory: ${m._1} remaining memory: ${m._2}")
    }
  }

  override def run(): Unit = {
    while (true) {
      Thread.sleep(checkPeriod)
      logStorageStatus()
      logMemoryStatus()
    }
  }

  setDaemon(true)
  start()
}
