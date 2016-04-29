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
    val allStorageStatuses = sc.getExecutorStorageStatus.toSeq

    for (storageStatus <- allStorageStatuses) {
      val executor = storageStatus.blockManagerId.host + ":" + storageStatus.blockManagerId.port
      val msg =
        s"StorageStatus: executor: $executor" +
          s"  diskUsed: ${storageStatus.diskUsed}" +
          s"  memUsed: ${storageStatus.memUsed}" +
          s"  offHeapUsed: ${storageStatus.offHeapUsed}"
      log.info(msg)
    }
  }

  private def logMemoryStatus(): Unit = {
    for ((executor, (maxMemory, remainingMemory)) <- sc.getExecutorMemoryStatus.toSeq) {
      val msg = s"Memory status: executor: $executor  max memory: $maxMemory  remaining memory: $remainingMemory"
      log.info(msg)
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
}
