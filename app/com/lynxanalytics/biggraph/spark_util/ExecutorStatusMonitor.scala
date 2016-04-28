// Periodically logs memory and storage usage stats of executors.
package com.lynxanalytics.biggraph.spark_util

import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import org.apache.spark
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import org.apache.spark.storage.{ StorageStatus, RDDBlockId }

class ExecutorStatusMonitor(
    sc: spark.SparkContext) extends Thread("executor-status-monitor") {

  private val checkPeriod =
    LoggedEnvironment.envOrElse("KITE_EXECUTOR_STATUS_MONITOR_PERIOD_MILLIS", "60000").toLong

  def rddGetTotal(storageStatus: StorageStatus, memFun: (StorageStatus, Int) => Long): Long = {
    val rddBlocks =
      storageStatus.rddBlocks.keys.toSeq.filter(_.isInstanceOf[RDDBlockId]).map(_.asInstanceOf[RDDBlockId])
    rddBlocks.map(_.rddId).distinct.map {
      id => memFun(storageStatus, id)
    }.sum
  }

  private def logStorageStatus(): Unit = {
    val allStorageStatuses = sc.getExecutorStorageStatus.toSeq
    def rddDisk(storageStatus: StorageStatus, id: Int) = storageStatus.diskUsedByRdd(id)
    def rddMem(storageStatus: StorageStatus, id: Int) = storageStatus.memUsedByRdd(id)
    def rddOffHeap(storageStatus: StorageStatus, id: Int) = storageStatus.offHeapUsedByRdd(id)

    allStorageStatuses.foreach {
      x =>
        // In the source: diskUsed = _nonRddStorageInfo._2 + _rddBlocks.keys.toSeq.map(diskUsedByRdd).sum
        val diskUsed = x.diskUsed
        val rddDiskUsed = rddGetTotal(x, rddDisk)

        val memUsed = x.memUsed
        val rddMemUsed = rddGetTotal(x, rddMem)

        val offHeapUsed = x.offHeapUsed
        val rddOffHeapUsed = rddGetTotal(x, rddOffHeap)

        val rddBlocksSize = x.rddBlocks.size

        val executor = x.blockManagerId.host + ":" + x.blockManagerId.port
        val msg =
          s"StorageStatus: executor: $executor" +
            s"  (rdd+other)" +
            s"  diskUsed: $rddDiskUsed+$diskUsed" +
            s"  memUsed: $rddMemUsed+$memUsed" +
            s"  offHeapUsed: $rddOffHeapUsed+$offHeapUsed"
        log.info(msg)
    }
  }

  private def logMemoryStatus(): Unit = {
    val memoryStatus = sc.getExecutorMemoryStatus.toSeq
    memoryStatus.foreach {
      case (e, m) =>
        val msg = s"Memory status: executor: $e  max memory: ${m._1}  remaining memory: ${m._2}"
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
  start()
}
