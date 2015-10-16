// Request handlers for cluster-level features.
package com.lynxanalytics.biggraph.controllers

import org.apache.spark
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.serving
import scala.compat.Platform
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

// Long-poll request for changes in the "busy" state of Spark.
case class SparkStatusRequest(
  syncedUntil: Long) // Client requests to be notified only of events after this time.

case class SparkStatusResponse(
  timestamp: Long, // This is the status at the given time.
  activeStages: List[StageInfo],
  pastStages: List[StageInfo])

case class StageInfo(
  id: String, // Stage ID with attempt ID.
  hash: Long, // Two stages that do the same thing are expected to have the same hash.
  size: Int, // Number of tasks.
  var tasksCompleted: Int = 0, // Number of tasks already done.
  var lastTaskTime: Long = 0, // Timestamp of last task completion.
  var failed: Boolean = false)

// This listener is used for long polling on /ajax/spark-status.
// The response is delayed until there is an update.
class KiteListener extends spark.scheduler.SparkListener {
  val activeStages = collection.mutable.Map[String, StageInfo]()
  val pastStages = collection.mutable.Queue[StageInfo]()
  val promises = collection.mutable.Set[concurrent.Promise[SparkStatusResponse]]()
  var currentResp = SparkStatusResponse(0, List(), List())
  var lastSparkActivity = 0L
  var stalled = false

  var kiteCoreWorking = true
  var kiteCoreLastChecked = 0L

  def updateKiteCoreStatus(newKiteCoreWorking: Boolean): Unit = {
    if (kiteCoreWorking != newKiteCoreWorking) {
      send()
    }
    kiteCoreWorking = newKiteCoreWorking
    kiteCoreLastChecked = Platform.currentTime
  }

  def isSparkActive: Boolean = activeStages.nonEmpty

  private def fullId(stage: org.apache.spark.scheduler.StageInfo): String =
    s"${stage.stageId}.${stage.attemptId}"

  override def onStageCompleted(
    stageCompleted: spark.scheduler.SparkListenerStageCompleted): Unit = synchronized {
    val id = fullId(stageCompleted.stageInfo)
    if (activeStages.contains(id)) {
      val stage = activeStages(id)
      activeStages -= id
      stage.failed = stageCompleted.stageInfo.failureReason.nonEmpty
      pastStages.enqueue(stage)
      while (pastStages.size > 10) {
        pastStages.dequeue()
      }
      send()
    }
  }

  override def onTaskEnd(taskEnd: spark.scheduler.SparkListenerTaskEnd): Unit = synchronized {
    val id = s"${taskEnd.stageId}.${taskEnd.stageAttemptId}"
    if (activeStages.contains(id)) {
      val stage = activeStages(id)
      stage.tasksCompleted += 1
      val time = taskEnd.taskInfo.finishTime
      sparkActivity(time)
      // Post at most one update per second.
      if (time - stage.lastTaskTime > 1000) {
        stage.lastTaskTime = time
        send()
      }
    }
  }

  override def onStageSubmitted(
    stageSubmitted: spark.scheduler.SparkListenerStageSubmitted): Unit = synchronized {
    val stage = stageSubmitted.stageInfo
    val id = fullId(stage)
    val hash = stage.details.hashCode
    val size = stage.numTasks
    val time = stage.submissionTime.getOrElse(System.currentTimeMillis)
    sparkActivity(time)
    if (!stage.details.contains("checkSparkOperational")) { // Ignore health checks.
      activeStages += id -> StageInfo(id, hash, size, lastTaskTime = time)
      send()
    }
  }

  def sparkActivity(time: Long): Unit = {
    lastSparkActivity = time max lastSparkActivity
    stalled = false
  }

  def onSparkStalled() {
    stalled = true
    send()
  }

  private def send(): Unit = synchronized {
    val time = System.currentTimeMillis
    currentResp =
      SparkStatusResponse(time, activeStages.values.toList, pastStages.reverseIterator.toList)
    for (p <- promises) {
      p.success(currentResp)
    }
    promises.clear()
  }

  // Returns a future response to a client who is up to date until the given timestamp.
  def future(syncedUntil: Long): concurrent.Future[SparkStatusResponse] = synchronized {
    val p = concurrent.promise[SparkStatusResponse]
    if (syncedUntil < currentResp.timestamp) {
      p.success(currentResp) // We immediately have news for you.
    } else {
      promises += p // No news currently. You have successfully subscribed.
    }
    return p.future
  }
}

class SparkCheckThread(
    listener: KiteListener,
    sc: spark.SparkContext) extends Thread("spark-check") {

  var runningCheckStart: Option[Long] = None

  override def run(): Unit = {
    while (true) {
      if (runningCheckStart.nonEmpty) {
        sc.setLocalProperty("spark.scheduler.pool", "sparkcheck")
        try {
          assert(sc.parallelize(Seq(1, 2, 3), 1).count == 3)
        } finally sc.setLocalProperty("spark.scheduler.pool", null)
        runningCheckStart = None
      } else {
        synchronized {
          try {
            wait()
          } catch {
            case e: InterruptedException => ()
          }
        }
      }
    }
  }

  def nudge(): Unit = synchronized {
    if (runningCheckStart.isEmpty) {
      runningCheckStart = Some(Platform.currentTime)
      notify()
    }
  }

  def isSparkActive: Boolean = listener.isSparkActive || runningCheckStart.nonEmpty

  def lastSparkActivity: Long = listener.lastSparkActivity max runningCheckStart.getOrElse(0)

  setDaemon(true)
}

class KiteMonitorThread(
    listener: KiteListener,
    environment: BigGraphEnvironment,
    maxNoSparkProgressMillis: Long,
    maxSparkIdleMillis: Long,
    maxCoreUncheckedMillis: Long,
    coreTimeoutMillis: Long) extends Thread("kite-monitor") {

  val sparkChecker = new SparkCheckThread(listener, environment.sparkContext)

  var disabledUntil = 0L

  private def kiteCoreWorks(): Boolean = {
    import com.lynxanalytics.biggraph.graph_operations.{ ExampleGraph, CountVertices }
    import com.lynxanalytics.biggraph.graph_api.Scripting._
    implicit val metaManager = environment.metaGraphManager
    implicit val dataManager = environment.dataManager

    val g = ExampleGraph()().result
    val op = CountVertices()
    val out = op(op.vertices, g.vertices).result
    out.count.value == 4
  }

  override def run(): Unit = {
    while (true) {
      val now = Platform.currentTime
      if (now > disabledUntil) {
        val nextCoreCheck = listener.kiteCoreLastChecked + maxCoreUncheckedMillis
        val sparkActive = sparkChecker.isSparkActive
        val lastSparkActivity = sparkChecker.lastSparkActivity
        val nextSparkCheck = if (sparkActive) {
          lastSparkActivity + maxNoSparkProgressMillis
        } else {
          lastSparkActivity + maxSparkIdleMillis
        }
        if (now > nextCoreCheck) {
          // do core checks
          val testsDone = future { kiteCoreWorks() }
          listener.updateKiteCoreStatus(
            scala.util.Try(
              Await.result(testsDone, duration.Duration(coreTimeoutMillis, "millisecond")))
              .getOrElse(false))
        }
        if (now > nextSparkCheck) {
          if (sparkActive) {
            // Nothing happened on an active spark for too long. Let's report this.
            listener.onSparkStalled()
            // We know we are in a bad state, but we don't want to report this in an infinite loop.
            // Disable monitoring for 10 minutes.
            disabledUntil = now + 10 * 60 * 1000
          } else {
            sparkChecker.nudge()
          }
        }
        val untilNextCheck = math.max(
          0,
          math.min(nextSparkCheck, nextCoreCheck) - Platform.currentTime)
        Thread.sleep(untilNextCheck)
      }
    }
  }

  setDaemon(true)
  sparkChecker.start()
}

class SparkClusterController(environment: BigGraphEnvironment) {
  val sc = environment.sparkContext
  val listener = new KiteListener
  sc.addSparkListener(listener)

  def sparkStatus(user: serving.User, req: SparkStatusRequest): concurrent.Future[SparkStatusResponse] = {
    listener.future(req.syncedUntil)
  }

  def sparkCancelJobs(user: serving.User, req: serving.Empty): Unit = {
    assert(user.isAdmin, "Only administrators can cancel jobs.")
    sc.cancelAllJobs()
  }

  def logSparkClusterInfo(): Unit = {
    // No way to find cores per executor programmatically. SPARK-2095
    // But NUM_CORES_PER_EXECUTOR is now always required when starting Kite and we launch spark
    // in a way that this is probably mostly reliable.
    val numCoresPerExecutor =
      scala.util.Properties.envOrNone("NUM_CORES_PER_EXECUTOR").get.toInt
    val numExecutors = (sc.getExecutorStorageStatus.size - 1) max 1
    val totalCores = numExecutors * numCoresPerExecutor
    val cacheMemory = sc.getExecutorMemoryStatus.values.map(_._1).sum
    val conf = sc.getConf
    // Unfortunately the defaults are hard-coded in Spark and not available.
    val cacheFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val shuffleFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    val workFraction = 1.0 - cacheFraction - shuffleFraction
    val workMemory = workFraction * cacheMemory / cacheFraction
    log.info("Spark cluster status report. See estimated cluster parameters below.")
    log.info("Work memory: " + workMemory)
    log.info("Total cores: " + totalCores)
    log.info("Cache memory: " + cacheMemory)
    log.info("Work fraction: " + workFraction)
    log.info("Cache fraction: " + cacheFraction)
    log.info("WM per core: " + (workMemory / totalCores).toLong)
  }

  def checkSparkOperational(): Unit = {
    logSparkClusterInfo()
    val sc = environment.sparkContext
    // This pool's properties are defined at /conf/scheduler-pools.xml.
    sc.setLocalProperty("spark.scheduler.pool", "sparkcheck")
    try {
      assert(sc.parallelize(Seq(1, 2, 3), 1).count == 3)
      //exerciseMetaGraph()
    } finally sc.setLocalProperty("spark.scheduler.pool", null)
  }
}
