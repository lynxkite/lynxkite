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
  pastStages: List[StageInfo],
  sparkWorking: Boolean,
  kiteCoreWorking: Boolean)

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
  var currentResp =
    SparkStatusResponse(0, List(), List(), sparkWorking = true, kiteCoreWorking = true)
  var lastSparkActivity = 0L
  var sparkStalled = false

  var kiteCoreWorking = true
  var kiteCoreLastChecked = 0L

  def updateKiteCoreStatus(newKiteCoreWorking: Boolean): Unit = {
    val old = kiteCoreWorking
    kiteCoreWorking = newKiteCoreWorking
    kiteCoreLastChecked = Platform.currentTime
    if (old != kiteCoreWorking) {
      log.info(s"Monitor: kite core working state changed to: $kiteCoreWorking")
      send()
    }
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
      setSparkStalled(false)
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
  }

  def setSparkStalled(stalled: Boolean) {
    val old = sparkStalled
    sparkStalled = stalled
    if (old != sparkStalled) {
      log.info(s"Monitor: spark stalled state changed to: $stalled")
      send()
    }
  }

  private def send(): Unit = synchronized {
    val time = System.currentTimeMillis
    currentResp =
      SparkStatusResponse(
        time,
        activeStages.values.toList,
        pastStages.reverseIterator.toList,
        sparkWorking = !sparkStalled,
        kiteCoreWorking = kiteCoreWorking)
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

  var shouldRun = false

  override def run(): Unit = {
    while (true) {
      if (shouldRun) {
        sc.setLocalProperty("spark.scheduler.pool", "sparkcheck")
        try {
          assert(sc.parallelize(Seq(1, 2, 3), 1).count == 3)
        } finally sc.setLocalProperty("spark.scheduler.pool", null)
        shouldRun = false
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
    if (!shouldRun) {
      shouldRun = true
      notifyAll()
    }
  }

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

  private def logSparkClusterInfo(): Unit = {
    val sc = environment.sparkContext

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

  override def run(): Unit = {
    while (true) {
      val now = Platform.currentTime
      val nextCoreCheck = listener.kiteCoreLastChecked + maxCoreUncheckedMillis
      // We consider spark active if the checker is running, even if it failed to submit
      // any stages.
      val sparkActive = listener.isSparkActive || sparkChecker.shouldRun
      val sparkStalled = listener.sparkStalled
      val lastSparkActivity = listener.lastSparkActivity
      val nextSparkCheck = if (sparkActive) {
        if (sparkStalled) {
          // We use our idle check interval if we already know Spark is stalled to avoid
          // logging too much.
          lastSparkActivity + maxSparkIdleMillis
        } else {
          lastSparkActivity + maxNoSparkProgressMillis
        }
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
        logSparkClusterInfo()
        // We fake some activity to base the next check on the current time.
        listener.sparkActivity(now)
        if (sparkActive) {
          // Nothing happened on an active spark for too long. Let's report this.
          listener.setSparkStalled(true)
        } else {
          // Spark is not-active, but was idle for too long. Let's give it a nudge.
          sparkChecker.nudge()
        }
      }
      val nextCheck = math.min(nextSparkCheck, nextCoreCheck)
      val untilNextCheck = math.max(0, nextCheck - Platform.currentTime)
      Thread.sleep(untilNextCheck)
    }
  }

  setDaemon(true)
  sparkChecker.start()
}

class SparkClusterController(environment: BigGraphEnvironment) {
  val sc = environment.sparkContext
  val listener = new KiteListener
  sc.addSparkListener(listener)

  def getLongEnv(name: String): Option[Long] = scala.util.Properties.envOrNone(name).map(_.toLong)

  val monitor = new KiteMonitorThread(
    listener,
    environment,
    getLongEnv("KITE_MONITOR_MAX_NO_SPARK_PROGRESS_MILLIS")
      .getOrElse(10 * 60 * 1000),
    getLongEnv("KITE_MONITOR_IDLE_SPARK_CHECK_INTERVAL_MILLIS")
      .getOrElse(60 * 60 * 1000),
    getLongEnv("KITE_MONITOR_CORE_CHECK_INTERVAL_MILLIS")
      .getOrElse(5 * 60 * 1000),
    getLongEnv("KITE_MONITOR_CORE_CHECK_TIMEOUT_MILLIS")
      .getOrElse(10 * 1000))
  monitor.start()

  def sparkStatus(user: serving.User, req: SparkStatusRequest): concurrent.Future[SparkStatusResponse] = {
    listener.future(req.syncedUntil)
  }

  def sparkCancelJobs(user: serving.User, req: serving.Empty): Unit = {
    assert(user.isAdmin, "Only administrators can cancel jobs.")
    sc.cancelAllJobs()
  }

  def checkSparkOperational(): Unit = {
    val res = listener.currentResp
    assert(res.kiteCoreWorking && res.sparkWorking)
  }
}
