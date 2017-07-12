// Request handlers for cluster-level features.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import org.apache.spark
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.serving

// Long-poll request for changes in the "busy" state of Spark.
case class SparkStatusRequest(
  syncedUntil: Long) // Client requests to be notified only of events after this time.

case class SparkStatusResponse(
  timestamp: Long, // This is the status at the given time.
  activeStages: List[StageInfo],
  pastStages: List[StageInfo],
  activeExecutorNum: Option[Int],
  configedExecutorNum: Option[Int],
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
class KiteListener(sc: spark.SparkContext) extends spark.scheduler.SparkListener {
  private val activeStages = collection.mutable.Map[String, StageInfo]()
  private val pastStages = collection.mutable.Queue[StageInfo]()
  private val promises = collection.mutable.Set[concurrent.Promise[SparkStatusResponse]]()
  private var currentResp =
    SparkStatusResponse(0, List(), List(), None, None, sparkWorking = true, kiteCoreWorking = true)
  // The time of the last registered spark task finish event.
  private var lastSparkTaskFinish = 0L
  // Whether, to the knowledge of this listener, spark is stalled.
  private var sparkStalled = false
  // Whether, to the knowledge of this listener, kite core (*Manager) is working.
  private var kiteCoreWorking = true

  def getCurrentResponse = synchronized { currentResp }

  def getLastSparkTaskFinish = synchronized { lastSparkTaskFinish }

  def updateKiteCoreStatus(newKiteCoreWorking: Boolean): Unit = synchronized {
    val old = kiteCoreWorking
    kiteCoreWorking = newKiteCoreWorking
    if (old != kiteCoreWorking) {
      log.info(s"Monitor: kite core working state changed to: $kiteCoreWorking")
      send()
    }
  }

  def isSparkActive: Boolean = synchronized { activeStages.nonEmpty }

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
      lastSparkTaskFinish = time max lastSparkTaskFinish
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
    if (!stage.details.contains("checkSparkOperational")) { // Ignore health checks.
      activeStages += id -> StageInfo(id, hash, size, lastTaskTime = time)
      send()
    }
  }

  def isSparkStalled = synchronized { sparkStalled }
  def setSparkStalled(stalled: Boolean): Unit = synchronized {
    val old = sparkStalled
    sparkStalled = stalled
    if (old != sparkStalled) {
      log.info(s"Monitor: spark stalled state changed to: $stalled")
      send()
    }
  }

  def numExecutors: Option[Int] = synchronized {
    LoggedEnvironment.envOrNone("SPARK_MASTER").get match {
      case s if s.startsWith("local") => None
      case _ => Some(sc.statusTracker.getExecutorInfos.size - 1)
    }
  }

  private def send(): Unit = synchronized {
    val time = System.currentTimeMillis
    currentResp =
      SparkStatusResponse(
        time,
        activeStages.values.toList,
        pastStages.reverseIterator.toList,
        numExecutors,
        sys.props.get("spark.executor.instances").map(_.toInt),
        sparkWorking = !sparkStalled,
        kiteCoreWorking = kiteCoreWorking)
    for (p <- promises) {
      p.success(currentResp)
    }
    promises.clear()
  }

  // Returns a future response to a client who is up to date until the given timestamp.
  def future(syncedUntil: Long): concurrent.Future[SparkStatusResponse] = synchronized {
    val p = concurrent.Promise[SparkStatusResponse]
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

  private var shouldRun = false

  override def run(): Unit = {
    while (true) {
      if (synchronized { shouldRun }) {
        try {
          val result = sc.parallelize(Seq(1, 2, 3), 1).count
          assert(result == 3, s"Incorrect result from Spark: $result != 3")
        } catch {
          // We don't care about errors here, we just want to start some Spark job
          // that can be observed by the listener to succeed (or not). So we just catch all
          // possible error and let the thread live regardless.
          case e: Throwable =>
            log.error("Error in running the Spark check job in SparkCheckThread", e)
        }
        synchronized {
          shouldRun = false
        }
      } else {
        synchronized {
          wait()
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

  def isRunning: Boolean = synchronized { shouldRun }

  setDaemon(true)
}

// A monitoring thread that periodically checks Kite components for being operational. It works
// together with a KiteListener: it updates the listener with its findings and also uses the
// listener's insights for determining Spark's status. (If we see successful tasks from normal
// (non-monitoring) spark jobs then we don't even start test spark jobs to test the cluster.)
class KiteMonitorThread(
    listener: KiteListener,
    environment: BigGraphEnvironment,
    // We report a problem if no successful tasks were reported on an active spark for this long.
    maxNoSparkProgressMillis: Long,
    // We start a test job on spark if it was idle for this amount of time.
    maxSparkIdleMillis: Long,
    // We check kite core if it went unchecked for this amount of time.
    maxCoreUncheckedMillis: Long,
    // We declare kite core dead if it didn't respond in this amount of time.
    coreTimeoutMillis: Long) extends Thread("kite-monitor") {

  val sparkChecker = new SparkCheckThread(listener, environment.sparkContext)
  sparkChecker.start()

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
    // But NUM_CORES_PER_EXECUTOR is now always required when starting Kite and we launch Spark
    // in a way that this is probably mostly reliable.
    val numCoresPerExecutor =
      LoggedEnvironment.envOrNone("NUM_CORES_PER_EXECUTOR").get.toInt
    val totalCores = listener.numExecutors.getOrElse(1) * numCoresPerExecutor
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
    var sparkLastLookedAt = 0L
    var kiteCoreLastChecked = 0L

    // Before starting the normal monitoring setup, we do a first core check. The first check
    // might take a longer time as the example graph may not be computed yet.
    assert(kiteCoreWorks(), "First kite core check failed")
    while (true) {
      val now = System.currentTimeMillis
      val nextCoreCheck = kiteCoreLastChecked + maxCoreUncheckedMillis
      // We consider spark active if the checker is running, even if it failed to submit
      // any stages.
      val sparkActive = listener.isSparkActive || sparkChecker.isRunning
      val sparkStalled = listener.isSparkStalled
      val lastSparkEvent = listener.getLastSparkTaskFinish max sparkLastLookedAt
      val sc = environment.sparkContext
      val runningTaskCount = if (!sc.isStopped) sc.getAllPools.map(_.runningTasks).sum else 0
      val nextSparkCheck = if (runningTaskCount > 0) {
        now + maxNoSparkProgressMillis
      } else if (sparkActive) {
        if (sparkStalled) {
          // We use our idle check interval if we already know Spark is stalled to avoid
          // logging too much.
          lastSparkEvent + maxSparkIdleMillis
        } else {
          lastSparkEvent + maxNoSparkProgressMillis
        }
      } else {
        lastSparkEvent + maxSparkIdleMillis
      }
      if (now > nextCoreCheck) {
        // do core checks
        import scala.concurrent.ExecutionContext.Implicits.global
        val testsDone = concurrent.Future { kiteCoreWorks() }
        listener.updateKiteCoreStatus(
          try {
            concurrent.Await.result(
              testsDone,
              concurrent.duration.Duration(coreTimeoutMillis, "millisecond"))
          } catch {
            case e: java.util.concurrent.TimeoutException =>
              log.error("Kite core test timed out. Thread dump:\n" + threadDump())
              false
            case e: Throwable =>
              log.error("Error while testing kite core", e)
              false
          })
        kiteCoreLastChecked = System.currentTimeMillis
      } else if (now > nextSparkCheck) {
        logSparkClusterInfo()
        sparkLastLookedAt = now
        if (sparkActive) {
          // Nothing happened on an active spark for too long. Let's report this.
          listener.setSparkStalled(true)
        } else {
          // Spark is non-active, but was idle for too long. Let's give it a nudge.
          sparkChecker.nudge()
        }
      }
      val nextCheck = nextSparkCheck min nextCoreCheck
      val untilNextCheck = 0L max (nextCheck - System.currentTimeMillis)
      Thread.sleep(untilNextCheck)
    }
  }

  def threadDump(): String = {
    val mxbean = java.lang.management.ManagementFactory.getThreadMXBean
    val threadInfos = mxbean.getThreadInfo(mxbean.getAllThreadIds, /* max lines of stack trace per thread: */ 1000)
    threadInfos.flatMap { info =>
      s"\n\n${info.getThreadName} (${info.getThreadState}):" +:
        info.getStackTrace.map(line => s"\n  at $line")
    }.mkString
  }

  setDaemon(true)
}

// This is a thread that can be used to watch the healthiness
// of LynxKite and kill the whole process if it has been unhealthy
// for too long.
// - LynxKite will be shut down if shutDownTimeoutSecs pass after
//   the last successful and positive health check.
// - listener and controller are used for performing the health
//   checks.
object InternalWatchdogThread {
  val NanosPerSeconds = 1000 * 1000 * 1000
  val CheckPeriodMillis = 1000
}
class InternalWatchdogThread(
  shutdownTimeoutSecs: Int,
  listener: KiteListener,
  controller: SparkClusterController)
    extends Thread("internal-watchdog-thread") {

  val warningTimeoutSecs = shutdownTimeoutSecs / 2

  def checkExitCondition(lastOkStatusTimeNanos: Long): Unit = {
    val unhealthyForSeconds = (System.nanoTime - lastOkStatusTimeNanos) /
      InternalWatchdogThread.NanosPerSeconds
    def msg = {
      s"Watchdog: LynxKite has been unhealthy for ${unhealthyForSeconds} seconds."
    }

    if (unhealthyForSeconds > shutdownTimeoutSecs) {
      log.error(msg + " Exiting LynxKite.")
      System.exit(1)
    }
    if (unhealthyForSeconds > warningTimeoutSecs) {
      log.warn(msg)
    }
  }

  override def run(): Unit = {
    var lastOkStatusTimeNanos = System.nanoTime
    while (true) {
      val status = listener.getCurrentResponse
      if (controller.getForceReportHealthy() ||
        (status.sparkWorking && status.kiteCoreWorking)) {
        lastOkStatusTimeNanos = System.nanoTime
      }
      checkExitCondition(lastOkStatusTimeNanos)
      Thread.sleep(InternalWatchdogThread.CheckPeriodMillis)
    }
  }

  setDaemon(true)
}

class SparkClusterController(environment: BigGraphEnvironment) {
  // The health checks are always running, but if the below flag is true, then their results
  // are going to be ignored.
  private var forceReportHealthy = false
  val sc = environment.sparkContext
  val listener = new KiteListener(sc)
  sc.addSparkListener(listener)
  LoggedEnvironment.envOrNone(
    "KITE_INTERNAL_WATCHDOG_TIMEOUT_SECONDS").foreach { timeoutSecs =>
      val watchdog = new InternalWatchdogThread(
        timeoutSecs.toInt, listener, this)
      watchdog.start()
    }

  def getLongEnv(name: String): Option[Long] = LoggedEnvironment.envOrNone(name).map(_.toLong)

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

  def setForceReportHealthy(value: Boolean): Unit = synchronized {
    forceReportHealthy = value
  }

  def getForceReportHealthy(): Boolean = synchronized {
    forceReportHealthy
  }

  def sparkStatus(user: serving.User, req: SparkStatusRequest)(
    implicit ec: concurrent.ExecutionContext): concurrent.Future[SparkStatusResponse] = {
    val res = listener.future(req.syncedUntil)
    if (!getForceReportHealthy) {
      res
    } else {
      res.map { _.copy(kiteCoreWorking = true, sparkWorking = true) }
    }
  }

  def sparkCancelJobs(user: serving.User, req: serving.Empty): Unit = {
    assert(user.isAdmin, "Only administrators can cancel jobs.")
    sc.cancelAllJobs()
  }

  def checkSparkOperational(): Unit = {
    if (!getForceReportHealthy) {
      val res = listener.getCurrentResponse
      assert(res.kiteCoreWorking && res.sparkWorking)
    }
  }
}
