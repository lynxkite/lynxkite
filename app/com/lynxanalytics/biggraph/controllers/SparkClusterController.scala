// Request handlers for cluster-level features.
package com.lynxanalytics.biggraph.controllers

import org.apache.spark
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.serving

// Long-poll request for changes in the "busy" state of Spark.
case class SparkStatusRequest(
  syncedUntil: Long) // Client requests to be notified only of events after this time.

case class SparkStatusResponse(
  timestamp: Long, // This is the status at the given time.
  activeStages: List[StageInfo],
  pastStages: List[StageInfo])

case class StageInfo(
  id: Int, // Stage ID.
  hash: Long, // Two stages that do the same thing are expected to have the same hash.
  size: Int, // Number of tasks.
  var tasksCompleted: Int = 0, // Number of tasks already done.
  var lastTaskTime: Long = 0, // Timestamp of last task completion.
  var failed: Boolean = false)

case class SparkClusterStatusResponse(
  master: String,
  workerInstances: Int)

// Request for resizing the cluster. (If supported by the BigGraphEnvironment.)
case class SetClusterNumInstanceRequest(
  workerInstances: Int)

// This listener is used for long polling on /ajax/spark-status.
// The response is delayed until there is an update.
class SparkListener extends spark.scheduler.SparkListener {
  val activeStages = collection.mutable.Map[Int, StageInfo]()
  val pastStages = collection.mutable.Queue[StageInfo]()
  val promises = collection.mutable.Set[concurrent.Promise[SparkStatusResponse]]()
  var currentResp = SparkStatusResponse(0, List(), List())

  override def onStageCompleted(
    stageCompleted: spark.scheduler.SparkListenerStageCompleted): Unit = synchronized {
    val id = stageCompleted.stageInfo.stageId
    val stage = activeStages(id)
    activeStages -= id
    stage.failed = stageCompleted.stageInfo.failureReason.nonEmpty
    pastStages.enqueue(stage)
    while (pastStages.size > 10) {
      pastStages.dequeue()
    }
    send()
  }

  override def onTaskEnd(taskEnd: spark.scheduler.SparkListenerTaskEnd): Unit = synchronized {
    val id = taskEnd.stageId
    if (activeStages.contains(id)) {
      val stage = activeStages(id)
      stage.tasksCompleted += 1
      val time = taskEnd.taskInfo.finishTime
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
    val id = stage.stageId
    val hash = stage.details.hashCode
    val size = stage.numTasks
    val time = stage.submissionTime.getOrElse(System.currentTimeMillis)
    activeStages += id -> StageInfo(id, hash, size, lastTaskTime = time)
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

class SparkClusterController(environment: BigGraphEnvironment) {
  val sc = environment.sparkContext
  val listener = new SparkListener
  sc.addSparkListener(listener)

  def sparkStatus(user: serving.User, req: SparkStatusRequest): concurrent.Future[SparkStatusResponse] = {
    listener.future(req.syncedUntil)
  }

  def sparkCancelJobs(user: serving.User, req: serving.Empty): Unit = {
    assert(user.isAdmin, "Only administrators can cancel jobs.")
    sc.cancelAllJobs()
  }

  def getClusterStatus(user: serving.User, request: serving.Empty): SparkClusterStatusResponse = {
    SparkClusterStatusResponse(environment.sparkContext.master, environment.numInstances)
  }

  def setClusterNumInstances(user: serving.User, request: SetClusterNumInstanceRequest): SparkClusterStatusResponse = {
    assert(user.isAdmin, "Only administrators can resize the cluster.")
    environment.setNumInstances(request.workerInstances)
    return getClusterStatus(user, serving.Empty())
  }

  def checkSparkOperational(): Unit = {
    val sc = environment.sparkContext
    // This pool's properties are defined at /conf/scheduler-pools.xml.
    sc.setLocalProperty("spark.scheduler.pool", "sparkcheck")
    try {
      assert(sc.parallelize(Seq(1, 2, 3), 1).count == 3)
      exerciseMetaGraph()
    } finally sc.setLocalProperty("spark.scheduler.pool", null)
  }

  private def exerciseMetaGraph() = {
    import com.lynxanalytics.biggraph.graph_operations.{ ExampleGraph, CountVertices }
    import com.lynxanalytics.biggraph.graph_api.Scripting._
    implicit val metaManager = environment.metaGraphManager
    implicit val dataManager = environment.dataManager

    val g = ExampleGraph()().result
    val op = CountVertices()
    val out = op(op.vertices, g.vertices).result
    assert(out.count.value == 4)
  }
}
