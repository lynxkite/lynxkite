package com.lynxanalytics.biggraph.controllers

import org.apache.spark
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.serving

case class SparkStatusRequest(
  syncedUntil: Long) // Client requests to be notified only of events after this time.

case class SparkStatusResponse(
  activeStages: Seq[Int],
  timestamp: Long) // This is the status at the given time.

case class SparkClusterStatusResponse(
  master: String,
  workerInstances: Int)

case class SetClusterNumInstanceRequest(
  password: String,
  workerInstances: Int)

// This listener is used for long polling on /ajax/spark-status.
// The response is delayed until there is an update.
class SparkListener extends spark.scheduler.SparkListener {
  val activeStages = collection.mutable.Set[Int]()
  val promises = collection.mutable.Set[concurrent.Promise[SparkStatusResponse]]()
  var currentResp = SparkStatusResponse(Seq(), 0)

  override def onStageCompleted(
    stageCompleted: spark.scheduler.SparkListenerStageCompleted): Unit = synchronized {
    activeStages -= stageCompleted.stageInfo.stageId
    send()
  }

  override def onStageSubmitted(
    stageSubmitted: spark.scheduler.SparkListenerStageSubmitted): Unit = synchronized {
    activeStages += stageSubmitted.stageInfo.stageId
    send()
  }

  private def send(): Unit = synchronized {
    val time = System.currentTimeMillis
    currentResp = SparkStatusResponse(activeStages.toSeq, time)
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
    sc.cancelAllJobs()
  }

  def getClusterStatus(user: serving.User, request: serving.Empty): SparkClusterStatusResponse = {
    SparkClusterStatusResponse(environment.sparkContext.master, environment.numInstances)
  }

  def setClusterNumInstances(user: serving.User, request: SetClusterNumInstanceRequest): SparkClusterStatusResponse = {
    if (request.password != "UCU8HB0d6fQJwyD8UAdDb")
      throw new IllegalArgumentException("Bad password!")
    environment.setNumInstances(request.workerInstances)
    return getClusterStatus(user, serving.Empty())
  }
}
