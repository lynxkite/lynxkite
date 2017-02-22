package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api.{ SafeFuture, ThreadUtil }

import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.duration.Duration

class LoggedFutures(implicit val executionContext: ExecutionContextExecutorService) {
  private val loggedFutures = collection.mutable.Map[Object, SafeFuture[Unit]]()

  def register(func: => Unit): Unit = loggedFutures.synchronized {
    val key = new Object

    val future = SafeFuture {
      try {
        func
      } catch {
        case t: Throwable => log.error("Future failed: ", t)
      } finally loggedFutures.synchronized {
        loggedFutures.remove(key)
      }
    }
    loggedFutures.put(key, future)
  }

  def waitAllFutures() = {
    val futures = loggedFutures.synchronized { loggedFutures.values.toList }
    SafeFuture.sequence(futures).awaitReady(Duration.Inf)
  }

  def isEmpty() = {
    val futures = loggedFutures.synchronized { loggedFutures.values.toList }
    futures.isEmpty
  }
}