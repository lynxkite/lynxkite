// Class to keep track of futures that are still running, so that
// we can wait for all of them to finish.
package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api.{ SafeFuture, ThreadUtil }

import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.duration.Duration

class ControlledFutures(implicit val executionContext: ExecutionContextExecutorService) {
  private val controlledFutures = collection.mutable.Map[Object, SafeFuture[Unit]]()

  def register(func: => Unit): Unit = synchronized {
    val key = new Object

    val future = SafeFuture {
      try {
        func
      } catch {
        case t: Throwable => log.error("Future failed: ", t)
      } finally synchronized {
        controlledFutures.remove(key)
      }
    }
    controlledFutures.put(key, future)
  }

  def waitAllFutures() = {
    val futures = synchronized { controlledFutures.values.toList }
    SafeFuture.sequence(futures).awaitReady(Duration.Inf)
  }

  def isEmpty() = {
    val futures = synchronized { controlledFutures.values.toList }
    futures.isEmpty
  }
}
