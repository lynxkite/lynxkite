// Class to keep track of futures that are still running, so that
// we can wait for all of them to finish.
package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api.{ SafeFuture, ThreadUtil }

import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.duration.Duration

class ControlledFutures(implicit val executionContext: ExecutionContextExecutorService) {
  private val controlledFutures = collection.mutable.Map[Object, SafeFuture[Any]]()

  def registerFuture(future: SafeFuture[Any]): Unit = synchronized {
    val key = new Object
    controlledFutures.put(key, future.andThen {
      case scala.util.Failure(t) => log.error("Future failed: ", t)
    } andThen {
      case _ => synchronized { controlledFutures.remove(key) }
    })
  }

  def register(func: => Unit): Unit = {
    registerFuture(SafeFuture { func })
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
