// Class to keep track of futures that are still running, so that
// we can wait for all of them to finish.
package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.{logger => log}
import com.lynxanalytics.biggraph.graph_api.{SafeFuture, ThreadUtil}

import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.duration.Duration

class ControlledFutures(implicit val executionContext: ExecutionContextExecutorService) {
  private val controlledFutures = collection.mutable.Map[Object, SafeFuture[Any]]()

  def registerFuture[T](future: SafeFuture[T]): SafeFuture[T] = synchronized {
    val key = new Object
    controlledFutures.put(
      key,
      future.andThen {
        case scala.util.Failure(t) => log.error("Future failed: ", t)
      } andThen {
        case _ => synchronized { controlledFutures.remove(key) }
      })
    future
  }

  def register[T](func: => T): SafeFuture[T] = {
    registerFuture[T](SafeFuture.async(func))
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
