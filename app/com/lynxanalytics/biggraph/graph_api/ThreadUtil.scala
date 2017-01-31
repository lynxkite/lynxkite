// Common utilities for working with threads.
package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

object ThreadUtil {
  def limitedExecutionContext(name: String, maxParallelism: Int) = {
    concurrent.ExecutionContext.fromExecutorService(
      java.util.concurrent.Executors.newFixedThreadPool(
        maxParallelism,
        new java.util.concurrent.ThreadFactory() {
          private var nextIndex = 1
          private val uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler {
            def uncaughtException(thread: Thread, cause: Throwable): Unit = {
              // SafeFuture should catch everything but this is still here to be sure.
              log.error(s"$name thread failed:", cause)
              throw cause
            }
          }
          def newThread(r: Runnable) = synchronized {
            val t = new Thread(r)
            t.setDaemon(true)
            t.setName(s"$name-$nextIndex")
            t.setUncaughtExceptionHandler(uncaughtExceptionHandler)
            nextIndex += 1
            t
          }
        }
      ))
  }
}

class LockableExecutionContext(name: String, maxParallelism: Int)
    extends concurrent.ExecutionContext {
  private val ec = ThreadUtil.limitedExecutionContext(name, maxParallelism)
  private var locked = new ThreadLocal[Boolean] { override def initialValue() = false }
  override def reportFailure(cause: Throwable): Unit = ec.reportFailure(cause)
  override def execute(runnable: Runnable): Unit = synchronized {
    assert(!locked.get, s"The $name thread pool cannot be used in this context.")
    ec.execute(runnable)
  }
  def withLock[T](func: => T): T = {
    locked.set(true)
    try func
    finally locked.set(false)
  }
}
