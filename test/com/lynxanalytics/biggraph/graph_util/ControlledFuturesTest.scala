package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.graph_api.{ SafeFuture, ThreadUtil }
import org.scalatest.FunSuite

class ControlledFuturesTest extends FunSuite {
  val maxParalellism = 11
  val numFutures = 487
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext("LoggedFuturesTest", maxParallelism = maxParalellism)

  test("LoggedFutures works") {
    val loggedFutures = new ControlledFutures()(executionContext)
    val counter = new java.util.concurrent.atomic.AtomicInteger(0)
    for (_ <- 1 to numFutures) {
      loggedFutures.register {
        counter.incrementAndGet()
      }
    }
    loggedFutures.waitAllFutures()
    assert(counter.get() == numFutures)
    assert(loggedFutures.isEmpty())
  }
}
