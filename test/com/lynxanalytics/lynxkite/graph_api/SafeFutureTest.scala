package com.lynxanalytics.lynxkite.graph_api

import org.scalatest.funsuite.AnyFunSuite

class SafeFutureTest extends AnyFunSuite {
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration.Duration.Inf

  test("basics") {
    val sf = SafeFuture.async(5)
    assert(sf.awaitResult(Inf) == 5)
    assert(sf.value == Some(util.Success(5)))
    val mapped = sf.map(_ + 1)
    assert(mapped.awaitResult(Inf) == 6)
    assert(mapped.value == Some(util.Success(6)))
  }

  case class TestException() extends Exception("test")

  test("regular exception") {
    val sf = SafeFuture.async(throw TestException())
    intercept[TestException] { sf.awaitResult(Inf) }
    assert(sf.value == Some(util.Failure(TestException())))
  }

  test("fatal exception") {
    val sf = SafeFuture.async(???)
    val e = intercept[java.util.concurrent.ExecutionException] { sf.awaitResult(Inf) }
    assert(e.getCause.getClass == classOf[NotImplementedError])
    assert(sf.value.get.recover { case t: Throwable => t.getCause.getClass }.get ==
      classOf[NotImplementedError])
  }
}
