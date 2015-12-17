// Scala Futures don't handle all exceptions. #2707
// SafeFuture is a partial re-implementation of the Scala Future interface that catches
// all exceptions.
package com.lynxanalytics.biggraph.graph_api

import scala.concurrent.duration.Duration

object SafeFuture {
  class Wrapper(t: Throwable) extends Exception(t)

  def apply[T](body: => T)(implicit ec: concurrent.ExecutionContext) =
    new SafeFuture(concurrent.Future { wrapException(body) })

  def successful[T](value: T) = new SafeFuture(concurrent.Future.successful(value))

  def wrapException[B](body: => B): B = {
    try body catch {
      case t: Throwable => throw new Wrapper(t)
    }
  }

  def wrapException[A, B](body: A => B): A => B = { a =>
    try body(a) catch {
      case t: Throwable => throw new Wrapper(t)
    }
  }

  def sequence[T](s: Seq[SafeFuture[T]])(implicit ec: concurrent.ExecutionContext) =
    new SafeFuture(concurrent.Future.sequence(s.map(_.future)))
}
class SafeFuture[+T](val future: concurrent.Future[T]) {
  def map[U](f: T => U)(implicit ec: concurrent.ExecutionContext) =
    new SafeFuture(future.map(SafeFuture.wrapException(f)))

  def flatMap[U](f: T => SafeFuture[U])(implicit ec: concurrent.ExecutionContext) =
    new SafeFuture(future.flatMap(t => f(t).future))

  def awaitResult(atMost: Duration) = concurrent.Await.result(future, atMost)
  def awaitReady(atMost: Duration): Unit = concurrent.Await.ready(future, atMost)

  // Simple forwarding for methods that do not create a new Future.
  def onFailure[U](pf: PartialFunction[Throwable, U])(implicit ec: concurrent.ExecutionContext) =
    future.onFailure(pf)

  def foreach[U](f: T => U)(implicit ec: concurrent.ExecutionContext) =
    future.foreach(f)

  def value = future.value
}
