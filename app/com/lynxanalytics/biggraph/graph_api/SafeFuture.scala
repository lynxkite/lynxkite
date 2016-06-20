// Scala Futures don't handle all exceptions. #2707
// SafeFuture is a partial re-implementation of the Scala Future interface that catches
// all exceptions. Errors (as opposed to exceptions) are also caught, but they end up
// wrapped inside java.util.concurrent.ExecutionException by the Promise API.
// https://github.com/scala/scala/blob/v2.10.4/src/library/scala/concurrent/Promise.scala#L20
package com.lynxanalytics.biggraph.graph_api

import scala.concurrent._
import scala.util._

object SafeFuture {
  def apply[T](func: => T)(implicit ec: ExecutionContext) =
    new SafeFuture(unwrapException(Future { wrapException(func) }))

  def successful[T](value: T) = new SafeFuture(Future.successful(value))

  def sequence[T](s: Seq[SafeFuture[T]])(implicit ec: ExecutionContext) =
    new SafeFuture(Future.sequence(s.map(_.future)))

  private case class Wrapper(t: Throwable) extends Exception(t)

  private def wrapException[B](func: => B): B = {
    try func catch {
      case t: Throwable => throw new Wrapper(t)
    }
  }

  private def wrapException[A, B](func: A => B): A => B = { a =>
    try func(a) catch {
      case t: Throwable => throw new Wrapper(t)
    }
  }

  private def unwrapException[T](f: Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val p = Promise[T]()
    f.onComplete {
      case Failure(Wrapper(t)) => p.complete(Failure(t))
      case x => p.complete(x)
    }
    p.future
  }
}
class SafeFuture[+T] private (val future: Future[T]) {
  def map[U](func: T => U)(implicit ec: ExecutionContext) =
    new SafeFuture(SafeFuture.unwrapException(future.map(SafeFuture.wrapException(func))))

  def flatMap[U](func: T => SafeFuture[U])(implicit ec: ExecutionContext) =
    new SafeFuture(future.flatMap(t => func(t).future))

  def awaitResult(atMost: duration.Duration) = Await.result(future, atMost)
  def awaitReady(atMost: duration.Duration): Unit = Await.ready(future, atMost)

  // Simple forwarding for methods that do not create a new Future.
  def onFailure[U](pf: PartialFunction[Throwable, U])(implicit ec: ExecutionContext) =
    future.onFailure(pf)

  def foreach[U](f: T => U)(implicit ec: ExecutionContext) =
    future.foreach(f)

  def value = future.value

  def isCompleted = future.isCompleted

  def zip[U](other: SafeFuture[U]): SafeFuture[(T, U)] =
    new SafeFuture(future.zip(other.future))
}
