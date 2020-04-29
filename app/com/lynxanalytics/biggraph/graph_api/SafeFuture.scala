// Scala Futures don't handle all exceptions. #2707
// SafeFuture is a partial re-implementation of the Scala Future interface that catches
// all exceptions. Errors (as opposed to exceptions) are also caught, but they end up
// wrapped inside java.util.concurrent.ExecutionException by the Promise API.
// https://github.com/scala/scala/blob/v2.10.4/src/library/scala/concurrent/Promise.scala#L20
package com.lynxanalytics.biggraph.graph_api

import scala.concurrent._
import scala.util._
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

object SafeFuture {
  def async[T](func: => T)(implicit ec: ExecutionContext) =
    new SafeFuture(unwrapException(Future { wrapException(func) }), Seq())

  def wrap[T](f: Future[T]) = {
    new SafeFuture(f, Seq())
  }

  def successful[T](value: T) = new SafeFuture(Future.successful(value), Seq())
  def failed[T](exception: Throwable) = new SafeFuture(Future.failed[T](exception), Seq())

  def sequence[T](s: Iterable[SafeFuture[T]])(implicit ec: ExecutionContext) =
    new SafeFuture(Future.sequence(s.map(_.future)), s.toSeq)

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
    val stack = new Throwable("SafeFuture creator's stack trace")
    val p = Promise[T]()
    f.onComplete {
      case Failure(Wrapper(t)) =>
        t.addSuppressed(stack)
        p.complete(Failure(t))
      case x => p.complete(x)
    }
    p.future
  }
}
class SafeFuture[+T] private (val future: Future[T], val dependencies: Seq[SafeFuture[_]]) {
  def map[U](func: T => U)(implicit ec: ExecutionContext) =
    new SafeFuture(SafeFuture.unwrapException(future.map(SafeFuture.wrapException(func))), Seq(this))

  def flatMap[U](func: T => SafeFuture[U])(implicit ec: ExecutionContext) =
    new SafeFuture(future.flatMap(t => func(t).future), Seq(this))

  def andThen[U](pf: PartialFunction[Try[T], U])(implicit ec: ExecutionContext) =
    new SafeFuture(future.andThen(pf), Seq(this))

  def withLogging(logMessage: String)(implicit ec: ExecutionContext) = {
    val startTime = System.currentTimeMillis()
    andThen {
      case _ =>
        val elapsed = System.currentTimeMillis() - startTime
        log.info(s"elapsed: ${elapsed} ${logMessage}")
    }
    this
  }

  def awaitResult(atMost: duration.Duration) = Await.result(future, atMost)
  def awaitReady(atMost: duration.Duration): Unit = Await.ready(future, atMost)

  // Simple forwarding for methods that do not create a new Future.
  def onFailure[U](pf: PartialFunction[Throwable, U])(implicit ec: ExecutionContext) =
    future.onFailure(pf)

  def foreach[U](f: T => U)(implicit ec: ExecutionContext) =
    future.foreach(f)

  def value: Option[Try[T]] = future.value

  def get = value.get.get

  def as[T] = this.asInstanceOf[SafeFuture[T]]

  def isCompleted = future.isCompleted

  def hasFailed: Boolean = {
    value match {
      case None => false
      case Some(x: Try[T]) => x.isFailure
    }
  }

  def isWaiting = !dependencies.forall(_.isCompleted)

  // Returns the set of futures leading up to and including this future.
  lazy val dependencySet: Set[SafeFuture[_]] = {
    val visited = collection.mutable.Set[SafeFuture[_]](this)
    val queue = collection.mutable.ListBuffer[SafeFuture[_]](this)
    for (f <- queue) {
      val next = f.dependencies.toSet.diff(visited)
      queue ++= next
      visited ++= next
    }
    visited.toSet
  }
}
