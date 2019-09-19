// If you have a key for something, NotParallel makes sure the code for the key does not run
// multiple times in parallel. Kind of like a lock, except if someone is already running the code,
// it will just wait for it and not run it one more time.
package com.lynxanalytics.biggraph.graph_api

class NotParallel[T] {
  private val promises = collection.mutable.Map[T, concurrent.Promise[Unit]]()

  class NotParallelPromise(key: T, promise: concurrent.Promise[Unit], shouldRun: Boolean) {
    def fulfill[U](work: => U, otherwise: => U): U = {
      if (shouldRun) {
        val result = util.Try(work)
        NotParallel.this.synchronized { promises -= key }
        promise.complete(result.map(_ => ()))
        result.get
      } else {
        // Another thread is doing it already. Just wait for the result.
        concurrent.Await.result(promise.future, concurrent.duration.Duration.Inf)
        otherwise
      }
    }
  }

  def promise(key: T): NotParallelPromise = synchronized {
    promises.get(key) match {
      case Some(p) => new NotParallelPromise(key, p, shouldRun = false)
      case None =>
        val p = concurrent.Promise[Unit]()
        promises(key) = p
        new NotParallelPromise(key, p, shouldRun = true)
    }
  }
}
