// If you have a key for something, NotParallel makes sure the code for the key does not run
// multiple times in parallel. Kind of like a lock, except if someone is already running the code,
// it will just wait for it and not run it one more time.
package com.lynxanalytics.biggraph.graph_api

class NotParallel[T] {
  private val promises = collection.mutable.Map[T, concurrent.Promise[Unit]]()
  def apply(key: T)(fn: => Unit): Unit = {
    val (ongoing, promise) = synchronized {
      promises.get(key) match {
        case Some(p) => (true, p)
        case None =>
          val p = concurrent.Promise[Unit]()
          promises(key) = p
          (false, p)
      }
    }
    if (ongoing) {
      // Another thread is doing it already. Just wait for the result.
      concurrent.Await.result(promise.future, concurrent.duration.Duration.Inf)
    } else {
      // We have to do it.
      fn
      synchronized { promises -= key }
      promise.success(())
    }
  }
}
