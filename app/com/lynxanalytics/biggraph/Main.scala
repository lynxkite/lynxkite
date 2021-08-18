// The LynxKite main class to give to spark-submit.
package com.lynxanalytics.biggraph

object Main {
  def main(args: Array[String]): Unit = {
    play.core.server.ProdServerStart.main(args)
    // Returning from the main thread will cause Spark to stop since https://github.com/apache/spark/pull/32283.
    // But Play starts separate threads for things. So what do we do with this thread? Just chill out.
    while (true) {
      Thread.sleep(365L * 24 * 3600 * 1000)
    }
  }
}
