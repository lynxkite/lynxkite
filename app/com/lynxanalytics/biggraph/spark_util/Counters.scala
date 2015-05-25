// Accumulator-based counters for debugging.
package com.lynxanalytics.biggraph.spark_util

import org.apache.spark
import scala.collection.mutable
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

object Counters {
  def registerCounter(name: String, counter: spark.Accumulator[Int]): Unit =
    counters += ((name, counter))

  def newCounter(name: String, sc: spark.SparkContext): spark.Accumulator[Int] = {
    val counter = sc.accumulator(0)
    registerCounter(name, counter)
    counter
  }

  def printAll: Unit = {
    for ((name, counter) <- counters) {
      log.info(s"$name: ${counter.value}")
    }
  }

  private val counters = mutable.Buffer[(String, spark.Accumulator[Int])]()
}
