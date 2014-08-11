package com.lynxanalytics.biggraph.spark_util

import org.apache.spark
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.SparkContext.IntAccumulatorParam
import scala.collection.mutable

object Counters {
  def registerCounter(name: String, counter: spark.Accumulator[Int]): Unit =
    counters += ((name, counter))

  def newCounter(name: String, sc: spark.SparkContext): spark.Accumulator[Int] = {
    val counter = sc.accumulator(0)
    registerCounter(name, counter)
    counter
  }

  def printAll: Unit =
    counters.foreach { case (name, counter) => println("[%800s]: %10d".format(name, counter.value)) }

  private val counters = mutable.Buffer[(String, spark.Accumulator[Int])]()
}
