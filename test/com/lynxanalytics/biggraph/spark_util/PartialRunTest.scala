package com.lynxanalytics.biggraph.spark_util

import org.scalatest.FunSuite
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import com.lynxanalytics.biggraph.TestSparkContext
import com.lynxanalytics.biggraph.Timed

class PartialRunTest extends FunSuite with TestSparkContext {
  import Implicits._

  def genData(parts: Int, rows: Int, seed: Int): RDD[(Long, Char)] = {
    val raw = sparkContext.parallelize(1 to parts, parts).mapPartitionsWithIndex {
      (i, it) => new util.Random(i + seed).alphanumeric.take(rows).iterator
    }
    val partitioner = new HashPartitioner(raw.partitions.size)
    raw.zipWithUniqueId.map { case (v, id) => id -> v }.partitionBy(partitioner)
  }

  def countAs(in: RDD[(Long, Char)], prefLength: Int): Int = {
    in.mapPartitions(
      it => Iterator(it.take(prefLength).filter { case (k, v) => v == 'a' }.size))
      .reduce(_ + _)
  }

  test("benchmark partial map", com.lynxanalytics.biggraph.Benchmark) {
    val table = "%10s | %10s | %10s | %10s | %10s"
    println(table.format("rows", "rows processed", "as", "time (ms)", "nanos / row processed"))
    for (round <- 10 to 10) {
      val parts = 4
      val rows = 10 * round
      val d = genData(parts, rows, 0).toSortedRDD.cache
      println("Caching initial data", Timed(d.calculate).nanos / 1000000)
      println("Second calculate", Timed(d.calculate).nanos / 1000000)
      for (pref <- 1 to 10) {
        val d1 = d.filter(a => { Thread.sleep(10L); true }).toSortedRDD
        val d2 = d.mapValues(a => { Thread.sleep(10L); a }).toSortedRDD
        val data = d1.join(d2).mapValues { case (a, b) => a }
        val prefLength = pref * rows / 10
        val t = Timed(countAs(data, prefLength))
        val processed = prefLength * parts
        println(table.format(
          parts * rows,
          processed,
          t.value,
          t.nanos / 1000000,
          t.nanos / processed))
      }
    }
  }
}
