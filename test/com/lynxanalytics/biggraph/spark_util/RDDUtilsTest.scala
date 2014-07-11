package com.lynxanalytics.biggraph.spark_util

import org.scalatest.FunSuite
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.lynxanalytics.biggraph.TestSparkContext

class RDDUtilsTest extends FunSuite with TestSparkContext {
  test("fastNumbered works for a few items") {
    import Implicits._
    val rdd = sparkContext.parallelize(1 to 10, 3)
    val numbered = rdd.fastNumbered.collect.toSeq
    assert(numbered == Seq(0 -> 1, 3 -> 2, 6 -> 3, 1 -> 4, 4 -> 5, 7 -> 6, 2 -> 7, 5 -> 8, 8 -> 9, 11 -> 10))
  }

  test("genID works for small and large numbers") {
    val parts = 10
    val border = Int.MaxValue / parts
    val interestingRows = Seq(
      0, 1, parts - 1, parts, parts + 1,
      border - 1, border, border + 1,
      2 * border - 1, 2 * border, 2 * border + 1,
      3 * border - 1, 3 * border, 3 * border + 1,
      4 * border - 1, 4 * border, 4 * border + 1,
      5 * border - 1, 5 * border, 5 * border + 1)
    val partitioner = new HashPartitioner(parts)
    val ids = collection.mutable.Set[Long]()
    for (part <- 0 until parts) {
      for (row <- interestingRows) {
        val id = Implicits.genID(parts, part, row)
        assert(partitioner.getPartition(id) == part, s"genID($parts, $part, $row)")
        assert(!ids.contains(id), s"genID($parts, $part, $row)")
        ids += id
      }
    }
  }

  test("genID works for random numbers") {
    val rnd = new util.Random(0)
    for (i <- 0 to 1000) {
      val parts = rnd.nextInt(1000) max 1
      val part = rnd.nextInt(parts)
      val row = rnd.nextInt(1000000)
      val id = Implicits.genID(parts, part, row)
      val partitioner = new HashPartitioner(parts)
      assert(partitioner.getPartition(id) == part, s"genID($parts, $part, $row)")
    }
  }

  case class Timed[X](nanos: Long, value: X)
  object Timed {
    def apply[X](f: => X): Timed[X] = {
      val t0 = System.nanoTime
      val value = f
      val duration = System.nanoTime - t0
      Timed(duration, value)
    }
  }

  test("benchmark zipJoin", com.lynxanalytics.biggraph.Benchmark) {
    import Implicits._
    class Demo(parts: Int, rows: Int) {
      val data = gen(parts, rows, 1)
      val other = gen(parts, rows, 2).sample(false, 0.5, 0).partitionBy(data.partitioner.get)
      def zippedSum = getSum(data.zipJoin(other))
      def joinedSum = getSum(data.join(other))
      def gen(parts: Int, rows: Int, seed: Int) = {
        val raw = sparkContext.parallelize(1 to parts, parts).mapPartitionsWithIndex {
          (i, it) => new util.Random(i + seed).alphanumeric.take(rows).iterator
        }
        val partitioner = new org.apache.spark.HashPartitioner(raw.partitions.size)
        val data = raw.zipWithUniqueId.map { case (v, id) => id -> v }.partitionBy(partitioner).mapPartitions(p => p.toSeq.sorted.iterator, true)
        data.cache()
        data.foreach(_ => ()) // Trigger computation and caching.
        data
      }
      def getSum(rdd: org.apache.spark.rdd.RDD[(Long, (Char, Char))]) = rdd.mapValues { case (a, b) => a compare b }.values.reduce(_ + _)
    }

    val parts = 4
    val table = "%10s | %10s | %10s"
    println(table.format("rows", "join (ms)", "zipJoin (ms)"))
    for (round <- 10 to 20) {
      val rows = 100000 * round
      val demo = new Demo(parts, rows)
      val zipped = Timed(demo.zippedSum)
      val joined = Timed(demo.joinedSum)
      assert(joined.value == zipped.value)
      println(table.format(parts * rows, joined.nanos / 1000000, zipped.nanos / 1000000))
    }
  }
}
