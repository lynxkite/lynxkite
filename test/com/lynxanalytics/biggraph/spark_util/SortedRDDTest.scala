package com.lynxanalytics.biggraph.spark_util

import org.scalatest.FunSuite
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.lynxanalytics.biggraph.TestSparkContext
import com.lynxanalytics.biggraph.Timed

class SortedRDDTest extends FunSuite with TestSparkContext {
  import Implicits._
  test("join") {
    val p = new HashPartitioner(4)
    val a = sparkContext.parallelize(10 to 15).map(x => (x, x)).partitionBy(p).toSortedRDD
    val b = sparkContext.parallelize(20 to 25).map(x => (x, x)).partitionBy(p).toSortedRDD
    val j: SortedRDD[Int, (Int, Int)] = a.sortedJoin(b)
    assert(j.collect.toSeq == Seq())
  }

  test("distinct") {
    val p = new HashPartitioner(4)
    val a = sparkContext.parallelize((1 to 5) ++ (3 to 7)).map(x => (x, x)).partitionBy(p).toSortedRDD
    val d: SortedRDD[Int, Int] = a.distinct
    assert(d.keys.collect.toSeq.sorted == (1 to 7))
  }

  def genData(parts: Int, rows: Int, seed: Int): RDD[(Long, Char)] = {
    val raw = sparkContext.parallelize(1 to parts, parts).mapPartitionsWithIndex {
      (i, it) => new util.Random(i + seed).alphanumeric.take(rows).iterator
    }
    val partitioner = new HashPartitioner(raw.partitions.size)
    raw.zipWithUniqueId.map { case (v, id) => id -> v }.partitionBy(partitioner)
  }

  test("benchmark join", com.lynxanalytics.biggraph.Benchmark) {
    class Demo(parts: Int, rows: Int) {
      val data = genData(parts, rows, 1).toSortedRDD.cache
      data.calculate
      val other = genData(parts, rows, 2).sample(false, 0.5, 0)
        .partitionBy(data.partitioner.get).toSortedRDD.cache
      other.calculate
      def oldJoin = getSum(data.join(other))
      def newJoin = getSum(data.sortedJoin(other))
      def getSum(rdd: RDD[(Long, (Char, Char))]) = rdd.mapValues { case (a, b) => a compare b }.values.reduce(_ + _)
    }
    val parts = 4
    val table = "%10s | %10s | %10s"
    println(table.format("rows", "old (ms)", "new (ms)"))
    for (round <- 10 to 20) {
      val rows = 100000 * round
      val demo = new Demo(parts, rows)
      val mew = Timed(demo.newJoin)
      val old = Timed(demo.oldJoin)
      println(table.format(parts * rows, old.nanos / 1000000, mew.nanos / 1000000))
      assert(mew.value == old.value)
    }
  }

  test("benchmark leftOuterJoin if right is not sorted", com.lynxanalytics.biggraph.Benchmark) {
    class Demo(parts: Int, rows: Int, frac: Double) {
      val data = genData(parts, rows, 1).toSortedRDD.cache
      data.calculate
      val other = genData(parts, rows, 2).sample(false, frac, 0)
        .partitionBy(data.partitioner.get).cache
      other.calculate
      def oldJoin = getSum(data.leftOuterJoin(other).mapValues { case (a, b) => a -> b.getOrElse('0') }.toSortedRDD)
      def newJoin = getSum(data.sortedLeftOuterJoin(other.toSortedRDD).mapValues { case (a, b) => a -> b.getOrElse('0') }.asSortedRDD)
      def getSum(rdd: RDD[(Long, (Char, Char))]) = rdd.mapValues { case (a, b) => a compare b }.values.reduce(_ + _)
    }
    val parts = 4
    val rows = 100000
    val table = "%10s | %10s | %10s"
    println(table.format("frac", "old (ms)", "new (ms)"))
    for (round <- 1 to 10) {
      val frac = round * 0.1
      val demo = new Demo(parts, rows, frac)
      val mew = Timed(demo.newJoin)
      val old = Timed(demo.oldJoin)
      println(table.format(frac, old.nanos / 1000000, mew.nanos / 1000000))
      assert(mew.value == old.value)
    }
  }

  test("benchmark distinct", com.lynxanalytics.biggraph.Benchmark) {
    class Demo(parts: Int, rows: Int) {
      val sorted = genData(parts, rows, 1).values.map(x => (x, x))
        .partitionBy(new HashPartitioner(parts)).toSortedRDD.cache
      sorted.calculate
      val vanilla = sorted.filter(_ => true).cache
      vanilla.calculate
      def oldDistinct = vanilla.distinct.collect.toSeq.sorted
      def newDistinct = sorted.distinct.collect.toSeq.sorted
    }
    val parts = 4
    val table = "%10s | %10s | %10s"
    println(table.format("rows", "old (ms)", "new (ms)"))
    for (round <- 10 to 20) {
      val rows = 100000 * round
      val demo = new Demo(parts, rows)
      val mew = Timed(demo.newDistinct)
      val old = Timed(demo.oldDistinct)
      println(table.format(parts * rows, old.nanos / 1000000, mew.nanos / 1000000))
      assert(mew.value == old.value)
    }
  }

  test("sorted filter") {
    val sorted = genData(4, 1000, 1).values.map(x => (x, x))
      .partitionBy(new HashPartitioner(4)).toSortedRDD
    val filtered = sorted.filter(_._2 != 'a')
    assert(sorted.count > filtered.count)
  }
}
