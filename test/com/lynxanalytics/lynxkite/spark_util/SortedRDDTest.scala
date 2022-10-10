package com.lynxanalytics.biggraph.spark_util

import org.apache.spark.Partitioner
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import com.lynxanalytics.biggraph.TestSparkContext
import com.lynxanalytics.biggraph.Timed

import scala.reflect.ClassTag

class SortedRDDTest extends AnyFunSuite with TestSparkContext {
  import Implicits._

  test("join without intersection") {
    val p = new HashPartitioner(4)
    val a = sparkContext.parallelize(10 to 15).map(x => (x, x)).sort(p)
    val b = sparkContext.parallelize(20 to 25).map(x => (x, x)).sortUnique(p)
    val j: SortedRDD[Int, (Int, Int)] = a.sortedJoin(b)
    assert(j.collect.toSeq == Seq())
  }

  test("join with intersection") {
    val p = new HashPartitioner(4)
    val a = sparkContext.parallelize(10 to 15).map(x => (x, x + 1)).sort(p)
    val b = sparkContext.parallelize(15 to 25).map(x => (x, x + 2)).sortUnique(p)
    val j: SortedRDD[Int, (Int, Int)] = a.sortedJoin(b)
    assert(j.collect.toSeq == Seq(15 -> (16, 17)))
  }

  test("join with unique keys on both sides") {
    val p = new HashPartitioner(4)
    val a = sparkContext.parallelize(10 to 15).map(x => (x, x + 1)).sortUnique(p)
    val b = sparkContext.parallelize(15 to 25).map(x => (x, x + 2)).sortUnique(p)
    val j: UniqueSortedRDD[Int, (Int, Int)] = a.sortedJoin(b)
    assert(j.collect.toSeq == Seq(15 -> (16, 17)))
  }

  test("left outer join with unique keys on both sides") {
    val p = new HashPartitioner(4)
    val a = sparkContext.parallelize(14 to 15).map(x => (x, x + 1)).sortUnique(p)
    val b = sparkContext.parallelize(15 to 25).map(x => (x, x + 2)).sortUnique(p)
    val j: UniqueSortedRDD[Int, (Int, Option[Int])] = a.sortedLeftOuterJoin(b)
    assert(j.collect.toSeq == Seq(14 -> (15, None), 15 -> (16, Some(17))))
  }

  test("join with multiple keys on the left side") {
    val p = new HashPartitioner(4)
    val a = sparkContext.parallelize(Seq(0 -> 1, 10 -> 11, 10 -> 12, 11 -> 10, 20 -> 12)).sort(p)
    val b = sparkContext.parallelize(5 to 15).map(x => (x, "A")).sortUnique(p)
    val sj: SortedRDD[Int, (Int, String)] = a.sortedJoin(b)
    val j: RDD[(Int, (Int, String))] = a.join(b)
    assert(sj.count == j.count)
  }

  test("join with multiple keys on both sides") {
    val p = new HashPartitioner(4)
    val a = sparkContext.parallelize(Seq(0 -> 1, 10 -> 11, 10 -> 12, 11 -> 10, 20 -> 12)).sort(p)
    val b = sparkContext.parallelize(Seq(10 -> "A", 10 -> "B")).sort(p)
    val sj: SortedRDD[Int, (Int, String)] = a.sortedJoinWithDuplicates(b)
    val j: RDD[(Int, (Int, String))] = a.join(b)
    assert(sj.count == j.count)
  }

  test("distinctByKey") {
    val p = new HashPartitioner(4)
    val a = sparkContext.parallelize(
      Array((1, 1), (2, 2), (3, 3), (4, 4), (3, 5), (4, 6), (5, 7), (6, 8)))
      .sort(p)
    val d: SortedRDD[Int, Int] = a.distinctByKey
    assert(d.keys.collect.toSeq.sorted == (1 to 6))
  }

  def assertSortedPartitions(
      rdd: SortedRDD[Int, Int],
      partitioner: Partitioner) {
    assert(rdd.partitioner.orNull eq partitioner)
    val partitionContents = rdd
      .mapPartitionsWithIndex { (pid, it) => it.map(x => (pid, x)) }
      .groupByKey()
      .map { case (pid, list) => list.toSeq }
    for (partition <- partitionContents.collect.toSeq) {
      assert(partition == partition.sorted)
    }
  }

  test("sortedRepartition") {
    val partitioner1 = new HashPartitioner(4)
    val partitioner2 = new HashPartitioner(3)
    val rdd = sparkContext.parallelize(
      Array((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)))
    assertSortedPartitions(
      rdd.sort(partitioner1).sortedRepartition(partitioner2),
      partitioner2)
    assertSortedPartitions(
      rdd.sortUnique(partitioner1).sortedRepartition(partitioner2),
      partitioner2)
  }

  def genData(parts: Int, rows: Int, seed: Int): RDD[(Long, Char)] = {
    val raw = sparkContext.parallelize(1 to parts, parts).mapPartitionsWithIndex {
      (i, it) => new util.Random(i + seed).alphanumeric.take(rows).iterator
    }
    val partitioner = new HashPartitioner(raw.partitions.size)
    raw.zipWithUniqueId.map(_.swap).partitionBy(partitioner)
  }

  test("benchmark sorting", com.lynxanalytics.biggraph.Benchmark) {
    class Demo(parts: Int, rows: Int) {
      val data = genData(parts, rows, 1).cache
      data.calculate
      def oldSort = data.mapPartitions(_.toIndexedSeq.sortBy(_._1).iterator, preservesPartitioning = true).collect
      def newSort = data.mapPartitions(
        x => {
          val a = x.toArray
          scala.util.Sorting.quickSort(a)
          a.iterator
        },
        preservesPartitioning = true).collect
    }
    val parts = 4
    val table = "%10s | %10s | %10s"
    println(table.format("rows", "old (ms)", "new (ms)"))
    println(table.format("---:", "-------:", "-------:")) // github markdown
    for (round <- 10 to 20) {
      val rows = 100000 * round
      val demo = new Demo(parts, rows)
      val mew = Timed(demo.newSort)
      val old = Timed(demo.oldSort)
      println(table.format(parts * rows, old.nanos / 1000000, mew.nanos / 1000000))
      assert(mew.value.toSeq == old.value.toSeq)
    }
  }

  ignore("benchmark groupByKey on non-partitioned RDD", com.lynxanalytics.biggraph.Benchmark) {
    class Demo(parts: Int, rows: Int) {
      val vanilla = genData(7, rows, 1).values.map(x => (x, x)).cache
      vanilla.calculate
      def oldGrouped = vanilla.groupBySortedKey(new HashPartitioner(parts)).collect
      def newGrouped = vanilla.sort(new HashPartitioner(parts)).groupByKey.collect
    }
    val parts = 4
    val table = "%10s | %10s | %10s"
    println(table.format("rows", "old (ms)", "new (ms)"))
    println(table.format("---:", "-------:", "-------:")) // github markdown
    for (round <- 10 to 20) {
      val rows = 100000 * round
      val demo = new Demo(parts, rows)
      val old = Timed(demo.oldGrouped)
      val mew = Timed(demo.newGrouped)
      println(table.format(parts * rows, old.nanos / 1000000, mew.nanos / 1000000))
      assert(mew.value.toSeq == old.value.toSeq)
    }
  }

  test("benchmark groupByKey on SortedRDD", com.lynxanalytics.biggraph.Benchmark) {
    class Demo(parts: Int, rows: Int) {
      val sorted = genData(parts, rows, 1).values.map(x => (x, x))
        .sort(new HashPartitioner(parts)).cache
      sorted.calculate
      def oldGrouped = sorted.groupBySortedKey(sorted.partitioner.get).collect
      def newGrouped = sorted.groupByKey.collect
    }
    val parts = 4
    val table = "%10s | %10s | %10s"
    println(table.format("rows", "old (ms)", "new (ms)"))
    println(table.format("---:", "-------:", "-------:")) // github markdown
    for (round <- 10 to 20) {
      val rows = 100000 * round
      val demo = new Demo(parts, rows)
      val old = Timed(demo.oldGrouped)
      val mew = Timed(demo.newGrouped)
      println(table.format(parts * rows, old.nanos / 1000000, mew.nanos / 1000000))
      assert(mew.value.toSeq == old.value.toSeq)
    }
  }

  test("benchmark join", com.lynxanalytics.biggraph.Benchmark) {
    class Demo(parts: Int, rows: Int) {
      val partitioner = new HashPartitioner(parts)
      val data = genData(parts, rows, 1).sort(partitioner).cache
      data.calculate
      val other = genData(parts, rows, 2).sample(false, 0.5, 0)
        .partitionBy(data.partitioner.get).sortUnique(partitioner).cache
      other.calculate
      def oldJoin = getSum(data.join(other))
      def newJoin = getSum(data.sortedJoin(other))
      def getSum(rdd: RDD[(Long, (Char, Char))]) = rdd.mapValues { case (a, b) => a compare b }.values.reduce(_ + _)
    }
    val parts = 4
    val table = "%10s | %10s | %10s"
    println(table.format("rows", "old (ms)", "new (ms)"))
    println(table.format("---:", "-------:", "-------:")) // github markdown
    for (round <- 10 to 20) {
      val rows = 100000 * round
      val demo = new Demo(parts, rows)
      val mew = Timed(demo.newJoin)
      val old = Timed(demo.oldJoin)
      println(table.format(parts * rows, old.nanos / 1000000, mew.nanos / 1000000))
      assert(mew.value == old.value)
    }
  }

  test("benchmark distinct", com.lynxanalytics.biggraph.Benchmark) {
    class Demo(parts: Int, rows: Int) {
      val sorted = genData(parts, rows, 1).values.map(x => (x, x))
        .sort(new HashPartitioner(parts)).cache
      sorted.calculate
      val vanilla = sorted.map(x => x).cache
      vanilla.calculate
      def oldDistinct = vanilla.distinct.collect.toSeq.sorted
      def newDistinct = sorted.distinct.collect.toSeq.sorted
    }
    val parts = 4
    val table = "%10s | %10s | %10s"
    println(table.format("rows", "old (ms)", "new (ms)"))
    println(table.format("---:", "-------:", "-------:")) // github markdown
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
      .sort(new HashPartitioner(4))
    val filtered = sorted.filter(_._2 != 'a')
    assert(sorted.count > filtered.count)
  }

  test("sorted mapValues") {
    val sorted = genData(4, 1000, 1).values.map(x => (x, x))
      .sort(new HashPartitioner(4))
    val mew = sorted.mapValues(x => 'a')
    val mewKeys = sorted.mapValuesWithKeys(x => 'a')
    val old = sorted.map(x => x).mapValues(x => 'a')
    assert(mew.collect.toMap == old.collect.toMap)
    assert(mew.collect.toMap == mewKeys.collect.toMap)
  }

  test("id filter on ArrayBackedSortedRDD") {
    val sorted = genData(4, 1000, 1).values.map(x => (x, x))
      .sort(new HashPartitioner(4))
    val ids = IndexedSeq('l', 'n', 'x', 'y')
    val restricted = sorted.restrictToIdSet(ids)
    val filtered = sorted.filter { case (id, value) => ids.contains(id) }
    assert(restricted.collect.toSeq.sorted == filtered.collect.toSeq.sorted)
  }

  test("id filter on DerivedRDD") {
    val partitioner = new HashPartitioner(4)
    val sorted1 = genData(4, 1000, 1).values.map(x => (x, x))
      .sort(partitioner)
    val sorted2 = genData(4, 1000, 2).values.map(x => (x, x))
      .sort(partitioner)
    val complex =
      sorted1.mapValues(2 * _).sortedLeftOuterJoin(
        sorted2.distinctByKey.filter(a => a._2 != 'x').filter(a => a._2 != 'a'))
    // Make sure the test is not trivial.
    assert(complex.count > 1000)

    val ids = IndexedSeq('l', 'n', 'x', 'y')
    val restricted = complex.restrictToIdSet(ids)
    val filtered = complex.filter { case (id, value) => ids.contains(id) }
    // Make sure the test is not trivial.
    assert(filtered.count > 100)
    assert(filtered.count < 1000)
    // Our restiction is identical the result of the trusted RDD filter.
    assert(restricted.collect.toSeq.sorted == filtered.collect.toSeq.sorted)
  }

  test("benchmark mapValues with keys", com.lynxanalytics.biggraph.Benchmark) {
    class Demo(parts: Int, rows: Int) {
      val sorted = genData(parts, rows, 1).values.map(x => (x, x))
        .sort(new HashPartitioner(parts)).cache
      sorted.calculate
      def oldMV = sorted.map { case (id, x) => id -> (id, x) }.sort(sorted.partitioner.get).collect.toMap
      def newMV = sorted.mapValuesWithKeys { case (id, x) => (id, x) }.collect.toMap
    }
    val parts = 4
    val table = "%10s | %10s | %10s"
    println(table.format("rows", "old (ms)", "new (ms)"))
    println(table.format("---:", "-------:", "-------:")) // github markdown
    for (round <- 10 to 20) {
      val rows = 10000 * round
      val demo = new Demo(parts, rows)
      val mew = Timed(demo.newMV)
      val old = Timed(demo.oldMV)
      println(table.format(parts * rows, old.nanos / 1000000, mew.nanos / 1000000))
      assert(mew.value == old.value)
    }
  }
}
