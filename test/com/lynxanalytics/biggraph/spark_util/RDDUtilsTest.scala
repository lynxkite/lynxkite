package com.lynxanalytics.biggraph.spark_util

import org.scalatest.FunSuite
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import com.lynxanalytics.biggraph.graph_api.RuntimeContext
import com.lynxanalytics.biggraph.TestSparkContext

class RDDUtilsTest extends FunSuite with TestSparkContext {
  test("genId works for small and large numbers") {
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
        val id = Implicits.genId(parts, part, row)
        assert(partitioner.getPartition(id) == part, s"genId($parts, $part, $row)")
        assert(!ids.contains(id), s"genId($parts, $part, $row)")
        ids += id
      }
    }
  }

  test("genId works for random numbers") {
    val rnd = new util.Random(0)
    for (i <- 0 to 1000) {
      val parts = rnd.nextInt(1000) max 1
      val part = rnd.nextInt(parts)
      val row = rnd.nextInt(1000000)
      val id = Implicits.genId(parts, part, row)
      val partitioner = new HashPartitioner(parts)
      assert(partitioner.getPartition(id) == part, s"genId($parts, $part, $row)")
    }
  }

  test("groupBySortedKey works as expected") {
    import Implicits._
    val rnd = new util.Random(0)
    val data = (0 until 1000).map(_ => (rnd.nextInt(100), rnd.nextLong()))
    val sourceRDD = sparkContext.parallelize(data, 10)
    val p = new HashPartitioner(10)
    val groupped = sourceRDD.groupBySortedKey(p).mapValues(_.toSeq.sorted)
    assert(groupped.keys.collect.toSet == sourceRDD.keys.collect.toSet)
    assert(groupped.values.map(_.size).reduce(_ + _) == 1000)
    val groupped2 = sourceRDD.sort(p).groupBySortedKey(p).mapValues(_.toSeq.sorted)
    assert(groupped.collect.toSeq == groupped2.collect.toSeq)
  }

  test("reduceBySortedKey works as expected") {
    import Implicits._
    val rnd = new util.Random(0)
    val data = (0 until 1000).map(_ => (rnd.nextInt(100), rnd.nextLong()))
    val sourceRDD = sparkContext.parallelize(data, 10)
    val p = new HashPartitioner(10)
    val reduced = sourceRDD.reduceBySortedKey(p, _ + _)
    assert(reduced.keys.collect.toSeq.sorted == sourceRDD.keys.collect.toSet.toSeq.sorted)
    assert(reduced.values.reduce(_ + _) == sourceRDD.values.reduce(_ + _))
    val reduced2 = sourceRDD.sort(p).reduceBySortedKey(p, _ + _)
    assert(reduced.collect.toSeq == reduced2.collect.toSeq)
  }

  test("countApproxEvenRDD works as expected") {
    import Implicits._
    val rnd = new util.Random(0)
    val data = (0 until 1000).map(_ => (rnd.nextInt(100), rnd.nextLong()))
    val rdd = sparkContext.parallelize(data, 10)
    implicit val rc = RuntimeContext(sparkContext, null, null, null, null)
    assert(RDDUtils.countApproxEvenRDD(rdd) == 1000)
  }
}
