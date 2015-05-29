package com.lynxanalytics.biggraph.spark_util

import org.scalatest.FunSuite
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import com.lynxanalytics.biggraph.TestSparkContext

class RDDUtilsTest extends FunSuite with TestSparkContext {
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

  test("lookup operations work as expected") {
    val rnd = new util.Random(0)
    val localSource = (0 until 1000).map(_ => (rnd.nextInt(100), rnd.nextLong()))
    val localLookup = (0 until 50).map(x => (x, rnd.nextDouble()))
    val localLookupMap = localLookup.toMap
    val localResult = localSource
      .flatMap {
        case (key, value) => localLookupMap.get(key).map(lv => key -> (value, lv))
      }
      .sorted

    def checkGood(rdd: RDD[(Int, (Long, Double))]) {
      assert(rdd.collect.toSeq.sorted == localResult)
    }

    import Implicits._
    val sourceRDD = sparkContext.parallelize(localSource, 10)
    val lookupRDD = sparkContext.parallelize(localLookup).toSortedRDD(new HashPartitioner(10))

    checkGood(RDDUtils.joinLookup(sourceRDD, lookupRDD))
    checkGood(RDDUtils.smallTableLookup(sourceRDD, localLookupMap))
    checkGood(RDDUtils.hybridLookup(sourceRDD, lookupRDD, 200))
    checkGood(RDDUtils.hybridLookup(sourceRDD, lookupRDD, 0))

    val counts = sourceRDD
      .keys
      .map(_ -> 1l)
      .reduceBySortedKey(lookupRDD.partitioner.get, _ + _)
    val lookupRDDWihtCounts = lookupRDD.sortedJoin(counts)
    checkGood(RDDUtils.hybridLookupUsingCounts(sourceRDD, lookupRDDWihtCounts, 200))
    checkGood(RDDUtils.hybridLookupUsingCounts(sourceRDD, lookupRDDWihtCounts, 0))
  }
}
